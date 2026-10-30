"""File state machine. Every transition is a guarded UPDATE on `version`, so two
workers can never both advance the same file.

pending -> uploading -> uploaded -> processing -> completed
uploading | processing -> failed_retryable | waiting_vendor | failed
any non-final -> cancelled
"""
from __future__ import annotations

import sqlite3

from . import clock, estimates, events
from .events import FINAL

STAGES = ("scan", "extract", "thumbnail", "process", "finalize")
FAN_IN = ("extract", "thumbnail", "process")  # finalize waits for all three

TRANSITIONS: dict[str, set[str]] = {
    "pending": {"uploading", "completed", "failed", "cancelled"},
    "uploading": {"uploading", "uploaded", "failed_retryable", "waiting_vendor", "failed", "cancelled", "pending"},
    "uploaded": {"processing", "failed_retryable", "waiting_vendor", "failed", "cancelled"},
    "processing": {"processing", "completed", "failed_retryable", "waiting_vendor", "failed", "cancelled"},
    "failed_retryable": {"processing", "failed_retryable", "waiting_vendor", "failed", "cancelled", "completed"},
    "waiting_vendor": {"processing", "failed_retryable", "waiting_vendor", "failed", "cancelled", "completed"},
    "failed": {"pending", "processing", "cancelled"},  # user retry / DLQ replay
    "completed": set(),
    "cancelled": set(),
}


class InvalidTransition(Exception):
    pass


def get_file(conn: sqlite3.Connection, file_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM files WHERE id = ?", (file_id,)).fetchone()


def transition(conn: sqlite3.Connection, f: sqlite3.Row, to: str, **fields) -> bool:
    """UPDATE files SET status=:to, ..., version=version+1 WHERE id=:id AND version=:version.

    Returns False if someone else already moved the file (zero rows updated).
    """
    if to not in TRANSITIONS[f["status"]]:
        raise InvalidTransition(f"{f['status']} -> {to}")
    cols = {"status": to, "updated_at": clock.now(), **fields}
    sets = ", ".join(f"{k} = ?" for k in cols)
    cur = conn.execute(
        f"UPDATE files SET {sets}, version = version + 1 WHERE id = ? AND version = ?",
        (*cols.values(), f["id"], f["version"]),
    )
    return cur.rowcount == 1


def progress(conn: sqlite3.Connection, file_id: str) -> float:
    """processing_pct of an uploaded file (see estimates.processing_pct)."""
    return estimates.processing_pct(conn, get_file(conn, file_id))


def derive_processing_status(conn: sqlite3.Connection, file_id: str) -> str:
    """While a file is in the pipeline, its status follows its jobs."""
    jobs = conn.execute("SELECT status, attempt FROM jobs WHERE file_id = ?", (file_id,)).fetchall()
    if any(j["status"] == "waiting_vendor" for j in jobs):
        return "waiting_vendor"
    if any(j["status"] == "queued" and j["attempt"] > 0 for j in jobs):  # waiting out a backoff
        return "failed_retryable"
    return "processing"


def sync_status(conn: sqlite3.Connection, file_id: str, **fields) -> sqlite3.Row:
    """Re-derive and store the status of an in-pipeline file. Final states are left alone."""
    f = get_file(conn, file_id)
    if f["status"] in FINAL or f["status"] in ("pending", "uploading"):
        return f
    to = derive_processing_status(conn, file_id)
    fields.setdefault("progress_pct", progress(conn, file_id))
    transition(conn, f, to, **fields)
    return get_file(conn, file_id)


def emit_stage(conn: sqlite3.Connection, f: sqlite3.Row, stage: str, **extra) -> None:
    """file.stage: the bar's processing segment is `processing_pct`."""
    events.emit(conn, f["batch_id"], "file.stage",
                {"file_id": f["id"], "stage": stage, "progress_pct": f["progress_pct"],
                 "processing_pct": f["progress_pct"] if f["uploaded_at"] is not None else 0.0, **extra}, f["id"])


def requeue(conn: sqlite3.Connection, where: str, args: tuple) -> None:
    """Re-queue jobs with fresh attempts (user retry, DLQ replay). A process job that failed
    or died gets a new generation, so it submits a new vendor job instead of finding the old one."""
    now = clock.now()
    conn.execute(
        f"UPDATE jobs SET generation = generation + 1, vendor_job_id = NULL, vendor_status = NULL, "
        f"vendor_progress = NULL, vendor_queue_position = NULL, vendor_updated_at = NULL, submitted_at = NULL, "
        f"polls = 0 WHERE stage = 'process' AND status IN ('failed', 'dead') AND {where}", args)
    conn.execute(f"UPDATE jobs SET status = 'queued', attempt = 0, next_run_at = ?, locked_until = NULL, "
                 f"lease_id = NULL, updated_at = ? WHERE {where}", (now, now, *args))
