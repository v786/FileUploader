"""Rolling p50 durations and the server half of the combined progress bar.

Every finished stage records how long it took, keyed by file family and size
bucket. From those samples we derive:

- `processing_estimate_ms` per file, returned by batch create so the client can
  fix the upload/processing weights of its bar when the upload starts;
- `processing_pct` (0-100), the post-upload stages weighted by each stage's p50.
"""
from __future__ import annotations

import sqlite3
import statistics

from . import clock, filetypes
from .config import get_settings

TOTAL = "_total"  # pseudo-stage: uploaded -> completed
SAMPLE_WINDOW = 101
MIN_SAMPLES = 5


def _bucket(name: str, size: int) -> str:
    return f"{filetypes.family(name)}:{filetypes.size_bucket(size)}"


def record(conn: sqlite3.Connection, stage: str, f: sqlite3.Row, duration_s: float) -> None:
    conn.execute("INSERT INTO stage_samples (stage, bucket, duration_s, ts) VALUES (?, ?, ?, ?)",
                 (stage, _bucket(f["name"], f["size_bytes"]), max(0.0, duration_s), clock.now()))


def _median(conn: sqlite3.Connection, stage: str, bucket: str | None) -> float | None:
    q = "SELECT duration_s FROM stage_samples WHERE stage = ?"
    args: tuple = (stage,)
    if bucket is not None:
        q += " AND bucket = ?"
        args += (bucket,)
    rows = [r[0] for r in conn.execute(q + " ORDER BY ts DESC LIMIT ?", (*args, SAMPLE_WINDOW))]
    return statistics.median(rows) if len(rows) >= MIN_SAMPLES else None


def p50(conn: sqlite3.Connection, stage: str, name: str, size: int) -> float:
    """p50 for this file type and size bucket, else for the stage, else the configured prior."""
    got = _median(conn, stage, _bucket(name, size))
    if got is None:
        got = _median(conn, stage, None)
    if got is None:
        got = get_settings().stage_p50_prior_s.get(stage, 1.0)
    return got


def processing_estimate_s(conn: sqlite3.Connection, name: str, size: int) -> float:
    total = _median(conn, TOTAL, _bucket(name, size))
    if total is not None:
        return total
    # No history yet: the critical path through the stage priors.
    d = {st: p50(conn, st, name, size) for st in ("scan", "extract", "thumbnail", "process", "finalize")}
    if get_settings().process_parallel_with_scan:
        return max(d["scan"] + max(d["extract"], d["thumbnail"]), d["process"]) + d["finalize"]
    return d["scan"] + max(d["extract"], d["thumbnail"], d["process"]) + d["finalize"]


def processing_pct(conn: sqlite3.Connection, f: sqlite3.Row) -> float:
    """Post-upload progress, weighted by each stage's p50. Never moves backwards and
    reaches 100 only when the file completes."""
    if f["status"] == "completed":
        return 100.0
    stages = get_settings().stages
    weights = {st: p50(conn, st, f["name"], f["size_bytes"]) for st in stages}
    got = 0.0
    for j in conn.execute("SELECT stage, status, vendor_progress FROM jobs WHERE file_id = ?", (f["id"],)):
        if j["stage"] not in weights:
            continue
        if j["status"] == "done":
            got += weights[j["stage"]]
        elif j["vendor_progress"]:
            got += weights[j["stage"]] * min(100.0, j["vendor_progress"]) / 100.0
    pct = min(99.0, 100.0 * got / sum(weights.values()))
    prev = f["progress_pct"] if f["uploaded_at"] is not None else 0.0
    return round(max(prev, pct), 1)
