"""Transactional outbox + per-batch event stream.

State changes write their follow-ups (progress events, "enqueue next stage")
to `outbox` inside the same transaction. The relay later turns them into jobs
and stream events. A crash between the two only causes a repeat publish, which
the deterministic job id and the (batch_id, seq) key make harmless.
"""
from __future__ import annotations

import json
import sqlite3

from . import clock, db

FINAL = ("completed", "failed", "cancelled")
NEEDS_ATTENTION = ("failed",)


def emit(conn: sqlite3.Connection, batch_id: str, type_: str, data: dict, file_id: str | None = None) -> int:
    """Assign the next per-batch seq and stage the event in the outbox."""
    conn.execute("UPDATE batches SET last_seq = last_seq + 1 WHERE id = ?", (batch_id,))
    seq = conn.execute("SELECT last_seq FROM batches WHERE id = ?", (batch_id,)).fetchone()[0]
    if file_id:
        conn.execute("UPDATE files SET last_seq = ? WHERE id = ?", (seq, file_id))
    payload = {"batch_id": batch_id, "seq": seq, "type": type_, "data": {**data, "seq": seq}}
    conn.execute("INSERT INTO outbox (topic, payload, created_at) VALUES ('event', ?, ?)",
                 (json.dumps(payload), clock.now()))
    return seq


def enqueue(conn: sqlite3.Connection, stage: str, file_id: str, run_at: float | None = None) -> None:
    conn.execute("INSERT INTO outbox (topic, payload, created_at) VALUES ('enqueue', ?, ?)",
                 (json.dumps({"stage": stage, "file_id": file_id, "run_at": run_at}), clock.now()))


def summary_counts(conn: sqlite3.Connection, batch_id: str) -> dict:
    counts = {r["status"]: r["n"] for r in conn.execute(
        "SELECT status, COUNT(*) AS n FROM files WHERE batch_id = ? GROUP BY status", (batch_id,))}
    total = sum(counts.values())
    done = counts.get("completed", 0)
    attention = sum(counts.get(s, 0) for s in NEEDS_ATTENTION)
    return {"total": total, "counts": counts, "done": done, "need_attention": attention,
            "text": f"{done} of {total} done" + (f", {attention} need attention" if attention else "")}


def emit_summary(conn: sqlite3.Connection, batch_id: str) -> None:
    emit(conn, batch_id, "batch.summary", summary_counts(conn, batch_id))


# ---- relay ----

def relay_once(limit: int = 500) -> int:
    now = clock.now()
    with db.tx() as conn:
        rows = conn.execute(
            "SELECT id, topic, payload FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT ?", (limit,)
        ).fetchall()
        for r in rows:
            p = json.loads(r["payload"])
            if r["topic"] == "enqueue":
                f = conn.execute("SELECT batch_id, user_id FROM files WHERE id = ?", (p["file_id"],)).fetchone()
                # Deterministic job id: a duplicate add is a no-op.
                conn.execute(
                    "INSERT OR IGNORE INTO jobs (id, file_id, batch_id, user_id, stage, status, attempt, "
                    "next_run_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'queued', 0, ?, ?, ?)",
                    (f"{p['stage']}:{p['file_id']}", p["file_id"], f["batch_id"], f["user_id"], p["stage"],
                     p.get("run_at") or now, now, now),
                )
            elif r["topic"] == "event":
                conn.execute(
                    "INSERT OR IGNORE INTO events (batch_id, seq, type, payload, created_at) VALUES (?, ?, ?, ?, ?)",
                    (p["batch_id"], p["seq"], p["type"], json.dumps(p["data"]), now),
                )
            conn.execute("UPDATE outbox SET published_at = ? WHERE id = ?", (now, r["id"]))
    return len(rows)


def read_events(batch_id: str, after_seq: int, limit: int = 500) -> list[dict]:
    with db.read() as conn:
        rows = conn.execute(
            "SELECT seq, type, payload FROM events WHERE batch_id = ? AND seq > ? ORDER BY seq LIMIT ?",
            (batch_id, after_seq, limit),
        ).fetchall()
    return [{"seq": r["seq"], "type": r["type"], "data": json.loads(r["payload"])} for r in rows]


def can_replay_from(batch_id: str, after_seq: int) -> bool:
    """True if the stream still holds every event after `after_seq`."""
    with db.read() as conn:
        b = conn.execute("SELECT last_seq FROM batches WHERE id = ?", (batch_id,)).fetchone()
        if b is None:
            return False
        if after_seq >= b["last_seq"]:
            return True
        lo = conn.execute("SELECT MIN(seq) FROM events WHERE batch_id = ?", (batch_id,)).fetchone()[0]
    return lo is not None and lo <= after_seq + 1
