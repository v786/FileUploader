"""Housekeeping: abort multipart uploads idle for 24 h, expire event streams
(1 h), idempotency records (24 h) and old published outbox rows."""
from __future__ import annotations

import asyncio

from . import adapters, clock, db, events, state
from .config import get_settings
from .errors import message_for, retryable_by_user
from .log import log


def sweep_once() -> dict:
    s = get_settings()
    now = clock.now()
    store = adapters.get().store
    aborted = 0
    with db.read() as conn:
        stale = conn.execute(
            "SELECT id, s3_key, s3_upload_id FROM files WHERE status = 'uploading' AND s3_upload_id IS NOT NULL "
            "AND updated_at < ?", (now - s.multipart_idle_s,)).fetchall()
    for r in stale:
        store.abort_multipart(r["s3_key"], r["s3_upload_id"])
        with db.tx() as conn:
            f = state.get_file(conn, r["id"])
            if f["status"] != "uploading" or f["s3_upload_id"] != r["s3_upload_id"]:
                continue
            conn.execute("DELETE FROM file_parts WHERE file_id = ?", (f["id"],))
            code = "UPLOAD_EXPIRED"
            if state.transition(conn, f, "failed", s3_upload_id=None, error_code=code, error_message=message_for(code),
                                retryable_by_user=int(retryable_by_user(code)), progress_pct=0.0):
                events.emit(conn, f["batch_id"], "file.failed",
                            {"file_id": f["id"], "error_code": code, "message": message_for(code),
                             "retryable_by_user": True}, f["id"])
                events.emit_summary(conn, f["batch_id"])
                aborted += 1

    with db.tx() as conn:
        ev = conn.execute("DELETE FROM events WHERE created_at < ?", (now - s.event_retention_s,)).rowcount
        idem = conn.execute("DELETE FROM idempotency_keys WHERE created_at < ?", (now - s.idempotency_ttl_s,)).rowcount
        ob = conn.execute("DELETE FROM outbox WHERE published_at IS NOT NULL AND published_at < ?",
                          (now - 86400,)).rowcount
    out = {"aborted_uploads": aborted, "events_expired": ev, "idempotency_expired": idem, "outbox_pruned": ob}
    if any(out.values()):
        log("sweeper.run", **out)
    return out


async def run_sweeper(stop: asyncio.Event, interval_s: float = 60.0) -> None:
    while not stop.is_set():
        try:
            await asyncio.to_thread(sweep_once)
        except Exception as exc:  # noqa: BLE001
            log("sweeper.error", error=str(exc))
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
        except (asyncio.TimeoutError, TimeoutError):
            pass
