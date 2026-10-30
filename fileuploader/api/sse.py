"""GET /batches/:id/events — server-sent events with replay.

Every event carries a monotonic per-batch `seq` (also the SSE id). On
reconnect the client sends Last-Event-ID; we replay from the event stream
(kept 1 hour) or, if that is gone, send a `snapshot` of the whole batch. The
client applies an event only if its seq is newer, so replays are harmless.
"""
from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .. import db, events
from ..config import get_settings
from ..errors import ApiError
from .deps import user_id
from .routes import _batch_view

router = APIRouter()

HEARTBEAT_S = 15.0


def _frame(seq: int, type_: str, data: dict) -> str:
    return f"id: {seq}\nevent: {type_}\ndata: {json.dumps(data)}\n\n"


def _snapshot(batch_id: str) -> tuple[int, str]:
    with db.read() as conn:
        view = _batch_view(conn, batch_id)
    return view["seq"], _frame(view["seq"], "snapshot", view)


@router.get("/batches/{batch_id}/events")
async def stream(batch_id: str, request: Request, last_event_id: int | None = None, once: bool = False):
    with db.read() as conn:
        b = conn.execute("SELECT user_id FROM batches WHERE id = ?", (batch_id,)).fetchone()
    if b is None or b["user_id"] != user_id(request):
        raise ApiError(404, "NOT_FOUND")
    header = request.headers.get("Last-Event-ID")
    resume_from = int(header) if header and header.isdigit() else last_event_id
    poll = get_settings().poll_interval_s

    async def gen():
        yield "retry: 2000\n\n"
        if resume_from is not None and events.can_replay_from(batch_id, resume_from):
            last = resume_from
        else:
            last, frame = _snapshot(batch_id)
            yield frame
        idle = 0.0
        while True:
            batch = await asyncio.to_thread(events.read_events, batch_id, last)
            for e in batch:
                last = e["seq"]
                yield _frame(e["seq"], e["type"], e["data"])
            if once:
                return
            if batch:
                idle = 0.0
                continue
            if await request.is_disconnected():
                return
            await asyncio.sleep(poll)
            idle += poll
            if idle >= HEARTBEAT_S:
                idle = 0.0
                yield ": keep-alive\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
