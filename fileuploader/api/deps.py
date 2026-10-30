"""Shared request plumbing: identity, rate limits, idempotency, file views."""
from __future__ import annotations

import json
import math
import sqlite3
from typing import Callable

from fastapi import Request
from fastapi.responses import JSONResponse

from .. import clock, db, ratelimit
from ..config import get_settings
from ..errors import ApiError, fix_for
from ..ids import sha256_hex


def user_id(request: Request) -> str:
    uid = getattr(request.state, "user_id", None)
    if not uid:
        raise ApiError(401, "UNAUTHENTICATED")
    return uid


def limit(request: Request, name: str, cost: float = 1.0) -> None:
    """Take from a named per-user bucket; 429 + Retry-After when empty."""
    d = ratelimit.take([ratelimit.inbound_spec(name, user_id(request))], cost)
    if not d.allowed:
        raise ApiError(429, "RATE_LIMITED", retryable=True, retry_after_ms=math.ceil(d.wait_s * 1000))


def idempotent(request: Request, body: object, fn: Callable[[], tuple[int, dict]], required: bool = True) -> JSONResponse:
    """Same key + same body -> stored response; same key + different body -> 422;
    key still in flight -> 409 with Retry-After: 1. Stored for 24 h."""
    key = request.headers.get("Idempotency-Key")
    if not key:
        if required:
            raise ApiError(400, "IDEMPOTENCY_KEY_REQUIRED")
        status, out = fn()
        return JSONResponse(out, status_code=status)

    uid = user_id(request)
    req_hash = sha256_hex(request.method, request.url.path, json.dumps(body, sort_keys=True, default=str))
    s = get_settings()
    now = clock.now()
    with db.tx() as conn:
        row = conn.execute("SELECT * FROM idempotency_keys WHERE user_id = ? AND key = ?", (uid, key)).fetchone()
        stale_in_flight = row is not None and row["state"] == "in_flight" and row["created_at"] < now - 60
        if row is not None and (row["created_at"] < now - s.idempotency_ttl_s or stale_in_flight):
            conn.execute("DELETE FROM idempotency_keys WHERE user_id = ? AND key = ?", (uid, key))
            row = None
        if row is not None:
            if row["request_hash"] != req_hash:
                raise ApiError(422, "IDEMPOTENCY_KEY_REUSED")
            if row["state"] == "in_flight":
                raise ApiError(409, "IDEMPOTENCY_IN_FLIGHT", retryable=True, retry_after_ms=1000)
            return JSONResponse(json.loads(row["response_json"]), status_code=row["status_code"],
                                headers={"Idempotent-Replayed": "true"})
        conn.execute(
            "INSERT INTO idempotency_keys (user_id, key, request_hash, state, created_at) VALUES (?, ?, ?, 'in_flight', ?)",
            (uid, key, req_hash, now))

    def store(status: int, out: dict) -> None:
        with db.tx() as conn:
            conn.execute("UPDATE idempotency_keys SET state='done', status_code=?, response_json=? "
                         "WHERE user_id=? AND key=?", (status, json.dumps(out), uid, key))

    def forget() -> None:
        with db.tx() as conn:
            conn.execute("DELETE FROM idempotency_keys WHERE user_id=? AND key=?", (uid, key))

    try:
        status, out = fn()
    except ApiError as e:
        if e.status >= 500 or e.status in (409, 429):
            forget()  # transient: let the client retry with the same key
        else:
            store(e.status, e.body(getattr(request.state, "request_id", None)))
        raise
    except BaseException:
        forget()
        raise
    store(status, out)
    return JSONResponse(out, status_code=status)


def load_file(conn: sqlite3.Connection, request: Request, file_id: str) -> sqlite3.Row:
    f = conn.execute("SELECT * FROM files WHERE id = ?", (file_id,)).fetchone()
    if f is None or f["user_id"] != user_id(request):
        raise ApiError(404, "NOT_FOUND")
    return f


def part_size_for(size: int, requested: int | None = None) -> int:
    """The client may pick the part size (adaptive chunks); never more than 10,000 parts."""
    s = get_settings()
    return max(requested or s.chunk_size, math.ceil(size / s.max_parts))


def file_part_size(f: sqlite3.Row) -> int:
    return f["part_size"] or part_size_for(f["size_bytes"])


def part_count(size: int, part_size: int) -> int:
    return max(1, math.ceil(size / part_size))


def upload_mode(size: int) -> str:
    return "single" if size < get_settings().chunk_size else "multipart"


def file_view(f: sqlite3.Row, confirmed_bytes: int | None = None) -> dict:
    """`upload_pct` and `processing_pct` are the two segments of the file's combined bar;
    after a refresh this snapshot restores both."""
    uploaded = f["uploaded_at"] is not None or f["status"] == "completed"
    ps = file_part_size(f)
    out = {
        "id": f["id"], "batch_id": f["batch_id"], "client_file_id": f["client_file_id"], "name": f["name"],
        "size_bytes": f["size_bytes"], "sha256": f["sha256"], "fingerprint": f["fingerprint"],
        "status": f["status"], "stage": f["stage"], "progress_pct": f["progress_pct"],
        "upload_pct": 100.0 if uploaded else f["progress_pct"],
        "processing_pct": f["progress_pct"] if uploaded else 0.0,
        "processing_estimate_ms": f["processing_estimate_ms"],
        "attempts": f["attempts"], "seq": f["last_seq"],
        "upload": {"mode": upload_mode(f["size_bytes"]), "part_size": ps, "part_count": part_count(f["size_bytes"], ps),
                   "dedupe_candidate": f["dedup_candidate"] is not None},
        "error": None, "dedup_of": f["dedup_of"],
    }
    if f["error_code"]:
        out["error"] = {"code": f["error_code"], "message": f["error_message"],
                        "retryable_by_user": bool(f["retryable_by_user"]), "fix": fix_for(f["error_code"])}
    if f["status"] == "completed":
        out["result_url"] = f"/files/{f['id']}/result"
    if confirmed_bytes is not None:
        out["upload"]["confirmed_bytes"] = confirmed_bytes
    return out
