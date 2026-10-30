"""Batch and file endpoints: the API only signs, records and completes —
file bytes go straight from the browser to object storage."""
from __future__ import annotations

import json
import os

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from .. import adapters, clock, db, events, state
from ..adapters.object_store import S3Error, stream_sha256
from ..config import get_settings
from ..errors import ApiError, message_for, retryable_by_user
from ..ids import new_id
from ..log import log
from .deps import file_view, idempotent, limit, load_file, part_count, upload_mode, user_id

router = APIRouter()

UPLOAD_ERROR_CODES = {"CHECKSUM_MISMATCH", "SIZE_MISMATCH", "UPLOAD_EXPIRED", "UPLOAD_INCOMPLETE"}


class FileIn(BaseModel):
    client_file_id: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=512)
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class BatchIn(BaseModel):
    files: list[FileIn] = Field(min_length=1)


class SignIn(BaseModel):
    part_numbers: list[int] = Field(min_length=1)


class PartIn(BaseModel):
    etag: str = Field(min_length=1)
    size_bytes: int = Field(ge=0)
    checksum: str | None = None


def _s3_key(user: str, file_id: str, sha: str) -> str:
    return f"users/{user}/{file_id}/{sha}"


def _validate(f: FileIn) -> str | None:
    s = get_settings()
    if f.size_bytes > s.max_file_size:
        return "FILE_TOO_LARGE"
    if os.path.splitext(f.name)[1].lower() not in s.allowed_extensions:
        return "UNSUPPORTED_TYPE"
    return None


# ------------------------------------------------------------------ batches

@router.post("/batches")
def create_batch(body: BatchIn, request: Request):
    uid = user_id(request)

    def run() -> tuple[int, dict]:
        s = get_settings()
        if len(body.files) > s.max_files_per_batch:
            raise ApiError(400, "TOO_MANY_FILES")
        ids = [f.client_file_id for f in body.files]
        if len(set(ids)) != len(ids):
            raise ApiError(400, "VALIDATION_ERROR", "client_file_id must be unique within a batch.")
        with db.read() as conn:
            waiting = conn.execute("SELECT COUNT(*) FROM jobs WHERE status = 'queued'").fetchone()[0]
        if waiting > s.queue_high_water:
            raise ApiError(503, "BUSY", retryable=True, retry_after_ms=5000)
        limit(request, "batch_creation")

        key = request.headers["Idempotency-Key"]
        now = clock.now()
        with db.tx() as conn:
            batch_id = new_id("bat")
            cur = conn.execute(
                "INSERT INTO batches (id, user_id, idempotency_key, created_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT (user_id, idempotency_key) DO NOTHING", (batch_id, uid, key, now))
            if cur.rowcount == 0:  # backstop to the idempotency middleware: return the existing batch
                batch_id = conn.execute("SELECT id FROM batches WHERE user_id=? AND idempotency_key=?",
                                        (uid, key)).fetchone()[0]
                return 201, _batch_view(conn, batch_id)

            for fin in body.files:
                fid = new_id("fil")
                err = _validate(fin)
                status, extra, dup = "pending", {}, None
                if err:
                    status = "failed"
                    extra = {"error_code": err, "error_message": message_for(err),
                             "retryable_by_user": int(retryable_by_user(err))}
                else:
                    dup = conn.execute(
                        "SELECT id, s3_key, result_json FROM files WHERE user_id = ? AND sha256 = ? "
                        "AND status = 'completed' AND size_bytes = ? LIMIT 1",
                        (uid, fin.sha256, fin.size_bytes)).fetchone()
                    if dup:  # content dedupe: link the existing object, no bytes move
                        status = "completed"
                        extra = {"dedup_of": dup["id"], "result_json": dup["result_json"], "progress_pct": 100.0,
                                 "stage": "finalize"}
                cols = {"id": fid, "batch_id": batch_id, "user_id": uid, "client_file_id": fin.client_file_id,
                        "name": fin.name, "size_bytes": fin.size_bytes, "sha256": fin.sha256, "status": status,
                        "s3_key": dup["s3_key"] if dup else _s3_key(uid, fid, fin.sha256),
                        "upload_mode": upload_mode(fin.size_bytes), "created_at": now, "updated_at": now, **extra}
                conn.execute(f"INSERT INTO files ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))}) "
                             "ON CONFLICT (batch_id, client_file_id) DO NOTHING", tuple(cols.values()))
                if status == "failed":
                    events.emit(conn, batch_id, "file.failed",
                                {"file_id": fid, "error_code": err, "message": message_for(err),
                                 "retryable_by_user": retryable_by_user(err)}, fid)
                elif status == "completed":
                    events.emit(conn, batch_id, "file.completed",
                                {"file_id": fid, "result_url": f"/files/{fid}/result"}, fid)
            events.emit_summary(conn, batch_id)
            out = _batch_view(conn, batch_id)
        log("batch.created", batch_id=batch_id, user_id=uid, files=len(body.files),
            request_id=request.state.request_id)
        return 201, out

    return idempotent(request, body.model_dump(), run)


def _batch_view(conn, batch_id: str) -> dict:
    b = conn.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)).fetchone()
    files = conn.execute("SELECT * FROM files WHERE batch_id = ? ORDER BY rowid",
                         (batch_id,)).fetchall()
    confirmed = {r["file_id"]: r["b"] for r in conn.execute(
        "SELECT file_id, SUM(size_bytes) AS b FROM file_parts WHERE file_id IN "
        "(SELECT id FROM files WHERE batch_id = ?) GROUP BY file_id", (batch_id,))}
    return {"id": b["id"], "created_at": b["created_at"], "seq": b["last_seq"],
            "summary": events.summary_counts(conn, batch_id),
            "files": [file_view(f, confirmed.get(f["id"], 0)) for f in files]}


@router.get("/batches/{batch_id}")
def get_batch(batch_id: str, request: Request):
    with db.read() as conn:
        b = conn.execute("SELECT user_id FROM batches WHERE id = ?", (batch_id,)).fetchone()
        if b is None or b["user_id"] != user_id(request):
            raise ApiError(404, "NOT_FOUND")
        return _batch_view(conn, batch_id)


@router.get("/files")
def list_files(request: Request, unfinished: bool = False):
    """The next visit lists unfinished files and offers Resume."""
    q = "SELECT * FROM files WHERE user_id = ?"
    if unfinished:
        q += " AND status IN ('pending', 'uploading')"
    with db.read() as conn:
        rows = conn.execute(q + " ORDER BY created_at DESC LIMIT 500", (user_id(request),)).fetchall()
    return {"files": [file_view(f) for f in rows]}


@router.get("/files/{file_id}")
def get_file(file_id: str, request: Request):
    with db.read() as conn:
        return file_view(load_file(conn, request, file_id))


# ------------------------------------------------------------------ uploads

@router.post("/files/{file_id}/multipart")
def start_multipart(file_id: str, request: Request):
    s = get_settings()
    store = adapters.get().store
    with db.read() as conn:
        f = load_file(conn, request, file_id)
    if f["status"] not in ("pending", "uploading"):
        raise ApiError(409, "INVALID_STATE")

    if upload_mode(f["size_bytes"]) == "single":
        with db.tx() as conn:
            f = state.get_file(conn, file_id)
            if f["status"] == "pending":
                state.transition(conn, f, "uploading")
        return {"mode": "single", "url": store.presign_put(f["s3_key"], s.presign_ttl_s),
                "expires_at": clock.now() + s.presign_ttl_s, "part_size": s.chunk_size, "part_count": 1}

    upload_id = f["s3_upload_id"]
    if upload_id is None:
        # CreateMultipartUpload once; if a concurrent call wins, abort ours and use the stored id.
        created = store.create_multipart(f["s3_key"])
        with db.tx() as conn:
            f = state.get_file(conn, file_id)
            if f["s3_upload_id"] is None and f["status"] in ("pending", "uploading"):
                state.transition(conn, f, "uploading", s3_upload_id=created)
                upload_id = created
            else:
                upload_id = f["s3_upload_id"]
        if upload_id != created:
            store.abort_multipart(f["s3_key"], created)
    return {"mode": "multipart", "upload_id": upload_id, "part_size": s.chunk_size,
            "part_count": part_count(f["size_bytes"])}


@router.post("/files/{file_id}/parts:sign")
def sign_parts(file_id: str, body: SignIn, request: Request):
    s = get_settings()
    if len(body.part_numbers) > s.max_sign_page:
        raise ApiError(400, "VALIDATION_ERROR", f"Sign at most {s.max_sign_page} parts per call.")
    with db.read() as conn:
        f = load_file(conn, request, file_id)
    if f["status"] != "uploading" or not f["s3_upload_id"]:
        raise ApiError(409, "INVALID_STATE", "Start the multipart upload first.")
    n_parts = part_count(f["size_bytes"])
    bad = [n for n in body.part_numbers if not 1 <= n <= n_parts]
    if bad:
        raise ApiError(400, "VALIDATION_ERROR", f"Part numbers must be between 1 and {n_parts}.")
    limit(request, "part_signing", cost=len(body.part_numbers))
    store = adapters.get().store
    return {"urls": {str(n): store.presign_part(f["s3_key"], f["s3_upload_id"], n, s.presign_ttl_s)
                     for n in body.part_numbers},
            "expires_at": clock.now() + s.presign_ttl_s}


def _expected_part_size(size: int, n: int) -> int:
    chunk = get_settings().chunk_size
    return min(chunk, size - (n - 1) * chunk)


@router.post("/files/{file_id}/parts/{part_number}")
def report_part(file_id: str, part_number: int, body: PartIn, request: Request):
    with db.read() as conn:
        f = load_file(conn, request, file_id)
    if f["status"] != "uploading" or not f["s3_upload_id"]:
        raise ApiError(409, "INVALID_STATE")
    if not 1 <= part_number <= part_count(f["size_bytes"]):
        raise ApiError(400, "VALIDATION_ERROR", "Invalid part number.")
    # Trust but verify against the store (S3 ListParts).
    stored = adapters.get().store.list_parts(f["s3_key"], f["s3_upload_id"]).get(part_number)
    if stored is None or stored[0] != body.etag.strip('"') or stored[1] != body.size_bytes:
        raise ApiError(400, "VALIDATION_ERROR", "Part not found in storage or ETag/size mismatch.")
    if body.size_bytes != _expected_part_size(f["size_bytes"], part_number):
        raise ApiError(400, "VALIDATION_ERROR", "Unexpected part size.")
    with db.tx() as conn:
        conn.execute(
            "INSERT INTO file_parts (file_id, part_number, etag, size_bytes, checksum, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (file_id, part_number) DO UPDATE SET "
            "etag=excluded.etag, size_bytes=excluded.size_bytes, checksum=excluded.checksum, "
            "updated_at=excluded.updated_at",
            (file_id, part_number, body.etag.strip('"'), body.size_bytes, body.checksum, clock.now()))
        confirmed = conn.execute("SELECT COALESCE(SUM(size_bytes), 0) FROM file_parts WHERE file_id = ?",
                                 (file_id,)).fetchone()[0]
        pct = round(100.0 * confirmed / max(1, f["size_bytes"]), 2)
        conn.execute("UPDATE files SET progress_pct = ?, updated_at = ? WHERE id = ?", (pct, clock.now(), file_id))
    return {"part_number": part_number, "confirmed_bytes": confirmed, "progress_pct": pct}


@router.get("/files/{file_id}/parts")
def list_parts(file_id: str, request: Request):
    with db.read() as conn:
        f = load_file(conn, request, file_id)
        parts = conn.execute("SELECT part_number, etag, size_bytes FROM file_parts WHERE file_id = ? "
                             "ORDER BY part_number", (file_id,)).fetchall()
    n = part_count(f["size_bytes"])
    have = {p["part_number"] for p in parts}
    return {"status": f["status"], "upload_id": f["s3_upload_id"], "part_size": get_settings().chunk_size,
            "part_count": n, "parts": [dict(p) for p in parts],
            "missing": [i for i in range(1, n + 1) if i not in have],
            "confirmed_bytes": sum(p["size_bytes"] for p in parts)}


def _fail_upload(file_id: str, code: str) -> None:
    with db.tx() as conn:
        f = state.get_file(conn, file_id)
        if f["status"] != "uploading":
            return
        state.transition(conn, f, "failed", error_code=code, error_message=message_for(code),
                         retryable_by_user=int(retryable_by_user(code)))
        events.emit(conn, f["batch_id"], "file.failed",
                    {"file_id": file_id, "error_code": code, "message": message_for(code),
                     "retryable_by_user": retryable_by_user(code)}, file_id)
        events.emit_summary(conn, f["batch_id"])


@router.post("/files/{file_id}/complete")
def complete(file_id: str, request: Request):
    store = adapters.get().store
    with db.read() as conn:
        f = load_file(conn, request, file_id)
        parts = conn.execute("SELECT part_number, etag FROM file_parts WHERE file_id = ? ORDER BY part_number",
                             (file_id,)).fetchall()
    if f["status"] not in ("pending", "uploading"):
        if f["status"] == "failed" or f["status"] == "cancelled":
            raise ApiError(409, "INVALID_STATE")
        return file_view(f)  # already completed by an earlier call

    try:
        if upload_mode(f["size_bytes"]) == "multipart":
            n = part_count(f["size_bytes"])
            if not f["s3_upload_id"] or len(parts) != n:
                raise ApiError(409, "UPLOAD_INCOMPLETE", retryable=True)
            store.complete_multipart(f["s3_key"], f["s3_upload_id"], [(p["part_number"], p["etag"]) for p in parts])
    except S3Error as e:
        log("upload.complete_failed", file_id=file_id, error=str(e))
        raise ApiError(409, "UPLOAD_INCOMPLETE", retryable=True) from None

    size = store.head(f["s3_key"])
    if size is None:
        raise ApiError(409, "UPLOAD_INCOMPLETE", retryable=True)
    if size != f["size_bytes"]:
        _fail_upload(file_id, "SIZE_MISMATCH")
        raise ApiError(422, "SIZE_MISMATCH", retryable=True)
    if stream_sha256(store, f["s3_key"], size) != f["sha256"]:
        _fail_upload(file_id, "CHECKSUM_MISMATCH")
        raise ApiError(422, "CHECKSUM_MISMATCH", retryable=True)

    with db.tx() as conn:
        f = state.get_file(conn, file_id)
        if f["status"] in ("pending", "uploading"):
            # uploaded + "enqueue scan" in one transaction (outbox)
            state.transition(conn, f, "uploaded", progress_pct=0.0, stage=None)
            events.enqueue(conn, "scan", file_id)
            events.emit(conn, f["batch_id"], "file.stage",
                        {"file_id": file_id, "stage": "uploaded", "progress_pct": 0.0}, file_id)
        f = state.get_file(conn, file_id)
    log("upload.completed", file_id=file_id, batch_id=f["batch_id"], request_id=request.state.request_id)
    return file_view(f)


# ------------------------------------------------------------------ user actions

@router.post("/files/{file_id}/retry")
def retry(file_id: str, request: Request):
    def run() -> tuple[int, dict]:
        store = adapters.get().store
        with db.tx() as conn:
            f = load_file(conn, request, file_id)
            if f["status"] != "failed":
                raise ApiError(409, "INVALID_STATE", "Only failed files can be retried.")
            if not f["retryable_by_user"]:
                raise ApiError(409, "NOT_RETRYABLE")
            has_jobs = conn.execute("SELECT 1 FROM jobs WHERE file_id = ?", (file_id,)).fetchone()
            clear = {"error_code": None, "error_message": None, "retryable_by_user": 0}
            if f["error_code"] in UPLOAD_ERROR_CODES or not has_jobs:
                # Upload problem: re-upload this file only, from scratch.
                old_upload = f["s3_upload_id"]
                conn.execute("DELETE FROM file_parts WHERE file_id = ?", (file_id,))
                state.transition(conn, f, "pending", s3_upload_id=None, progress_pct=0.0, **clear)
            else:
                # Reset only the failed stage(s); completed stages are kept.
                old_upload = None
                conn.execute(
                    "UPDATE jobs SET status='queued', attempt=0, next_run_at=?, locked_until=NULL, lease_id=NULL, "
                    "updated_at=? WHERE file_id=? AND status IN ('failed', 'dead', 'skipped')",
                    (clock.now(), clock.now(), file_id))
                state.transition(conn, f, "processing", progress_pct=state.progress(conn, file_id), **clear)
            f = state.get_file(conn, file_id)
            events.emit(conn, f["batch_id"], "file.stage",
                        {"file_id": file_id, "stage": f["stage"] or f["status"], "progress_pct": f["progress_pct"]},
                        file_id)
            events.emit_summary(conn, f["batch_id"])
        if old_upload:
            store.abort_multipart(f["s3_key"], old_upload)
        log("file.retry", file_id=file_id, request_id=request.state.request_id)
        return 200, file_view(f)

    return idempotent(request, {"file_id": file_id}, run, required=False)


@router.post("/files/{file_id}/cancel")
def cancel(file_id: str, request: Request):
    with db.tx() as conn:
        f = load_file(conn, request, file_id)
        if f["status"] in events.FINAL and f["status"] != "failed":
            return file_view(f)
        upload_id = f["s3_upload_id"] if f["status"] == "uploading" else None
        state.transition(conn, f, "cancelled")
        conn.execute("UPDATE jobs SET status='cancelled', lease_id=NULL, updated_at=? "
                     "WHERE file_id=? AND status NOT IN ('done')", (clock.now(), file_id))
        events.emit(conn, f["batch_id"], "file.cancelled", {"file_id": file_id}, file_id)
        events.emit_summary(conn, f["batch_id"])
        f = state.get_file(conn, file_id)
    if upload_id:
        adapters.get().store.abort_multipart(f["s3_key"], upload_id)
    return file_view(f)


@router.get("/files/{file_id}/result")
def result(file_id: str, request: Request):
    s = get_settings()
    store = adapters.get().store
    with db.read() as conn:
        f = load_file(conn, request, file_id)
    if f["status"] != "completed":
        raise ApiError(409, "INVALID_STATE", "This file isn't finished yet.")
    res = json.loads(f["result_json"] or "{}")
    thumb = (res.get("thumbnail") or {}).get("thumbnail_key")
    return {"file": file_view(f), "result": res,
            "download_url": store.presign_get(f["s3_key"], s.presign_ttl_s),
            "thumbnail_url": store.presign_get(thumb, s.presign_ttl_s) if thumb else None}
