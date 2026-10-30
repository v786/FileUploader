"""Mock S3 (stands in for MinIO): a filesystem-backed object store with the S3
features we rely on — multipart uploads, presigned URLs, ETags, per-part
SHA-256 checksums and ranged reads. Browsers upload to it directly through the
/mock-s3 routes, so file bytes never touch the API handlers.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import shutil
import time
import uuid
from pathlib import Path
from urllib.parse import quote, urlencode


class S3Error(Exception):
    def __init__(self, status: int, code: str, message: str = "") -> None:
        super().__init__(f"{code}: {message}")
        self.status = status
        self.code = code


class LocalObjectStore:
    def __init__(self, root: str, bucket: str, public_base_url: str, secret: str) -> None:
        self.root = Path(root)
        self.bucket = bucket
        self.base = public_base_url.rstrip("/")
        self.secret = secret.encode()
        (self.root / bucket).mkdir(parents=True, exist_ok=True)
        (self.root / ".multipart").mkdir(parents=True, exist_ok=True)

    # ---- paths ----
    def _obj(self, key: str) -> Path:
        p = (self.root / self.bucket / key).resolve()
        if not str(p).startswith(str((self.root / self.bucket).resolve())):
            raise S3Error(400, "InvalidKey")
        return p

    def _mp(self, upload_id: str) -> Path:
        if not upload_id.isalnum():
            raise S3Error(404, "NoSuchUpload")
        return self.root / ".multipart" / upload_id

    def _meta(self, upload_id: str) -> dict:
        try:
            return json.loads((self._mp(upload_id) / "meta.json").read_text())
        except FileNotFoundError:
            raise S3Error(404, "NoSuchUpload") from None

    # ---- signing ----
    def _sig(self, method: str, key: str, expires: int, upload_id: str = "", part: int = 0) -> str:
        msg = f"{method}\n{self.bucket}\n{key}\n{expires}\n{upload_id}\n{part}".encode()
        return hmac.new(self.secret, msg, hashlib.sha256).hexdigest()

    def _url(self, method: str, key: str, ttl_s: int, upload_id: str = "", part: int = 0) -> str:
        expires = int(time.time()) + ttl_s
        q = {"X-Expires": expires, "X-Signature": self._sig(method, key, expires, upload_id, part)}
        if upload_id:
            q = {"uploadId": upload_id, "partNumber": part, **q}
        return f"{self.base}/mock-s3/{self.bucket}/{quote(key)}?{urlencode(q)}"

    def verify(self, method: str, key: str, expires: int, signature: str, upload_id: str = "", part: int = 0) -> None:
        if expires < time.time():
            raise S3Error(403, "AccessDenied", "Request has expired")
        if not hmac.compare_digest(self._sig(method, key, expires, upload_id, part), signature):
            raise S3Error(403, "SignatureDoesNotMatch")

    def presign_part(self, key: str, upload_id: str, part_number: int, ttl_s: int) -> str:
        return self._url("PUT", key, ttl_s, upload_id, part_number)

    def presign_put(self, key: str, ttl_s: int) -> str:
        return self._url("PUT", key, ttl_s)

    def presign_get(self, key: str, ttl_s: int) -> str:
        return self._url("GET", key, ttl_s)

    # ---- multipart ----
    def create_multipart(self, key: str) -> str:
        upload_id = uuid.uuid4().hex
        d = self._mp(upload_id)
        d.mkdir(parents=True)
        (d / "meta.json").write_text(json.dumps({"key": key, "created_at": time.time()}))
        return upload_id

    def upload_part(self, key: str, upload_id: str, part_number: int, data: bytes,
                    checksum_sha256_b64: str | None = None) -> str:
        meta = self._meta(upload_id)
        if meta["key"] != key:
            raise S3Error(400, "InvalidRequest", "key does not match upload")
        if not 1 <= part_number <= 10000:
            raise S3Error(400, "InvalidArgument", "part number")
        if checksum_sha256_b64 is not None:
            actual = base64.b64encode(hashlib.sha256(data).digest()).decode()
            if actual != checksum_sha256_b64:
                raise S3Error(400, "BadDigest", "x-amz-checksum-sha256 mismatch")
        etag = hashlib.md5(data).hexdigest()
        tmp = self._mp(upload_id) / f"{part_number}.tmp"
        tmp.write_bytes(data)
        os.replace(tmp, self._mp(upload_id) / f"{part_number}.part")  # a re-sent part replaces itself
        (self._mp(upload_id) / f"{part_number}.etag").write_text(etag)
        return etag

    def list_parts(self, key: str, upload_id: str) -> dict[int, tuple[str, int]]:
        self._meta(upload_id)
        out = {}
        for p in self._mp(upload_id).glob("*.part"):
            try:
                etag = p.with_suffix(".etag").read_text()
            except FileNotFoundError:
                continue  # part still being written
            out[int(p.stem)] = (etag, p.stat().st_size)
        return out

    def complete_multipart(self, key: str, upload_id: str, parts: list[tuple[int, str]]) -> str:
        try:
            self._meta(upload_id)
        except S3Error:
            # Already completed by an earlier (crashed or retried) call: same object is there.
            if self.head(key) is not None:
                return self._etag_of(key)
            raise
        existing = self.list_parts(key, upload_id)
        for n, etag in parts:
            if n not in existing or existing[n][0] != etag.strip('"'):
                raise S3Error(400, "InvalidPart", f"part {n}")
        dest = self._obj(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".tmp")
        md5s = b""
        with open(tmp, "wb") as out:
            for n, _ in sorted(parts):
                src = self._mp(upload_id) / f"{n}.part"
                md5s += bytes.fromhex(existing[n][0])
                with open(src, "rb") as part:
                    shutil.copyfileobj(part, out, 1024 * 1024)
        os.replace(tmp, dest)
        shutil.rmtree(self._mp(upload_id), ignore_errors=True)
        etag = f"{hashlib.md5(md5s).hexdigest()}-{len(parts)}"
        dest.with_name(dest.name + ".etag").write_text(etag)
        return etag

    def abort_multipart(self, key: str, upload_id: str) -> None:
        shutil.rmtree(self._mp(upload_id), ignore_errors=True)

    # ---- objects ----
    def put(self, key: str, data: bytes, checksum_sha256_b64: str | None = None) -> str:
        if checksum_sha256_b64 is not None:
            actual = base64.b64encode(hashlib.sha256(data).digest()).decode()
            if actual != checksum_sha256_b64:
                raise S3Error(400, "BadDigest")
        dest = self._obj(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, dest)  # deterministic key: re-upload overwrites identical bytes
        etag = hashlib.md5(data).hexdigest()
        dest.with_name(dest.name + ".etag").write_text(etag)
        return etag

    def _etag_of(self, key: str) -> str:
        p = self._obj(key)
        try:
            return p.with_name(p.name + ".etag").read_text()
        except FileNotFoundError:
            return ""

    def head(self, key: str) -> int | None:
        p = self._obj(key)
        return p.stat().st_size if p.is_file() else None

    def get_range(self, key: str, start: int, end: int) -> bytes:
        """Inclusive byte range, like an HTTP Range GET."""
        p = self._obj(key)
        if not p.is_file():
            raise S3Error(404, "NoSuchKey")
        with open(p, "rb") as f:
            f.seek(start)
            return f.read(max(0, end - start + 1))

    def iter_multipart_uploads(self):
        for d in (self.root / ".multipart").iterdir():
            try:
                yield d.name, json.loads((d / "meta.json").read_text())
            except (FileNotFoundError, json.JSONDecodeError):
                continue


def stream_sha256(store, key: str, size: int, chunk: int = 1024 * 1024) -> str:
    """Hash an object with ranged GETs so no stage holds a whole file in memory."""
    h = hashlib.sha256()
    pos = 0
    while pos < size:
        h.update(store.get_range(key, pos, min(size, pos + chunk) - 1))
        pos += chunk
    return h.hexdigest()
