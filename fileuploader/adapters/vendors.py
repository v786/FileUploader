"""Mock vendors: scanner, extractor, thumbnailer and the external processor.

They are not always-succeed stubs. Each reads a fault profile (env
MOCK_<VENDOR>_FAULTS as JSON, or POST /__faults/<vendor> at runtime) so the
resilience code is exercised:

  latency_ms, latency_jitter_ms  slow responses (timeouts)
  error_rate, error_status       flakiness (retries)
  rate_limit_rps                 429 + Retry-After above this rate (limiter)
  down: true                     connection refused (breaker, waiting_vendor)
  hang: true                     never responds (aborts)
  fail_file_ids: [...]           permanent 4xx for chosen files (isolation)

The processor (async job API + webhooks) also takes:

  duration_ms, queue_ms          how long a job runs, and waits in the vendor's queue first
  progress_mode                  percent | states | none — what status reports carry
  progress_curve                 linear | ease_out
  webhook_delay_ms               late webhooks
  webhook_duplicate_rate         the same event delivered twice
  webhook_out_of_order           due webhooks delivered newest first
  webhook_drop_rate              webhooks never sent (proves the polling fallback)

Profiles live in a small JSON file next to the object store so that a fault
set through the API process is seen by worker processes too.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import math
import os
import random
import threading
import uuid
from collections import deque
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs, unquote, urlsplit

from .. import clock, db
from ..errors import VendorConnectionError, VendorHTTPError
from ..filetypes import family
from ..log import log
from .object_store import S3Error

VENDORS = ("scanner", "extractor", "thumbnailer", "processor")
FAULT_KEYS = {"latency_ms", "latency_jitter_ms", "error_rate", "error_status", "rate_limit_rps",
              "down", "hang", "fail_file_ids"}
PROCESSOR_FAULT_KEYS = {"duration_ms", "queue_ms", "progress_mode", "progress_curve", "webhook_delay_ms",
                        "webhook_duplicate_rate", "webhook_out_of_order", "webhook_drop_rate"}
PROGRESS_MODES = ("percent", "states", "none")

EICAR = b"X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*"

# 1x1 transparent PNG
PLACEHOLDER_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
    "0000000d4944415478da63f8ffff3f0005fe02fea7d6a4c20000000049454e44ae426082"
)


class FaultStore:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()
        self._cache: tuple[float, dict] | None = None

    def all(self) -> dict[str, dict]:
        base = {v: json.loads(os.environ.get(f"MOCK_{v.upper()}_FAULTS", "{}")) for v in VENDORS}
        try:
            mtime = self.path.stat().st_mtime
        except FileNotFoundError:
            return base
        with self._lock:
            if self._cache is None or self._cache[0] != mtime:
                self._cache = (mtime, json.loads(self.path.read_text() or "{}"))
            for v, prof in self._cache[1].items():
                base[v] = prof
        return base

    def get(self, vendor: str) -> dict:
        return self.all().get(vendor, {})

    def set(self, vendor: str, profile: dict) -> dict:
        unknown = set(profile) - FAULT_KEYS - (PROCESSOR_FAULT_KEYS if vendor == "processor" else set())
        if unknown:
            raise ValueError(f"unknown fault keys: {sorted(unknown)}")
        if profile.get("progress_mode", "percent") not in PROGRESS_MODES:
            raise ValueError(f"progress_mode must be one of {PROGRESS_MODES}")
        with self._lock:
            current = json.loads(self.path.read_text() or "{}") if self.path.exists() else {}
            current[vendor] = profile
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(current))
            os.replace(tmp, self.path)
            self._cache = None
        return profile


class _MockVendor:
    name = ""

    def __init__(self, faults: FaultStore, store) -> None:
        self.faults = faults
        self.store = store
        self._hits: deque[float] = deque()
        self._hits_lock = threading.Lock()
        self.calls = 0                      # observability for tests
        self.seen_keys: dict[str, int] = {}  # idempotency keys received

    async def _inject(self, file_id: str, idempotency_key: str | None) -> None:
        if idempotency_key is not None:
            self.calls += 1
            self.seen_keys[idempotency_key] = self.seen_keys.get(idempotency_key, 0) + 1
        f = self.faults.get(self.name)
        if f.get("down"):
            raise VendorConnectionError(f"{self.name}: connection refused")
        if f.get("hang"):
            await asyncio.Event().wait()
        rps = f.get("rate_limit_rps")
        if rps:
            now = clock.now()
            with self._hits_lock:
                while self._hits and now - self._hits[0] > 1.0:
                    self._hits.popleft()
                if len(self._hits) >= rps:
                    raise VendorHTTPError(429, {"code": "RATE_LIMITED"}, retry_after_s=1.0)
                self._hits.append(now)
        latency = f.get("latency_ms", 0) + random.uniform(0, f.get("latency_jitter_ms", 0))
        if latency:
            await asyncio.sleep(latency / 1000)
        if f.get("error_rate") and random.random() < f["error_rate"]:
            raise VendorHTTPError(int(f.get("error_status", 503)))
        if idempotency_key is not None and file_id in f.get("fail_file_ids", []):
            raise VendorHTTPError(422, {"code": "CORRUPT_FILE"})


class MockScanner(_MockVendor):
    name = "scanner"

    async def scan(self, *, file_id: str, key: str, name: str, idempotency_key: str) -> dict:
        await self._inject(file_id, idempotency_key)
        if "EICAR" in name.upper():
            return {"verdict": "infected", "signature": "EICAR-Test-File"}
        # Stream the object with ranged GETs; never load it whole.
        size = self.store.head(key) or 0
        chunk, overlap, pos = 1024 * 1024, len(EICAR), 0
        while pos < size:
            data = await asyncio.to_thread(self.store.get_range, key, pos, min(size, pos + chunk + overlap) - 1)
            if EICAR in data:
                return {"verdict": "infected", "signature": "EICAR-Test-File"}
            pos += chunk
        return {"verdict": "clean"}


CANNED = {
    "pdf": {"kind": "document", "pages": 3, "text": "Lorem ipsum dolor sit amet."},
    "image": {"kind": "image", "width": 1024, "height": 768, "exif": {}},
    "text": {"kind": "text", "lines": 42, "encoding": "utf-8"},
    "office": {"kind": "document", "pages": 1, "text": "Quarterly summary"},
    "video": {"kind": "video", "duration_s": 12.5, "codec": "h264"},
    "other": {"kind": "binary"},
}


class MockExtractor(_MockVendor):
    name = "extractor"

    async def extract(self, *, file_id: str, key: str, name: str, size_bytes: int, idempotency_key: str) -> dict:
        await self._inject(file_id, idempotency_key)
        return {**CANNED[family(name)], "size_bytes": size_bytes, "name": name}


class MockThumbnailer(_MockVendor):
    name = "thumbnailer"

    async def thumbnail(self, *, file_id: str, key: str, name: str, idempotency_key: str) -> dict:
        await self._inject(file_id, idempotency_key)
        thumb_key = f"thumbnails/{key}.png"
        await asyncio.to_thread(self.store.put, thumb_key, PLACEHOLDER_PNG)  # idempotent overwrite
        return {"thumbnail_key": thumb_key, "width": 1, "height": 1}


def sign_webhook(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


class MockProcessor(_MockVendor):
    """External processing API with an async job model.

    POST /jobs  -> submit(): idempotent on the key, pulls the file from the presigned GET URL
    GET /jobs/x -> status(): queued (with queue position) -> running (percent) -> done (result)
    Webhooks    -> deliver_due(): HMAC-signed status changes sent to our /webhooks/processor
    """
    name = "processor"

    def __init__(self, faults: FaultStore, store, secret: str, webhook_url: str) -> None:
        super().__init__(faults, store)
        self.secret = secret
        self.webhook_url = webhook_url
        self.sender: Callable[[bytes, dict], int] | None = None  # default: HTTP POST to webhook_url
        self.polls = 0
        self.webhooks_sent = 0

    def _check_source(self, source_url: str) -> None:
        """The vendor pulls the object with the presigned GET; an expired URL is rejected."""
        u = urlsplit(source_url)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        prefix = f"/mock-s3/{self.store.bucket}/"
        if not u.path.startswith(prefix):
            raise VendorHTTPError(400, {"code": "SOURCE_URL_INVALID"})
        key = unquote(u.path[len(prefix):])
        try:
            self.store.verify("GET", key, int(q.get("X-Expires", "0")), q.get("X-Signature", ""))
        except S3Error as e:
            code = "SOURCE_URL_EXPIRED" if e.code == "AccessDenied" else "SOURCE_URL_INVALID"
            raise VendorHTTPError(400, {"code": code}) from None
        if self.store.head(key) is None:
            raise VendorHTTPError(400, {"code": "SOURCE_URL_INVALID"})

    async def submit(self, *, file_id: str, name: str, source_url: str, idempotency_key: str) -> dict:
        await self._inject(file_id, idempotency_key)
        self._check_source(source_url)
        with db.tx() as conn:
            conn.execute("INSERT OR IGNORE INTO mock_processor_jobs (job_id, idempotency_key, file_id, name, "
                         "created_at) VALUES (?, ?, ?, ?, ?)",
                         (f"pj_{uuid.uuid4().hex[:16]}", idempotency_key, file_id, name, clock.now()))
            row = conn.execute("SELECT job_id FROM mock_processor_jobs WHERE idempotency_key = ?",
                               (idempotency_key,)).fetchone()
        return {"job_id": row["job_id"], "status": "queued"}

    async def status(self, *, job_id: str) -> dict:
        self.polls += 1
        with db.read() as conn:
            row = conn.execute("SELECT * FROM mock_processor_jobs WHERE job_id = ?", (job_id,)).fetchone()
        await self._inject(row["file_id"] if row else "", None)
        if row is None:
            raise VendorHTTPError(404, {"code": "NOT_FOUND"})
        return self._snapshot(row, clock.now(), self.faults.get(self.name))

    def _snapshot(self, row, now: float, f: dict) -> dict:
        queue_s = f.get("queue_ms", 0) / 1000
        run_s = f.get("duration_ms", 2000) / 1000
        mode = f.get("progress_mode", "percent")
        elapsed = now - row["created_at"]
        out: dict = {"job_id": row["job_id"], "at": now}
        if elapsed < queue_s:
            out["status"] = "queued"
            if mode != "none":
                out["queue_position"] = max(1, math.ceil(queue_s - elapsed))
        elif elapsed < queue_s + run_s:
            out["status"] = "running"
            if mode == "percent":
                x = (elapsed - queue_s) / run_s
                if f.get("progress_curve") == "ease_out":
                    x = 1 - (1 - x) ** 2
                out["progress_pct"] = round(100 * x, 1)
        else:
            out["status"] = "done"
            if mode == "percent":
                out["progress_pct"] = 100.0
            out["result"] = {"kind": "processed", "family": family(row["name"]),
                             "summary": f"Processed {row['name']}", "pages": CANNED[family(row["name"])].get("pages")}
        return out

    def deliver_due(self) -> int:
        """Generate webhooks for status changes, then send the ones that are due."""
        f = self.faults.get(self.name)
        if f.get("down"):
            return 0
        now = clock.now()
        with db.tx() as conn:
            for row in conn.execute("SELECT * FROM mock_processor_jobs WHERE finished = 0").fetchall():
                snap = self._snapshot(row, now, f)
                mark = snap["status"]
                if snap["status"] == "running" and "progress_pct" in snap:
                    mark += f":{int(snap['progress_pct'] // 10)}"
                if mark == row["last_sent"]:
                    continue
                conn.execute("UPDATE mock_processor_jobs SET last_sent = ?, finished = ? WHERE job_id = ?",
                             (mark, int(snap["status"] in ("done", "failed")), row["job_id"]))
                if random.random() < f.get("webhook_drop_rate", 0):
                    continue
                body = json.dumps({"event_id": f"evt_{uuid.uuid4().hex[:16]}", "type": "job.updated", **snap})
                for _ in range(2 if random.random() < f.get("webhook_duplicate_rate", 0) else 1):
                    conn.execute("INSERT INTO mock_processor_webhooks (deliver_at, body) VALUES (?, ?)",
                                 (now + f.get("webhook_delay_ms", 0) / 1000, body))
            order = "DESC" if f.get("webhook_out_of_order") else "ASC"
            due = conn.execute(f"SELECT id, body FROM mock_processor_webhooks WHERE deliver_at <= ? "
                               f"ORDER BY id {order}", (now,)).fetchall()
            conn.executemany("DELETE FROM mock_processor_webhooks WHERE id = ?", [(r["id"],) for r in due])
        for r in due:
            self._send(r["body"].encode())
        return len(due)

    def _send(self, body: bytes) -> None:
        headers = {"Content-Type": "application/json", "X-Processor-Signature": sign_webhook(self.secret, body)}
        try:
            if self.sender is not None:
                status = self.sender(body, headers)
            else:
                import httpx
                status = httpx.post(self.webhook_url, content=body, headers=headers, timeout=5).status_code
            self.webhooks_sent += 1
            if status >= 300:
                log("mock_processor.webhook_rejected", status=status)
        except Exception as exc:  # noqa: BLE001 - a lost webhook is what polling is for
            log("mock_processor.webhook_error", error=str(exc))
