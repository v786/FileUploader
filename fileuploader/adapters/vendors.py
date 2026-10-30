"""Mock vendors: scanner, extractor, thumbnailer.

They are not always-succeed stubs. Each reads a fault profile (env
MOCK_<VENDOR>_FAULTS as JSON, or POST /__faults/<vendor> at runtime) so the
resilience code is exercised:

  latency_ms, latency_jitter_ms  slow responses (timeouts)
  error_rate, error_status       flakiness (retries)
  rate_limit_rps                 429 + Retry-After above this rate (limiter)
  down: true                     connection refused (breaker, waiting_vendor)
  hang: true                     never responds (aborts)
  fail_file_ids: [...]           permanent 4xx for chosen files (isolation)

Profiles live in a small JSON file next to the object store so that a fault
set through the API process is seen by worker processes too.
"""
from __future__ import annotations

import asyncio
import json
import os
import random
import threading
from .. import clock
from collections import deque
from pathlib import Path

from ..errors import VendorConnectionError, VendorHTTPError

VENDORS = ("scanner", "extractor", "thumbnailer")
FAULT_KEYS = {"latency_ms", "latency_jitter_ms", "error_rate", "error_status", "rate_limit_rps",
              "down", "hang", "fail_file_ids"}

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
        unknown = set(profile) - FAULT_KEYS
        if unknown:
            raise ValueError(f"unknown fault keys: {sorted(unknown)}")
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

    async def _inject(self, file_id: str, idempotency_key: str) -> None:
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
        if file_id in f.get("fail_file_ids", []):
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


def _family(name: str) -> str:
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    if ext == "pdf":
        return "pdf"
    if ext in {"png", "jpg", "jpeg", "gif", "webp"}:
        return "image"
    if ext in {"txt", "csv", "json"}:
        return "text"
    if ext in {"doc", "docx", "xls", "xlsx", "ppt", "pptx"}:
        return "office"
    if ext in {"mp4", "mov"}:
        return "video"
    return "other"


class MockExtractor(_MockVendor):
    name = "extractor"

    async def extract(self, *, file_id: str, key: str, name: str, size_bytes: int, idempotency_key: str) -> dict:
        await self._inject(file_id, idempotency_key)
        return {**CANNED[_family(name)], "size_bytes": size_bytes, "name": name}


class MockThumbnailer(_MockVendor):
    name = "thumbnailer"

    async def thumbnail(self, *, file_id: str, key: str, name: str, idempotency_key: str) -> dict:
        await self._inject(file_id, idempotency_key)
        thumb_key = f"thumbnails/{key}.png"
        await asyncio.to_thread(self.store.put, thumb_key, PLACEHOLDER_PNG)  # idempotent overwrite
        return {"thumbnail_key": thumb_key, "width": 1, "height": 1}
