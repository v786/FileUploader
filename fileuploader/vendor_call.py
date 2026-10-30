"""The single wrapper every vendor call goes through. In order:

idempotency cache check -> breaker check -> (retry budget) -> rate-limit token
-> timeout -> call -> classify error -> record breaker outcome -> cache result

Status polls pass `cache=False` (each poll must really ask) and their own rate-limit
`bucket`, so polls can never use up the submit quota; they still share the vendor's
breaker.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Awaitable, Callable

from . import breaker, clock, db, ratelimit
from .errors import (RateLimited, RetryBudgetExceeded, VendorHTTPError, VendorTimeout,
                     is_transient_vendor_error)
from .ids import sha256_hex
from .log import log


def vendor_key(file_id: str, stage: str, vendor: str) -> str:
    return sha256_hex(file_id, stage, vendor)


def cached(key: str) -> dict | None:
    with db.read() as conn:
        row = conn.execute("SELECT response_json FROM vendor_calls WHERE idempotency_key = ?", (key,)).fetchone()
    return json.loads(row["response_json"]) if row else None


async def call_vendor(vendor: str, key: str, fn: Callable[[], Awaitable[dict]], *,
                      timeout_s: float, is_retry: bool = False, request: Any = None,
                      bucket: str | None = None, cache: bool = True) -> dict:
    if cache:
        hit = cached(key)
        if hit is not None:
            return hit
    bucket = bucket or vendor

    permit = breaker.before_call(vendor)  # raises BreakerOpen

    if is_retry and not breaker.retry_budget_ok(vendor):
        breaker.release(permit)
        raise RetryBudgetExceeded(vendor, wait_s=1.0)

    wait = ratelimit.acquire_vendor(bucket)
    if wait > 0:
        breaker.release(permit)
        raise RateLimited(bucket, wait)

    try:
        result = await asyncio.wait_for(fn(), timeout=timeout_s)
    except (asyncio.TimeoutError, TimeoutError):
        breaker.record(permit, ok=False, timeout=True, is_retry=is_retry)
        log("vendor.timeout", vendor=vendor, timeout_s=timeout_s)
        raise VendorTimeout(vendor) from None
    except Exception as exc:
        transient = is_transient_vendor_error(exc)
        breaker.record(permit, ok=False, counts=transient, is_retry=is_retry)
        if isinstance(exc, VendorHTTPError) and exc.status == 429:
            ratelimit.drain([], exc.retry_after_s or 1.0, ratelimit.vendor_specs(bucket))
        log("vendor.error", vendor=vendor, error=str(exc), transient=transient)
        raise
    breaker.record(permit, ok=True, is_retry=is_retry)
    if not cache:
        return result

    with db.tx() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO vendor_calls (idempotency_key, vendor, request_hash, response_json, status, "
            "created_at) VALUES (?, ?, ?, ?, 'ok', ?)",
            (key, vendor, sha256_hex(json.dumps(request, sort_keys=True, default=str)), json.dumps(result),
             clock.now()),
        )
    return result
