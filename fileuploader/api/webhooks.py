"""POST /webhooks/processor — completion signal from the external processing API.

Verified by HMAC signature, deduped by the vendor's `event_id`, and any status
older than the stored one is ignored. It runs the same transaction as the
status poller (worker.apply_vendor_update), so whichever arrives first
completes the job and the other finds it already done.
"""
from __future__ import annotations

import asyncio
import hmac
import json

from fastapi import APIRouter, Request

from .. import worker
from ..adapters.vendors import sign_webhook
from ..config import get_settings
from ..errors import ApiError
from ..log import log

router = APIRouter()


@router.post("/webhooks/processor")
async def processor_webhook(request: Request):
    body = await request.body()
    expected = sign_webhook(get_settings().processor_webhook_secret, body)
    if not hmac.compare_digest(request.headers.get("X-Processor-Signature", ""), expected):
        raise ApiError(401, "INVALID_SIGNATURE")
    try:
        evt = json.loads(body)
        job_id, event_id = str(evt["job_id"]), str(evt["event_id"])
    except (ValueError, KeyError, TypeError):
        raise ApiError(400, "VALIDATION_ERROR") from None
    outcome = await asyncio.to_thread(worker.apply_vendor_update, job_id, evt, event_id)
    log("webhook.processor", vendor_job_id=job_id, event_id=event_id, status=evt.get("status"), outcome=outcome)
    return {"ok": True, "outcome": outcome}
