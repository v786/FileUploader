"""Error contract and classification.

Every error is classified once, where it happens, into a kind; the kind alone
decides what happens next.
"""
from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass


class Kind(str, enum.Enum):
    TRANSIENT = "transient"          # retry with backoff
    DEPENDENCY_DOWN = "dependency"   # breaker open: park as waiting_vendor
    PERMANENT = "permanent"          # stop, mark failed, ask the user
    UNKNOWN = "unknown"              # bug: transient for 3 attempts, then DLQ


# Stable codes -> (user-safe message, retryable_by_user)
CODES: dict[str, tuple[str, bool]] = {
    "FILE_TOO_LARGE": ("This file is larger than the 5 GB limit.", False),
    "UNSUPPORTED_TYPE": ("This file type isn't supported.", False),
    "TYPE_MISMATCH": ("This file's contents don't match its type.", False),
    "DUPLICATE_NAME": ("Another file in this upload has the same name.", False),
    "QUOTA_EXCEEDED": ("This file would take you over your storage quota.", False),
    "TOO_MANY_FILES": ("You can upload up to 50 files at once.", False),
    "CHECKSUM_MISMATCH": ("The uploaded file didn't match the original. Please try again.", True),
    "SIZE_MISMATCH": ("The uploaded file size didn't match. Please try again.", True),
    "MALWARE_DETECTED": ("This file was flagged as malware and can't be processed.", False),
    "CORRUPT_FILE": ("This file appears to be damaged and couldn't be processed.", True),
    "VENDOR_REJECTED": ("We couldn't process this file.", True),
    "VENDOR_UNAVAILABLE": ("A processing service is busy; we'll continue automatically.", True),
    "RETRIES_EXHAUSTED": ("We couldn't process this file after several tries.", True),
    "INTERNAL_ERROR": ("Something went wrong on our side; we've been notified.", True),
    "UPLOAD_EXPIRED": ("This upload was idle too long. Please select the file again.", True),
    "UPLOAD_INCOMPLETE": ("Some parts of this file haven't been uploaded yet.", True),
    "UPLOAD_REQUIRED": ("This file isn't a duplicate after all; please upload it.", True),
    "PROCESSING_FAILED": ("The processing service couldn't process this file.", True),
    "INVALID_SIGNATURE": ("Invalid webhook signature.", False),
    "RATE_LIMITED": ("Too many requests. Please slow down.", True),
    "BUSY": ("Busy — your upload will start shortly.", True),
    "IDEMPOTENCY_KEY_REUSED": ("This request key was already used with different data.", False),
    "IDEMPOTENCY_IN_FLIGHT": ("This request is still being processed.", True),
    "IDEMPOTENCY_KEY_REQUIRED": ("An Idempotency-Key header is required.", False),
    "NOT_FOUND": ("Not found.", False),
    "INVALID_STATE": ("This action isn't possible right now.", False),
    "NOT_RETRYABLE": ("This file can't be retried.", False),
    "VALIDATION_ERROR": ("The request was invalid.", False),
    "UNAUTHENTICATED": ("Missing user identity.", False),
}


# The exact fix a failed file offers, so the user can repair it in place.
FIXES: dict[str, tuple[str, str]] = {
    "UNSUPPORTED_TYPE": ("convert", "Convert to PDF"),
    "TYPE_MISMATCH": ("replace", "Replace file"),
    "FILE_TOO_LARGE": ("split", "Split into parts under 5 GB"),
    "DUPLICATE_NAME": ("rename", "Rename file"),
    "QUOTA_EXCEEDED": ("free_space", "Free up space"),
    "CORRUPT_FILE": ("replace", "Replace file"),
    "MALWARE_DETECTED": ("remove", "Remove"),
    "CHECKSUM_MISMATCH": ("retry", "Retry upload"),
    "SIZE_MISMATCH": ("retry", "Retry upload"),
    "UPLOAD_EXPIRED": ("retry", "Retry upload"),
}

# Vendor 4xx codes that are our side's transient problem, not the file's.
TRANSIENT_VENDOR_CODES = {"SOURCE_URL_EXPIRED"}


def fix_for(code: str) -> dict | None:
    if code in FIXES:
        action, label = FIXES[code]
        return {"action": action, "label": label}
    return {"action": "retry", "label": "Retry"} if retryable_by_user(code) else None


def message_for(code: str) -> str:
    return CODES.get(code, CODES["INTERNAL_ERROR"])[0]


def retryable_by_user(code: str) -> bool:
    return CODES.get(code, (None, True))[1]


class ApiError(Exception):
    """Raised by request handlers; rendered as the one error shape."""

    def __init__(self, status: int, code: str, message: str | None = None,
                 retryable: bool = False, retry_after_ms: int | None = None) -> None:
        super().__init__(code)
        self.status = status
        self.code = code
        self.message = message or message_for(code)
        self.retryable = retryable
        self.retry_after_ms = retry_after_ms

    def body(self, request_id: str | None = None) -> dict:
        return {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "retry_after_ms": self.retry_after_ms,
            "request_id": request_id or uuid.uuid4().hex,
        }


# ---- errors raised inside the pipeline ----

@dataclass
class StageError(Exception):
    kind: Kind
    code: str
    detail: str = ""          # internal only; never sent to the user
    retry_after_s: float | None = None

    def __str__(self) -> str:
        return f"{self.kind.value}:{self.code}:{self.detail}"


class VendorHTTPError(Exception):
    """What a vendor adapter raises for a non-2xx response."""

    def __init__(self, status: int, body: dict | None = None, retry_after_s: float | None = None) -> None:
        super().__init__(f"vendor http {status}")
        self.status = status
        self.body = body or {}
        self.retry_after_s = retry_after_s


class VendorConnectionError(Exception):
    pass


class VendorTimeout(Exception):
    pass


class BreakerOpen(Exception):
    def __init__(self, vendor: str, retry_at: float) -> None:
        super().__init__(f"breaker open for {vendor}")
        self.vendor = vendor
        self.retry_at = retry_at


class RateLimited(Exception):
    def __init__(self, vendor: str, wait_s: float) -> None:
        super().__init__(f"no token for {vendor}")
        self.vendor = vendor
        self.wait_s = wait_s


class RetryBudgetExceeded(Exception):
    def __init__(self, vendor: str, wait_s: float) -> None:
        super().__init__(f"retry budget exhausted for {vendor}")
        self.vendor = vendor
        self.wait_s = wait_s


def is_transient_vendor_error(exc: BaseException) -> bool:
    """Only these count against the breaker: timeouts, connection errors, 5xx, 429."""
    if isinstance(exc, (VendorTimeout, VendorConnectionError)):
        return True
    if isinstance(exc, VendorHTTPError):
        return exc.status >= 500 or exc.status == 429
    return False


def classify(exc: BaseException) -> StageError:
    if isinstance(exc, StageError):
        return exc
    if isinstance(exc, VendorTimeout):
        return StageError(Kind.TRANSIENT, "VENDOR_TIMEOUT", "timeout")
    if isinstance(exc, VendorConnectionError):
        return StageError(Kind.TRANSIENT, "VENDOR_CONNECTION", str(exc))
    if isinstance(exc, VendorHTTPError):
        if exc.status == 429:
            return StageError(Kind.TRANSIENT, "VENDOR_RATE_LIMITED", "429", exc.retry_after_s)
        if exc.status >= 500:
            return StageError(Kind.TRANSIENT, "VENDOR_5XX", str(exc.status))
        code = exc.body.get("code") if isinstance(exc.body, dict) else None
        if code in TRANSIENT_VENDOR_CODES:  # e.g. a presigned URL expired in a backlog: re-sign and retry
            return StageError(Kind.TRANSIENT, code, f"{exc.status} {exc.body}")
        if code not in CODES:
            code = "VENDOR_REJECTED"
        return StageError(Kind.PERMANENT, code, f"{exc.status} {exc.body}")
    if isinstance(exc, BreakerOpen):
        return StageError(Kind.DEPENDENCY_DOWN, "VENDOR_UNAVAILABLE", exc.vendor)
    return StageError(Kind.UNKNOWN, "INTERNAL_ERROR", f"{type(exc).__name__}: {exc}")
