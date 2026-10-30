"""Runtime configuration. Everything is overridable from the environment."""
from __future__ import annotations

import os
import threading
from dataclasses import dataclass, field
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent

MB = 1024 * 1024
GB = 1024 * MB


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass
class StageConfig:
    vendor: str | None
    concurrency: int
    timeout_s: float


@dataclass
class Settings:
    db_path: str = field(default_factory=lambda: _env("DB_PATH", str(ROOT / "data" / "app.db")))
    storage_dir: str = field(default_factory=lambda: _env("STORAGE_DIR", str(ROOT / "data" / "objects")))
    public_base_url: str = field(default_factory=lambda: _env("PUBLIC_BASE_URL", "http://localhost:8000"))
    signing_secret: str = field(default_factory=lambda: _env("SIGNING_SECRET", "dev-secret-change-me"))
    processor_webhook_secret: str = field(
        default_factory=lambda: _env("PROCESSOR_WEBHOOK_SECRET", "dev-webhook-secret-change-me"))
    vendor_mode: str = field(default_factory=lambda: _env("VENDOR_MODE", "mock"))
    limits_path: str = field(default_factory=lambda: _env("LIMITS_PATH", str(ROOT / "config" / "limits.yaml")))
    bucket: str = "uploads"

    # Upload limits
    max_files_per_batch: int = 50
    max_file_size: int = 5 * GB
    chunk_size: int = 8 * MB          # default part size; files smaller than this use one PUT
    min_part_size: int = 5 * MB       # bounds for a client-chosen (adaptive) part size
    max_part_size: int = 64 * MB
    max_parts: int = 10_000
    max_sign_page: int = 100
    presign_ttl_s: int = 15 * 60
    initial_signed_parts: int = 8     # URLs returned by POST /batches so the first bytes move at once
    user_quota_bytes: int = 200 * GB
    allowed_extensions: tuple[str, ...] = (
        ".pdf", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".txt", ".csv",
        ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".zip", ".mp4", ".mov", ".json",
    )

    # Pipeline
    stages: dict[str, StageConfig] = field(default_factory=lambda: {
        "scan": StageConfig("scanner", 8, 30.0),
        "extract": StageConfig("extractor", 4, 60.0),
        "thumbnail": StageConfig("thumbnailer", 6, 45.0),
        "process": StageConfig("processor", 8, 10.0),  # timeout per submit / poll call
        "finalize": StageConfig(None, 16, 5.0),
    })
    # p50 per stage until real samples exist (seconds)
    stage_p50_prior_s: dict[str, float] = field(default_factory=lambda: {
        "scan": 5.0, "extract": 8.0, "thumbnail": 4.0, "process": 20.0, "finalize": 0.5,
    })
    max_active_jobs_per_user: int = 10
    queue_high_water: int = 5000
    job_lock_s: float = 60.0  # stalled-job check: lease length

    # External processing API (async job + webhook)
    process_parallel_with_scan: bool = field(
        default_factory=lambda: _env("PROCESS_PARALLEL_WITH_SCAN", "0") == "1")  # only for a trusted vendor
    process_deadline_s: float = 15 * 60.0
    process_source_url_ttl_s: int = 3600
    process_poll_initial_s: float = 10.0
    process_poll_max_s: float = 60.0
    watch_ttl_s: float = 30.0  # a batch with an open SSE stream counts as watched for this long

    # Retries
    max_attempts: int = 6
    unknown_error_max_attempts: int = 3
    backoff_base_s: float = 2.0
    backoff_cap_s: float = 300.0
    retry_budget_ratio: float = 0.2
    retry_budget_window_s: float = 60.0
    retry_budget_min_calls: int = 10

    # Circuit breaker
    breaker_window_s: float = 30.0
    breaker_min_calls: int = 20
    breaker_failure_ratio: float = 0.5
    breaker_consecutive_timeouts: int = 5
    breaker_cooldown_s: float = 30.0
    breaker_cooldown_cap_s: float = 300.0
    breaker_half_open_trials: int = 3

    # Retention
    event_retention_s: float = 3600.0
    idempotency_ttl_s: float = 24 * 3600.0
    multipart_idle_s: float = 24 * 3600.0

    # Background loop pacing
    poll_interval_s: float = 0.2


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def set_settings(s: Settings) -> None:
    global _settings
    _settings = s
    _limits_cache.clear()


# ---- limits.yaml, hot-reloaded on mtime change ("changed without a deploy") ----

_limits_lock = threading.Lock()
_limits_cache: dict[str, object] = {}


def get_limits() -> dict:
    path = get_settings().limits_path
    mtime = os.path.getmtime(path)
    with _limits_lock:
        if _limits_cache.get("path") != path or _limits_cache.get("mtime") != mtime:
            with open(path) as f:
                _limits_cache.update(path=path, mtime=mtime, data=yaml.safe_load(f))
        return _limits_cache["data"]  # type: ignore[return-value]
