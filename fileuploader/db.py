"""SQLite: source of truth for file state, and the stand-in for Redis (queues,
token buckets, breaker state, event streams).

Every unit of work opens its own connection. Write transactions use
BEGIN IMMEDIATE, which takes SQLite's single write lock up front; that gives
us the same guarantee as Postgres' SELECT ... FOR UPDATE (stronger, in fact),
so read-then-write sequences like fan-in can't race across threads or processes.
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import Iterator

from .config import get_settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id              TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    last_seq        INTEGER NOT NULL DEFAULT 0,
    watched_until   REAL,             -- an SSE stream is open: this batch's jobs go in the interactive lane
    created_at      REAL NOT NULL,
    UNIQUE (user_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS files (
    id                TEXT PRIMARY KEY,
    batch_id          TEXT NOT NULL REFERENCES batches(id),
    user_id           TEXT NOT NULL,
    client_file_id    TEXT NOT NULL,
    name              TEXT NOT NULL,
    size_bytes        INTEGER NOT NULL,
    sha256            TEXT,             -- may arrive only at complete (hashed while uploading)
    fingerprint       TEXT,             -- size + hash of first and last 1 MB, for pre-upload dedupe
    status            TEXT NOT NULL,
    stage             TEXT,
    progress_pct      REAL NOT NULL DEFAULT 0,
    attempts          INTEGER NOT NULL DEFAULT 0,
    error_code        TEXT,
    error_message     TEXT,
    retryable_by_user INTEGER NOT NULL DEFAULT 0,
    s3_key            TEXT,
    s3_upload_id      TEXT,
    upload_mode       TEXT,
    part_size         INTEGER,
    dedup_of          TEXT,
    dedup_candidate   TEXT,             -- fingerprint hit, confirmed by the full hash at complete
    result_json       TEXT,
    process_result_key TEXT,            -- results/<file_id>/process.json
    processing_estimate_ms INTEGER,
    uploaded_at       REAL,
    last_seq          INTEGER NOT NULL DEFAULT 0,
    version           INTEGER NOT NULL DEFAULT 0,
    created_at        REAL NOT NULL,
    updated_at        REAL NOT NULL,
    UNIQUE (batch_id, client_file_id)
);
CREATE INDEX IF NOT EXISTS files_user_sha ON files (user_id, sha256, status);
CREATE INDEX IF NOT EXISTS files_user_fp ON files (user_id, fingerprint, status);

CREATE TABLE IF NOT EXISTS file_parts (
    file_id     TEXT NOT NULL REFERENCES files(id),
    part_number INTEGER NOT NULL,
    etag        TEXT NOT NULL,
    size_bytes  INTEGER NOT NULL,
    checksum    TEXT,
    updated_at  REAL NOT NULL,
    PRIMARY KEY (file_id, part_number)
);

-- One row per stage per file; doubles as the work queue (BullMQ stand-in).
-- id is the deterministic job id '<stage>:<file_id>'.
CREATE TABLE IF NOT EXISTS jobs (
    id           TEXT PRIMARY KEY,
    file_id      TEXT NOT NULL REFERENCES files(id),
    batch_id     TEXT NOT NULL,
    user_id      TEXT NOT NULL,
    stage        TEXT NOT NULL,
    status       TEXT NOT NULL,      -- queued | running | polling | done | waiting_vendor | failed | dead | skipped | cancelled
    attempt      INTEGER NOT NULL DEFAULT 0,
    next_run_at  REAL NOT NULL,
    locked_until REAL,
    lease_id     TEXT,             -- current lease holder; guards every job update
    last_error   TEXT,             -- JSON error chain
    result_json  TEXT,
    started_at   REAL,             -- first claim; for stage duration samples
    -- async vendor jobs (process stage): stored before the worker acks the submit
    vendor_job_id         TEXT,
    vendor_status         TEXT,    -- queued | running | done | failed
    vendor_progress       REAL,    -- 0-100, as reported or estimated
    vendor_queue_position INTEGER,
    vendor_updated_at     REAL,    -- vendor time of the newest status applied
    submitted_at          REAL,
    polls                 INTEGER NOT NULL DEFAULT 0,
    generation            INTEGER NOT NULL DEFAULT 0,  -- bumped when a failed vendor job is retried
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL,
    UNIQUE (file_id, stage)
);
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs (stage, status, next_run_at);
CREATE INDEX IF NOT EXISTS jobs_vendor_job ON jobs (vendor_job_id);

-- Webhook event ids already applied (dedupe).
CREATE TABLE IF NOT EXISTS webhook_events (
    event_id    TEXT PRIMARY KEY,
    vendor      TEXT NOT NULL,
    received_at REAL NOT NULL
);

-- Stage duration samples behind the rolling p50 estimates.
CREATE TABLE IF NOT EXISTS stage_samples (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    stage      TEXT NOT NULL,
    bucket     TEXT NOT NULL,      -- <family>:<size bucket>
    duration_s REAL NOT NULL,
    ts         REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS stage_samples_sb ON stage_samples (stage, bucket, ts);

CREATE TABLE IF NOT EXISTS dlq (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT NOT NULL,
    stage       TEXT NOT NULL,
    file_id     TEXT NOT NULL,
    error_chain TEXT NOT NULL,
    created_at  REAL NOT NULL,
    replayed_at REAL
);

CREATE TABLE IF NOT EXISTS vendor_calls (
    idempotency_key TEXT PRIMARY KEY,
    vendor          TEXT NOT NULL,
    request_hash    TEXT NOT NULL,
    response_json   TEXT NOT NULL,
    status          TEXT NOT NULL,
    created_at      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    topic        TEXT NOT NULL,     -- 'enqueue' | 'event'
    payload      TEXT NOT NULL,
    created_at   REAL NOT NULL,
    published_at REAL
);
CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (published_at, id);

-- Per-batch event stream (Redis stream stand-in), kept for event_retention_s.
CREATE TABLE IF NOT EXISTS events (
    batch_id   TEXT NOT NULL,
    seq        INTEGER NOT NULL,
    type       TEXT NOT NULL,
    payload    TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (batch_id, seq)
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    user_id       TEXT NOT NULL,
    key           TEXT NOT NULL,
    request_hash  TEXT NOT NULL,
    state         TEXT NOT NULL,    -- in_flight | done
    status_code   INTEGER,
    response_json TEXT,
    created_at    REAL NOT NULL,
    PRIMARY KEY (user_id, key)
);

CREATE TABLE IF NOT EXISTS rate_buckets (
    key        TEXT PRIMARY KEY,
    tokens     REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS breakers (
    vendor               TEXT PRIMARY KEY,
    state                TEXT NOT NULL DEFAULT 'closed',
    opened_at            REAL,
    cooldown_s           REAL NOT NULL DEFAULT 0,
    half_open_successes  INTEGER NOT NULL DEFAULT 0,
    half_open_inflight   INTEGER NOT NULL DEFAULT 0,
    consecutive_timeouts INTEGER NOT NULL DEFAULT 0
);

-- Rolling window of vendor call outcomes for the breaker and the retry budget.
CREATE TABLE IF NOT EXISTS vendor_call_log (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor   TEXT NOT NULL,
    ts       REAL NOT NULL,
    ok       INTEGER NOT NULL,
    is_retry INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS vendor_call_log_vt ON vendor_call_log (vendor, ts);

-- State of the mock processing vendor (stands in for that service's own storage;
-- kept here so API and worker processes see the same jobs).
CREATE TABLE IF NOT EXISTS mock_processor_jobs (
    job_id          TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    file_id         TEXT NOT NULL,
    name            TEXT NOT NULL,
    created_at      REAL NOT NULL,
    last_sent       TEXT,              -- last state a webhook was generated for
    finished        INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS mock_processor_webhooks (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    deliver_at REAL NOT NULL,
    body       TEXT NOT NULL
);
"""


def connect() -> sqlite3.Connection:
    path = get_settings().db_path
    conn = sqlite3.connect(path, timeout=30, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db() -> None:
    path = get_settings().db_path
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = connect()
    try:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(files)")}
        if cols and "fingerprint" not in cols:
            raise RuntimeError(f"{path} was created by an older schema; delete it (`make clean`) and restart.")
        conn.executescript(SCHEMA)
    finally:
        conn.close()


@contextmanager
def tx() -> Iterator[sqlite3.Connection]:
    """A write transaction holding the database write lock until commit."""
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")
    finally:
        conn.close()


@contextmanager
def read() -> Iterator[sqlite3.Connection]:
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()
