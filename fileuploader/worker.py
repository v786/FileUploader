"""Processing pipeline workers (BullMQ stand-in over the `jobs` table).

scan -> (extract || thumbnail) -> finalize

Each stage has its own worker pool sized by `concurrency`. Jobs are claimed
with a lease (`locked_until`); a crashed worker's job becomes claimable again
when its lease runs out (stalled-job check). Per-user fairness caps active jobs
per user. Every effect is idempotent, so any job may safely run twice.
"""
from __future__ import annotations

import asyncio
import json
import random
import sqlite3
import uuid
from dataclasses import dataclass

from . import adapters, clock, db, events, state
from .config import get_settings
from .errors import (BreakerOpen, Kind, RateLimited, RetryBudgetExceeded, StageError, classify,
                     message_for, retryable_by_user)
from .log import log
from .vendor_call import call_vendor, vendor_key


@dataclass
class Job:
    id: str
    file_id: str
    batch_id: str
    stage: str
    attempt: int
    prev_status: str
    lease_id: str


def backoff_delay(attempt: int) -> float:
    """Exponential backoff with full jitter: random(0, min(cap, base * 2^attempt))."""
    s = get_settings()
    return random.uniform(0, min(s.backoff_cap_s, s.backoff_base_s * (2 ** attempt)))


# ---------------------------------------------------------------- claiming

def claim(stage: str, n: int, worker_id: str) -> list[Job]:
    s = get_settings()
    lease_s = max(s.job_lock_s, 2 * s.stages[stage].timeout_s)
    out: list[Job] = []
    now = clock.now()
    with db.tx() as conn:
        for _ in range(n):
            row = conn.execute(
                """
                SELECT j.* FROM jobs j
                WHERE j.stage = ?
                  AND ((j.status IN ('queued', 'waiting_vendor') AND j.next_run_at <= ?)
                       OR (j.status = 'running' AND j.locked_until < ?))
                  AND (SELECT COUNT(*) FROM jobs r
                       WHERE r.user_id = j.user_id AND r.status = 'running' AND r.locked_until >= ?) < ?
                ORDER BY j.next_run_at
                LIMIT 1
                """,
                (stage, now, now, now, s.max_active_jobs_per_user),
            ).fetchone()
            if row is None:
                break
            lease_id = f"{worker_id}:{uuid.uuid4().hex[:8]}"
            conn.execute(
                "UPDATE jobs SET status='running', locked_until=?, lease_id=?, updated_at=? WHERE id=?",
                (now + lease_s, lease_id, now, row["id"]),
            )
            out.append(Job(row["id"], row["file_id"], row["batch_id"], row["stage"], row["attempt"],
                           row["status"], lease_id))
    return out


def _owns(conn: sqlite3.Connection, job: Job) -> bool:
    """Guard: only the current lease holder may move a job. Zero rows = someone else did."""
    r = conn.execute("SELECT status, lease_id FROM jobs WHERE id = ?", (job.id,)).fetchone()
    return r is not None and r["status"] == "running" and r["lease_id"] == job.lease_id


def _set_job(conn: sqlite3.Connection, job: Job, **cols) -> None:
    cols["updated_at"] = clock.now()
    sets = ", ".join(f"{k} = ?" for k in cols)
    conn.execute(f"UPDATE jobs SET {sets} WHERE id = ?", (*cols.values(), job.id))


def _error_chain(conn: sqlite3.Connection, job: Job, err: StageError) -> str:
    r = conn.execute("SELECT last_error FROM jobs WHERE id = ?", (job.id,)).fetchone()
    chain = json.loads(r["last_error"] or "[]")
    chain.append({"at": clock.now(), "attempt": job.attempt + 1, "kind": err.kind.value,
                  "code": err.code, "detail": err.detail[:500]})
    return json.dumps(chain[-20:])


# ---------------------------------------------------------------- stage handlers

async def _run_stage(job: Job, f: sqlite3.Row) -> dict:
    s = get_settings()
    cfg = s.stages[job.stage]
    a = adapters.get()
    is_retry = job.attempt > 0
    if job.stage == "scan":
        key = vendor_key(f["id"], "scan", "scanner")
        res = await call_vendor("scanner", key, lambda: a.scanner.scan(
            file_id=f["id"], key=f["s3_key"], name=f["name"], idempotency_key=key),
            timeout_s=cfg.timeout_s, is_retry=is_retry, request={"file_id": f["id"]})
        if res.get("verdict") != "clean":
            raise StageError(Kind.PERMANENT, "MALWARE_DETECTED", json.dumps(res))
        return res
    if job.stage == "extract":
        key = vendor_key(f["id"], "extract", "extractor")
        return await call_vendor("extractor", key, lambda: a.extractor.extract(
            file_id=f["id"], key=f["s3_key"], name=f["name"], size_bytes=f["size_bytes"], idempotency_key=key),
            timeout_s=cfg.timeout_s, is_retry=is_retry, request={"file_id": f["id"]})
    if job.stage == "thumbnail":
        key = vendor_key(f["id"], "thumbnail", "thumbnailer")
        return await call_vendor("thumbnailer", key, lambda: a.thumbnailer.thumbnail(
            file_id=f["id"], key=f["s3_key"], name=f["name"], idempotency_key=key),
            timeout_s=cfg.timeout_s, is_retry=is_retry, request={"file_id": f["id"]})
    if job.stage == "finalize":
        return {}  # assembled in _on_success under the write lock
    raise StageError(Kind.PERMANENT, "INTERNAL_ERROR", f"unknown stage {job.stage}")


# ---------------------------------------------------------------- outcomes

def _start(job: Job) -> sqlite3.Row | None:
    """Move the file into processing; returns None if the job should not run."""
    with db.tx() as conn:
        if not _owns(conn, job):
            return None
        f = state.get_file(conn, job.file_id)
        if f["status"] in events.FINAL:
            # Failed file: skip sibling stages; a user retry re-queues them with the failed one.
            _set_job(conn, job, status={"cancelled": "cancelled", "failed": "skipped"}.get(f["status"], "done"),
                     locked_until=None, lease_id=None)
            return None
        if job.prev_status == "waiting_vendor":
            return f  # still parked from the user's view until the call succeeds
        if f["status"] == "processing" and f["stage"] == job.stage:
            return f  # nothing visible changed (e.g. re-run after a rate-limit delay)
        if f["status"] != "processing":
            state.transition(conn, f, "processing", stage=job.stage)
        else:
            conn.execute("UPDATE files SET stage = ? WHERE id = ?", (job.stage, f["id"]))
        f = state.get_file(conn, job.file_id)
        events.emit(conn, job.batch_id, "file.stage",
                    {"file_id": f["id"], "stage": job.stage, "progress_pct": f["progress_pct"]}, f["id"])
        return f


def _on_success(job: Job, result: dict) -> None:
    with db.tx() as conn:
        if not _owns(conn, job):
            return  # lease lost: another worker owns this job; exit as success
        _set_job(conn, job, status="done", locked_until=None, lease_id=None, result_json=json.dumps(result))
        f = state.get_file(conn, job.file_id)
        if f["status"] in events.FINAL:
            return

        if job.stage == "scan":
            events.enqueue(conn, "extract", f["id"])
            events.enqueue(conn, "thumbnail", f["id"])
        elif job.stage in ("extract", "thumbnail"):
            # Fan-in: we hold the write lock, so exactly one transaction sees zero remaining.
            remaining = conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE file_id = ? AND stage IN ('extract','thumbnail') "
                "AND status != 'done'", (f["id"],)).fetchone()[0]
            if remaining == 0:
                events.enqueue(conn, "finalize", f["id"])  # job id finalize:<file_id>
        elif job.stage == "finalize":
            results = {r["stage"]: json.loads(r["result_json"] or "null") for r in conn.execute(
                "SELECT stage, result_json FROM jobs WHERE file_id = ?", (f["id"],))}
            out = {"scan": results.get("scan"), "metadata": results.get("extract"),
                   "thumbnail": results.get("thumbnail")}
            state.transition(conn, f, "completed", stage="finalize", progress_pct=100.0, error_code=None,
                             error_message=None, result_json=json.dumps(out))
            events.emit(conn, job.batch_id, "file.completed",
                        {"file_id": f["id"], "result_url": f"/files/{f['id']}/result"}, f["id"])
            events.emit_summary(conn, job.batch_id)
            log("file.completed", batch_id=job.batch_id, file_id=f["id"], job_id=job.id)
            return

        f = state.sync_status(conn, f["id"])
        events.emit(conn, job.batch_id, "file.stage",
                    {"file_id": f["id"], "stage": job.stage, "progress_pct": f["progress_pct"]}, f["id"])


def _delay(job: Job, wait_s: float, reason: str) -> None:
    """No token / retry budget spent: put the job back without spending an attempt."""
    with db.tx() as conn:
        if not _owns(conn, job):
            return
        _set_job(conn, job, status="queued" if job.prev_status != "waiting_vendor" else "waiting_vendor",
                 next_run_at=clock.now() + wait_s, locked_until=None, lease_id=None)
    log("job.delayed", job_id=job.id, file_id=job.file_id, wait_s=round(wait_s, 3), reason=reason)


def _park(job: Job, exc: BreakerOpen) -> None:
    """Breaker open: park as waiting_vendor until the cool-down ends; no attempt spent."""
    with db.tx() as conn:
        if not _owns(conn, job):
            return
        _set_job(conn, job, status="waiting_vendor", next_run_at=exc.retry_at, locked_until=None,
                 lease_id=None)
        f = state.sync_status(conn, job.file_id)
        if job.prev_status != "waiting_vendor":
            events.emit(conn, job.batch_id, "file.waiting", {"file_id": f["id"], "vendor": exc.vendor}, f["id"])
            events.emit_summary(conn, job.batch_id)
    log("job.parked", job_id=job.id, file_id=job.file_id, vendor=exc.vendor)


def _fail(job: Job, err: StageError, dead: bool) -> None:
    s = get_settings()
    code = err.code if not dead else ("INTERNAL_ERROR" if err.kind == Kind.UNKNOWN else "RETRIES_EXHAUSTED")
    with db.tx() as conn:
        if not _owns(conn, job):
            return
        chain = _error_chain(conn, job, err)
        _set_job(conn, job, status="dead" if dead else "failed", attempt=job.attempt + 1, locked_until=None,
                 lease_id=None, last_error=chain)
        if dead:
            conn.execute("INSERT INTO dlq (job_id, stage, file_id, error_chain, created_at) VALUES (?, ?, ?, ?, ?)",
                         (job.id, job.stage, job.file_id, chain, clock.now()))
        f = state.get_file(conn, job.file_id)
        if f["status"] in events.FINAL:
            return
        retryable = retryable_by_user(code)
        state.transition(conn, f, "failed", error_code=code, error_message=message_for(code),
                         retryable_by_user=int(retryable), attempts=f["attempts"] + 1)
        events.emit(conn, job.batch_id, "file.failed",
                    {"file_id": f["id"], "error_code": code, "message": message_for(code),
                     "retryable_by_user": retryable, "stage": job.stage}, f["id"])
        events.emit_summary(conn, job.batch_id)
    log("job.failed", job_id=job.id, file_id=job.file_id, batch_id=job.batch_id, code=code, dead=dead,
        detail=err.detail, max_attempts=s.max_attempts)


def _retry(job: Job, err: StageError) -> None:
    s = get_settings()
    attempt = job.attempt + 1
    delay = backoff_delay(attempt)
    if err.retry_after_s:
        delay = max(delay, err.retry_after_s)  # honour the vendor's Retry-After
    next_at = clock.now() + delay
    with db.tx() as conn:
        if not _owns(conn, job):
            return
        _set_job(conn, job, status="queued", attempt=attempt, next_run_at=next_at, locked_until=None,
                 lease_id=None, last_error=_error_chain(conn, job, err))
        conn.execute("UPDATE files SET attempts = attempts + 1 WHERE id = ?", (job.file_id,))
        f = state.sync_status(conn, job.file_id)
        max_attempts = s.unknown_error_max_attempts if err.kind == Kind.UNKNOWN else s.max_attempts
        events.emit(conn, job.batch_id, "file.retrying",
                    {"file_id": f["id"], "attempt": attempt, "max_attempts": max_attempts,
                     "next_retry_at": next_at, "reason": err.code}, f["id"])
    log("job.retrying", job_id=job.id, file_id=job.file_id, attempt=attempt, delay_s=round(delay, 3),
        code=err.code)


async def process(job: Job) -> None:
    s = get_settings()
    f = _start(job)
    if f is None:
        return
    try:
        result = await asyncio.wait_for(_run_stage(job, f), timeout=2 * s.stages[job.stage].timeout_s)
    except BreakerOpen as exc:
        _park(job, exc)
    except (RateLimited, RetryBudgetExceeded) as exc:
        _delay(job, exc.wait_s, type(exc).__name__)
    except (asyncio.TimeoutError, TimeoutError):
        _handle_error(job, StageError(Kind.TRANSIENT, "JOB_TIMEOUT", "job timeout"))
    except Exception as exc:  # noqa: BLE001 - every error is classified once, here
        _handle_error(job, classify(exc))
    else:
        _on_success(job, result)


def _handle_error(job: Job, err: StageError) -> None:
    s = get_settings()
    if err.kind == Kind.PERMANENT:
        _fail(job, err, dead=False)
    elif err.kind == Kind.DEPENDENCY_DOWN:
        _park(job, BreakerOpen(err.detail, clock.now() + s.breaker_cooldown_s))
    else:
        limit = s.unknown_error_max_attempts if err.kind == Kind.UNKNOWN else s.max_attempts
        if job.attempt + 1 >= limit:
            _fail(job, err, dead=True)
        else:
            _retry(job, err)


# ---------------------------------------------------------------- runners

async def tick(worker_id: str = "w") -> int:
    """Relay the outbox, then claim and run every ready job once (bounded per stage)."""
    events.relay_once()
    s = get_settings()
    claimed = [j for stage, cfg in s.stages.items() for j in claim(stage, cfg.concurrency, worker_id)]
    await asyncio.gather(*(process(j) for j in claimed))
    events.relay_once()
    return len(claimed)


async def run_until_idle(max_ticks: int = 1000) -> None:
    """Test/CLI helper: keep ticking until no job is ready right now."""
    for _ in range(max_ticks):
        if await tick() == 0:
            return
    raise RuntimeError("pipeline did not go idle")


async def run_stage_pool(stage: str, stop: asyncio.Event, worker_id: str) -> None:
    s = get_settings()
    conc = s.stages[stage].concurrency
    active: set[asyncio.Task] = set()
    while not stop.is_set():
        free = conc - len(active)
        if free > 0:
            for job in await asyncio.to_thread(claim, stage, free, worker_id):
                t = asyncio.create_task(process(job))
                active.add(t)
                t.add_done_callback(active.discard)
        try:
            await asyncio.wait_for(stop.wait(), timeout=s.poll_interval_s)
        except (asyncio.TimeoutError, TimeoutError):
            pass
    if active:
        await asyncio.wait(active, timeout=5)


async def run_relay(stop: asyncio.Event) -> None:
    s = get_settings()
    while not stop.is_set():
        try:
            n = await asyncio.to_thread(events.relay_once)
        except Exception as exc:  # noqa: BLE001
            log("relay.error", error=str(exc))
            n = 0
        if n == 0:
            try:
                await asyncio.wait_for(stop.wait(), timeout=s.poll_interval_s / 2)
            except (asyncio.TimeoutError, TimeoutError):
                pass


async def run_workers(stop: asyncio.Event, worker_id: str | None = None) -> None:
    from .sweeper import run_sweeper
    worker_id = worker_id or f"worker-{uuid.uuid4().hex[:6]}"
    s = get_settings()
    log("workers.start", worker_id=worker_id, stages={k: v.concurrency for k, v in s.stages.items()})
    await asyncio.gather(
        run_relay(stop),
        run_sweeper(stop),
        *(run_stage_pool(stage, stop, worker_id) for stage in s.stages),
    )
