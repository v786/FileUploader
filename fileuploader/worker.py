"""Processing pipeline workers (BullMQ stand-in over the `jobs` table).

scan -> (extract || thumbnail || process) -> finalize

Each stage has its own worker pool sized by `concurrency`. Jobs are claimed
with a lease (`locked_until`); a crashed worker's job becomes claimable again
when its lease runs out (stalled-job check). Per-user fairness caps active jobs
per user, and jobs of batches someone is watching (SSE open) go first. Every
effect is idempotent, so any job may safely run twice.

`process` is the external processing API, an async vendor job. The worker submits
it with a fresh presigned GET URL and stores the vendor's job id before acking;
the job then sits in `polling` and is re-claimed every 10 s, backing off to 60 s,
without holding a worker slot in between. The vendor's webhook usually completes
it first: webhook and poller run the same transaction (`apply_vendor_update`),
so whichever arrives first completes the job and the other finds it done.
With PROCESS_PARALLEL_WITH_SCAN=1 (trusted vendor only), process starts at upload
completion, alongside scan.
"""
from __future__ import annotations

import asyncio
import json
import random
import sqlite3
import uuid
from dataclasses import dataclass

from . import adapters, clock, db, estimates, events, state
from .config import get_settings
from .errors import (BreakerOpen, Kind, RateLimited, RetryBudgetExceeded, StageError, classify, fix_for,
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
    vendor_job_id: str | None = None


class _Poll(Exception):
    """The vendor job is still running: check again later."""


VENDOR_RANK = {"queued": 0, "running": 1, "done": 2, "failed": 2}
CLOSED_JOB = ("done", "cancelled", "skipped", "dead", "failed")


def process_key(file_id: str, generation: int) -> str:
    """Idempotency-Key for the vendor submit: sha256(file_id + stage + vendor), plus the
    generation once a user retry asks for a new vendor job."""
    return vendor_key(file_id, "process" if generation == 0 else f"process#{generation}", "processor")


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
                  AND ((j.status IN ('queued', 'waiting_vendor', 'polling') AND j.next_run_at <= ?)
                       OR (j.status = 'running' AND j.locked_until < ?))
                  AND (SELECT COUNT(*) FROM jobs r
                       WHERE r.user_id = j.user_id AND r.status = 'running' AND r.locked_until >= ?) < ?
                ORDER BY (SELECT COALESCE(b.watched_until, 0) >= ? FROM batches b WHERE b.id = j.batch_id) DESC,
                         j.next_run_at
                LIMIT 1
                """,
                (stage, now, now, now, s.max_active_jobs_per_user, now),
            ).fetchone()
            if row is None:
                break
            lease_id = f"{worker_id}:{uuid.uuid4().hex[:8]}"
            conn.execute(
                "UPDATE jobs SET status='running', locked_until=?, lease_id=?, started_at=COALESCE(started_at, ?), "
                "updated_at=? WHERE id=?",
                (now + lease_s, lease_id, now, now, row["id"]),
            )
            out.append(Job(row["id"], row["file_id"], row["batch_id"], row["stage"], row["attempt"],
                           row["status"], lease_id, row["vendor_job_id"]))
    return out


def _owns(conn: sqlite3.Connection, job: Job) -> bool:
    """Guard: only the current lease holder may move a job. Zero rows = someone else did."""
    r = conn.execute("SELECT status, lease_id FROM jobs WHERE id = ?", (job.id,)).fetchone()
    return r is not None and r["status"] == "running" and r["lease_id"] == job.lease_id


def _set_job(conn: sqlite3.Connection, job: Job | str, **cols) -> None:
    cols["updated_at"] = clock.now()
    sets = ", ".join(f"{k} = ?" for k in cols)
    conn.execute(f"UPDATE jobs SET {sets} WHERE id = ?", (*cols.values(), job if isinstance(job, str) else job.id))


def _record_duration(conn: sqlite3.Connection, job_id: str, stage: str, f: sqlite3.Row) -> None:
    started = conn.execute("SELECT started_at FROM jobs WHERE id = ?", (job_id,)).fetchone()[0]
    if started is not None:
        estimates.record(conn, stage, f, clock.now() - started)


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
    if job.stage == "process":
        return await _run_process(job, f)
    if job.stage == "finalize":
        return {}  # assembled in _on_success under the write lock
    raise StageError(Kind.PERMANENT, "INTERNAL_ERROR", f"unknown stage {job.stage}")


async def _run_process(job: Job, f: sqlite3.Row) -> None:
    """Submit once, then poll. Returns None: completion is applied by apply_vendor_update."""
    s = get_settings()
    cfg = s.stages["process"]
    a = adapters.get()
    with db.read() as conn:
        row = conn.execute("SELECT vendor_job_id, submitted_at, generation FROM jobs WHERE id = ?",
                           (job.id,)).fetchone()
    vendor_job_id, submitted_at = row["vendor_job_id"], row["submitted_at"]
    if vendor_job_id is None:
        key = process_key(f["id"], row["generation"])
        # Signed fresh on every attempt (never cached), so a backlog can't leave the vendor an expired URL.
        url = a.store.presign_get(f["s3_key"], s.process_source_url_ttl_s)
        res = await call_vendor("processor", key, lambda: a.processor.submit(
            file_id=f["id"], name=f["name"], source_url=url, idempotency_key=key),
            timeout_s=cfg.timeout_s, is_retry=job.attempt > 0,
            request={"file_id": f["id"], "generation": row["generation"]})
        vendor_job_id, submitted_at = res["job_id"], clock.now()
        with db.tx() as conn:  # stored before the ack: a replay resumes polling instead of submitting again
            if not _owns(conn, job):
                return None
            _set_job(conn, job, vendor_job_id=vendor_job_id, submitted_at=submitted_at)
    if clock.now() - (submitted_at or clock.now()) > s.process_deadline_s:
        raise StageError(Kind.TRANSIENT, "PROCESS_DEADLINE", f"vendor job {vendor_job_id} past the deadline")

    st = await call_vendor("processor", "", lambda: a.processor.status(job_id=vendor_job_id),
                           timeout_s=cfg.timeout_s, bucket="processor_poll", cache=False)
    outcome = apply_vendor_update(vendor_job_id, st)
    if outcome == "failed":
        code = (st.get("error") or {}).get("code") or "PROCESSING_FAILED"
        raise StageError(Kind.PERMANENT, code, json.dumps(st)[:500])
    if outcome in ("queued", "running"):
        raise _Poll()
    return None


def apply_vendor_update(vendor_job_id: str, upd: dict, event_id: str | None = None) -> str:
    """Apply a vendor status report, from a webhook or a poll.

    Webhooks are deduped by `event_id`, and a status older than the stored one is
    ignored, so duplicates and out-of-order delivery are harmless. "done" stores the
    result in S3 and completes the job and fan-in in one transaction.

    Returns the job's vendor status afterwards, or "duplicate" / "unknown" / "closed".
    """
    with db.read() as conn:
        j = conn.execute("SELECT file_id FROM jobs WHERE vendor_job_id = ?", (vendor_job_id,)).fetchone()
    if j is None:
        return "unknown"
    status = upd.get("status")
    result_key = f"results/{j['file_id']}/process.json"
    if status == "done":
        # Our own copy: never store only the vendor's URL, which may expire. Deterministic key: idempotent.
        adapters.get().store.put(result_key, json.dumps(upd.get("result") or {}).encode())

    now = clock.now()
    with db.tx() as conn:
        if event_id is not None and conn.execute(
                "INSERT OR IGNORE INTO webhook_events (event_id, vendor, received_at) VALUES (?, 'processor', ?)",
                (event_id, now)).rowcount == 0:
            return "duplicate"
        job = conn.execute("SELECT * FROM jobs WHERE vendor_job_id = ?", (vendor_job_id,)).fetchone()
        f = state.get_file(conn, job["file_id"])
        if job["status"] in CLOSED_JOB or f["status"] in events.FINAL:
            return "closed"
        at = float(upd.get("at") or now)
        old_rank, new_rank = VENDOR_RANK.get(job["vendor_status"], -1), VENDOR_RANK.get(status, -1)
        if new_rank < old_rank or (new_rank == old_rank and job["vendor_updated_at"] is not None
                                   and at < job["vendor_updated_at"]):
            return job["vendor_status"]  # older than what we have

        prev = job["vendor_progress"] or 0.0
        if status == "done":
            vp = 100.0
        elif upd.get("progress_pct") is not None:  # vendor reports a percent
            vp = min(99.0, float(upd["progress_pct"]))
        elif status == "running":  # states only: elapsed / p50, capped at 90% until it finishes
            p50 = estimates.p50(conn, "process", f["name"], f["size_bytes"])
            vp = min(90.0, 100.0 * (now - (job["submitted_at"] or now)) / p50)
        else:  # queued at the vendor: hold the bar still
            vp = prev
        vp = max(prev, vp)
        cols = {"vendor_status": status, "vendor_progress": vp, "vendor_updated_at": at,
                "vendor_queue_position": upd.get("queue_position") if status == "queued" else None}

        if status == "done":
            _set_job(conn, job["id"], status="done", locked_until=None, lease_id=None,
                     result_json=json.dumps({"result_key": result_key}), **cols)
            conn.execute("UPDATE files SET process_result_key = ? WHERE id = ?", (result_key, f["id"]))
            _record_duration(conn, job["id"], "process", f)
            _advance(conn, "process", f)
            log("vendor_job.done", file_id=f["id"], job_id=job["id"], vendor_job_id=vendor_job_id,
                via="webhook" if event_id else "poll")
            return "done"
        if status == "failed":
            if job["status"] in ("polling", "queued", "waiting_vendor"):
                cols["next_run_at"] = now  # a worker picks it up now and fails it through the normal path
            _set_job(conn, job["id"], **cols)
            return "failed"

        changed = status != job["vendor_status"] or round(vp) != round(prev)
        _set_job(conn, job["id"], **cols)
        if changed:
            conn.execute("UPDATE files SET progress_pct = ? WHERE id = ?",
                         (estimates.processing_pct(conn, state.get_file(conn, f["id"])), f["id"]))
            extra = {"vendor_status": status}
            if cols["vendor_queue_position"] is not None:
                extra["queue_position"] = cols["vendor_queue_position"]
            state.emit_stage(conn, state.get_file(conn, f["id"]), "process", **extra)
        return status


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
        if job.vendor_job_id is not None:
            return f  # a status poll: nothing visible changes
        if f["status"] == "processing" and f["stage"] == job.stage:
            return f  # nothing visible changed (e.g. re-run after a rate-limit delay)
        if f["status"] != "processing":
            state.transition(conn, f, "processing", stage=job.stage)
        else:
            conn.execute("UPDATE files SET stage = ? WHERE id = ?", (job.stage, f["id"]))
        f = state.get_file(conn, job.file_id)
        state.emit_stage(conn, f, job.stage)
        return f


def _on_success(job: Job, result: dict) -> None:
    with db.tx() as conn:
        if not _owns(conn, job):
            return  # lease lost: another worker owns this job; exit as success
        _set_job(conn, job, status="done", locked_until=None, lease_id=None, result_json=json.dumps(result))
        f = state.get_file(conn, job.file_id)
        if f["status"] in events.FINAL:
            return
        _record_duration(conn, job.id, job.stage, f)
        _advance(conn, job.stage, f)


def _advance(conn: sqlite3.Connection, stage: str, f: sqlite3.Row) -> None:
    """After a stage is done (inside its transaction): enqueue what comes next."""
    if stage == "scan":
        events.enqueue(conn, "extract", f["id"])
        events.enqueue(conn, "thumbnail", f["id"])
        if not get_settings().process_parallel_with_scan:
            events.enqueue(conn, "process", f["id"])
    elif stage in state.FAN_IN:
        # Fan-in: we hold the write lock, so exactly one transaction sees all three done.
        done = conn.execute(
            f"SELECT COUNT(*) FROM jobs WHERE file_id = ? AND stage IN ({','.join('?' * len(state.FAN_IN))}) "
            "AND status = 'done'", (f["id"], *state.FAN_IN)).fetchone()[0]
        if done == len(state.FAN_IN):
            events.enqueue(conn, "finalize", f["id"])  # job id finalize:<file_id>
    elif stage == "finalize":
        results = {r["stage"]: json.loads(r["result_json"] or "null") for r in conn.execute(
            "SELECT stage, result_json FROM jobs WHERE file_id = ?", (f["id"],))}
        out = {"scan": results.get("scan"), "metadata": results.get("extract"),
               "thumbnail": results.get("thumbnail"), "process": results.get("process")}
        if f["uploaded_at"] is not None:
            estimates.record(conn, estimates.TOTAL, f, clock.now() - f["uploaded_at"])
        state.transition(conn, f, "completed", stage="finalize", progress_pct=100.0, error_code=None,
                         error_message=None, result_json=json.dumps(out))
        events.emit(conn, f["batch_id"], "file.completed",
                    {"file_id": f["id"], "result_url": f"/files/{f['id']}/result"}, f["id"])
        events.emit_summary(conn, f["batch_id"])
        log("file.completed", batch_id=f["batch_id"], file_id=f["id"])
        return

    f = state.sync_status(conn, f["id"])
    state.emit_stage(conn, f, stage)


def _delay(job: Job, wait_s: float, reason: str) -> None:
    """No token / retry budget spent: put the job back without spending an attempt."""
    with db.tx() as conn:
        if not _owns(conn, job):
            return
        polling = conn.execute("SELECT vendor_job_id FROM jobs WHERE id = ?", (job.id,)).fetchone()[0] is not None
        status = "waiting_vendor" if job.prev_status == "waiting_vendor" else "polling" if polling else "queued"
        _set_job(conn, job, status=status, next_run_at=clock.now() + wait_s, locked_until=None, lease_id=None)
    log("job.delayed", job_id=job.id, file_id=job.file_id, wait_s=round(wait_s, 3), reason=reason)


def _reschedule_poll(job: Job) -> None:
    """Vendor job still running: poll again in 10 s, backing off to 60 s. No slot held meanwhile."""
    s = get_settings()
    with db.tx() as conn:
        if not _owns(conn, job):
            return
        polls = conn.execute("SELECT polls FROM jobs WHERE id = ?", (job.id,)).fetchone()[0]
        wait = min(s.process_poll_max_s, s.process_poll_initial_s * 1.5 ** polls)
        _set_job(conn, job, status="polling", polls=polls + 1, next_run_at=clock.now() + wait, locked_until=None,
                 lease_id=None)
        before = state.get_file(conn, job.file_id)["status"]
        f = state.sync_status(conn, job.file_id)
        if f["status"] != before:  # e.g. back from waiting_vendor
            state.emit_stage(conn, f, job.stage)


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
                     "retryable_by_user": retryable, "fix": fix_for(code), "stage": job.stage}, f["id"])
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
        extra = {"submitted_at": next_at} if err.code == "PROCESS_DEADLINE" else {}  # a fresh window to poll in
        _set_job(conn, job, status="queued", attempt=attempt, next_run_at=next_at, locked_until=None,
                 lease_id=None, last_error=_error_chain(conn, job, err), **extra)
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
    except _Poll:
        _reschedule_poll(job)
    except BreakerOpen as exc:
        _park(job, exc)
    except (RateLimited, RetryBudgetExceeded) as exc:
        _delay(job, exc.wait_s, type(exc).__name__)
    except (asyncio.TimeoutError, TimeoutError):
        _handle_error(job, StageError(Kind.TRANSIENT, "JOB_TIMEOUT", "job timeout"))
    except Exception as exc:  # noqa: BLE001 - every error is classified once, here
        _handle_error(job, classify(exc))
    else:
        if result is not None:
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

def deliver_mock_webhooks() -> int:
    """The mock processor's webhook sender (a real vendor runs its own)."""
    if get_settings().vendor_mode != "mock":
        return 0
    return adapters.get().processor.deliver_due()


async def tick(worker_id: str = "w") -> int:
    """Relay the outbox, deliver due webhooks, then claim and run every ready job once (bounded per stage)."""
    events.relay_once()
    delivered = await asyncio.to_thread(deliver_mock_webhooks)
    events.relay_once()
    s = get_settings()
    claimed = [j for stage, cfg in s.stages.items() for j in claim(stage, cfg.concurrency, worker_id)]
    await asyncio.gather(*(process(j) for j in claimed))
    events.relay_once()
    return len(claimed) + delivered


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


async def run_mock_webhooks(stop: asyncio.Event) -> None:
    s = get_settings()
    while not stop.is_set():
        try:
            await asyncio.to_thread(deliver_mock_webhooks)
        except Exception as exc:  # noqa: BLE001
            log("webhooks.error", error=str(exc))
        try:
            await asyncio.wait_for(stop.wait(), timeout=s.poll_interval_s)
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
        run_mock_webhooks(stop),
        *(run_stage_pool(stage, stop, worker_id) for stage in s.stages),
    )
