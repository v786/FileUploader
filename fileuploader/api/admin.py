"""Admin endpoints (DLQ, breakers, metrics) and the mock-vendor fault API."""
from __future__ import annotations

import json

from fastapi import APIRouter

from .. import adapters, breaker, clock, db, events, state
from ..adapters.vendors import VENDORS
from ..errors import ApiError
from ..log import log

router = APIRouter()


@router.get("/admin/dlq")
def list_dlq(stage: str | None = None):
    q = "SELECT * FROM dlq WHERE replayed_at IS NULL"
    args: tuple = ()
    if stage:
        q += " AND stage = ?"
        args = (stage,)
    with db.read() as conn:
        rows = conn.execute(q + " ORDER BY id", args).fetchall()
    return {"items": [{**dict(r), "error_chain": json.loads(r["error_chain"])} for r in rows]}


@router.post("/admin/dlq/{stage}/replay")
def replay_dlq(stage: str):
    """Re-drive dead jobs once the cause is fixed. Safe: every stage is idempotent."""
    if stage not in state.STAGES:
        raise ApiError(404, "NOT_FOUND")
    now = clock.now()
    replayed = 0
    with db.tx() as conn:
        rows = conn.execute("SELECT * FROM dlq WHERE stage = ? AND replayed_at IS NULL", (stage,)).fetchall()
        for r in rows:
            conn.execute("UPDATE dlq SET replayed_at = ? WHERE id = ?", (now, r["id"]))
            job = conn.execute("SELECT * FROM jobs WHERE id = ?", (r["job_id"],)).fetchone()
            f = state.get_file(conn, r["file_id"])
            if job is None or job["status"] != "dead" or f["status"] in ("completed", "cancelled"):
                continue
            conn.execute("UPDATE jobs SET status='queued', attempt=0, next_run_at=?, lease_id=NULL, "
                         "locked_until=NULL, updated_at=? WHERE id=?", (now, now, job["id"]))
            conn.execute("UPDATE jobs SET status='queued', next_run_at=?, updated_at=? "
                         "WHERE file_id=? AND status='skipped'", (now, now, f["id"]))
            if f["status"] == "failed":
                state.transition(conn, f, "processing", error_code=None, error_message=None, retryable_by_user=0)
                events.emit(conn, f["batch_id"], "file.stage",
                            {"file_id": f["id"], "stage": stage, "progress_pct": f["progress_pct"]}, f["id"])
                events.emit_summary(conn, f["batch_id"])
            replayed += 1
    log("dlq.replay", stage=stage, replayed=replayed)
    return {"replayed": replayed}


@router.get("/admin/breakers")
def breakers():
    return {v: breaker.state(v) for v in VENDORS}


@router.get("/admin/metrics")
def metrics():
    now = clock.now()
    with db.read() as conn:
        queues = {r["stage"]: {"waiting": r["waiting"], "running": r["running"], "parked": r["parked"],
                               "oldest_waiting_age_s": round(now - r["oldest"], 1) if r["oldest"] else 0}
                  for r in conn.execute(
                      "SELECT stage, SUM(status='queued') AS waiting, SUM(status='running') AS running, "
                      "SUM(status='waiting_vendor') AS parked, MIN(CASE WHEN status='queued' THEN created_at END) "
                      "AS oldest FROM jobs GROUP BY stage")}
        outcomes = {r["k"]: r["n"] for r in conn.execute(
            "SELECT CASE WHEN status='failed' THEN 'failed:' || COALESCE(error_code, '?') ELSE status END AS k, "
            "COUNT(*) AS n FROM files GROUP BY k")}
        dlq = conn.execute("SELECT COUNT(*) FROM dlq WHERE replayed_at IS NULL").fetchone()[0]
        vendor_calls = {r["vendor"]: {"calls_window": r["n"], "failures_window": r["f"], "retries_window": r["rt"]}
                        for r in conn.execute(
                            "SELECT vendor, COUNT(*) AS n, SUM(1-ok) AS f, SUM(is_retry) AS rt "
                            "FROM vendor_call_log GROUP BY vendor")}
    return {"queues": queues, "file_outcomes": outcomes, "dlq_size": dlq, "vendors": vendor_calls,
            "breakers": {v: breaker.state(v)["state"] for v in VENDORS}}


# ---- fault injection for the mock vendors ----

@router.get("/__faults")
def get_faults():
    return adapters.get().faults.all()


@router.post("/__faults/{vendor}")
def set_faults(vendor: str, profile: dict):
    if vendor not in VENDORS:
        raise ApiError(404, "NOT_FOUND")
    try:
        return {vendor: adapters.get().faults.set(vendor, profile)}
    except ValueError as e:
        raise ApiError(400, "VALIDATION_ERROR", str(e)) from None


@router.delete("/__faults/{vendor}")
def clear_faults(vendor: str):
    if vendor not in VENDORS:
        raise ApiError(404, "NOT_FOUND")
    return {vendor: adapters.get().faults.set(vendor, {})}
