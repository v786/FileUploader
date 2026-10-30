"""Per-vendor circuit breaker with state shared through SQLite.

closed -> open     : >= 50% failures over a rolling 30 s window with >= 20 calls,
                     or 5 timeouts in a row
open -> half_open  : after the cool-down (30 s, doubling on each re-open, capped at 5 min)
half_open -> closed: 3 trial calls in a row succeed
half_open -> open  : any trial call fails

Half-open admits at most 3 in-flight trials across the whole fleet; the
counter lives in the breakers row and is updated under the write lock.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from . import clock, db
from .config import get_settings
from .errors import BreakerOpen


@dataclass
class Permit:
    vendor: str
    trial: bool  # True if admitted as a half-open trial call


def _row(conn: sqlite3.Connection, vendor: str) -> sqlite3.Row:
    conn.execute("INSERT OR IGNORE INTO breakers (vendor) VALUES (?)", (vendor,))
    return conn.execute("SELECT * FROM breakers WHERE vendor = ?", (vendor,)).fetchone()


def state(vendor: str) -> dict:
    with db.tx() as conn:
        return dict(_row(conn, vendor))


def before_call(vendor: str) -> Permit:
    """Raises BreakerOpen if the call must not go out."""
    s = get_settings()
    now = clock.now()
    with db.tx() as conn:
        r = _row(conn, vendor)
        st = r["state"]
        if st == "open":
            retry_at = r["opened_at"] + r["cooldown_s"]
            if now < retry_at:
                raise BreakerOpen(vendor, retry_at)
            conn.execute(
                "UPDATE breakers SET state='half_open', half_open_successes=0, half_open_inflight=0 WHERE vendor=?",
                (vendor,),
            )
            st = "half_open"
            r = _row(conn, vendor)
        if st == "half_open":
            if r["half_open_inflight"] + r["half_open_successes"] >= s.breaker_half_open_trials:
                # Trials already out; everyone else waits a moment.
                raise BreakerOpen(vendor, now + 1.0)
            conn.execute("UPDATE breakers SET half_open_inflight = half_open_inflight + 1 WHERE vendor=?", (vendor,))
            return Permit(vendor, trial=True)
        return Permit(vendor, trial=False)


def _open(conn: sqlite3.Connection, vendor: str, reopen: bool, now: float) -> None:
    s = get_settings()
    r = _row(conn, vendor)
    cooldown = min(s.breaker_cooldown_cap_s, r["cooldown_s"] * 2) if reopen and r["cooldown_s"] else s.breaker_cooldown_s
    conn.execute(
        "UPDATE breakers SET state='open', opened_at=?, cooldown_s=?, half_open_successes=0, "
        "half_open_inflight=0, consecutive_timeouts=0 WHERE vendor=?",
        (now, cooldown, vendor),
    )


def record(permit: Permit, ok: bool, timeout: bool = False, counts: bool = True, is_retry: bool = False) -> None:
    """Record a call outcome. `counts=False` for errors that are the file's fault (4xx)."""
    s = get_settings()
    now = clock.now()
    vendor = permit.vendor
    failed = counts and not ok
    with db.tx() as conn:
        conn.execute(
            "INSERT INTO vendor_call_log (vendor, ts, ok, is_retry) VALUES (?, ?, ?, ?)",
            (vendor, now, 0 if failed else 1, 1 if is_retry else 0),
        )
        conn.execute("DELETE FROM vendor_call_log WHERE vendor=? AND ts < ?",
                     (vendor, now - max(s.breaker_window_s, s.retry_budget_window_s)))
        r = _row(conn, vendor)

        if permit.trial:
            if r["state"] != "half_open":
                return
            if failed:
                _open(conn, vendor, reopen=True, now=now)
                return
            successes = r["half_open_successes"] + 1
            if successes >= s.breaker_half_open_trials:
                conn.execute(
                    "UPDATE breakers SET state='closed', opened_at=NULL, cooldown_s=0, half_open_successes=0, "
                    "half_open_inflight=0, consecutive_timeouts=0 WHERE vendor=?",
                    (vendor,),
                )
                # Fresh window after recovery.
                conn.execute("DELETE FROM vendor_call_log WHERE vendor=? AND ts < ?", (vendor, now))
            else:
                conn.execute(
                    "UPDATE breakers SET half_open_successes=?, half_open_inflight=MAX(0, half_open_inflight-1) "
                    "WHERE vendor=?",
                    (successes, vendor),
                )
            return

        if r["state"] != "closed":
            return
        consecutive = r["consecutive_timeouts"] + 1 if (timeout and counts) else 0
        conn.execute("UPDATE breakers SET consecutive_timeouts=? WHERE vendor=?", (consecutive, vendor))
        if consecutive >= s.breaker_consecutive_timeouts:
            _open(conn, vendor, reopen=False, now=now)
            return
        if failed:
            total, fails = conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(1 - ok), 0) FROM vendor_call_log WHERE vendor=? AND ts >= ?",
                (vendor, now - s.breaker_window_s),
            ).fetchone()
            if total >= s.breaker_min_calls and fails / total >= s.breaker_failure_ratio:
                _open(conn, vendor, reopen=False, now=now)


def release(permit: Permit) -> None:
    """A permitted call never went out (e.g. no rate-limit token): give back the trial slot."""
    if not permit.trial:
        return
    with db.tx() as conn:
        conn.execute("UPDATE breakers SET half_open_inflight = MAX(0, half_open_inflight - 1) WHERE vendor=?",
                     (permit.vendor,))


def retry_budget_ok(vendor: str) -> bool:
    """Retries may be at most 20% of a vendor's recent call volume."""
    s = get_settings()
    now = clock.now()
    with db.read() as conn:
        total, retries = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(is_retry), 0) FROM vendor_call_log WHERE vendor=? AND ts >= ?",
            (vendor, now - s.retry_budget_window_s),
        ).fetchone()
    if total < s.retry_budget_min_calls:
        return True
    return (retries + 1) / (total + 1) <= s.retry_budget_ratio
