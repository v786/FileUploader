"""Unit tests: breaker transitions, backoff maths, classifier, token bucket, state guards."""
import pytest

from fileuploader import breaker, db, ratelimit, state, worker
from fileuploader.errors import (BreakerOpen, Kind, VendorConnectionError, VendorHTTPError, VendorTimeout,
                                 classify)


def _calls(vendor: str, ok: bool, n: int, timeout: bool = False):
    for _ in range(n):
        p = breaker.before_call(vendor)
        breaker.record(p, ok=ok, timeout=timeout)


def test_breaker_opens_on_failure_ratio(env):
    _calls("v", True, 10)
    _calls("v", False, 9)
    assert breaker.state("v")["state"] == "closed"  # 19 calls < minimum of 20
    _calls("v", False, 1)
    assert breaker.state("v")["state"] == "open"
    with pytest.raises(BreakerOpen):
        breaker.before_call("v")


def test_breaker_opens_on_consecutive_timeouts(env):
    _calls("v", False, 4, timeout=True)
    _calls("v", True, 1)  # resets the streak
    _calls("v", False, 4, timeout=True)
    assert breaker.state("v")["state"] == "closed"
    _calls("v", False, 1, timeout=True)
    assert breaker.state("v")["state"] == "open"


def test_breaker_4xx_never_trips(env):
    for _ in range(30):
        p = breaker.before_call("v")
        breaker.record(p, ok=False, counts=False)
    assert breaker.state("v")["state"] == "closed"


def test_half_open_admits_three_trials_and_cooldown_doubles(env):
    _calls("v", False, 5, timeout=True)
    st = breaker.state("v")
    assert st["state"] == "open" and st["cooldown_s"] == 30
    env.clock.advance(30)
    permits = [breaker.before_call("v") for _ in range(3)]
    with pytest.raises(BreakerOpen):
        breaker.before_call("v")  # only 3 trials across the fleet
    breaker.record(permits[0], ok=False)  # any trial failure re-opens, cool-down doubles
    st = breaker.state("v")
    assert st["state"] == "open" and st["cooldown_s"] == 60

    env.clock.advance(60)
    for _ in range(3):
        breaker.record(breaker.before_call("v"), ok=True)
    assert breaker.state("v")["state"] == "closed"


def test_cooldown_capped_at_five_minutes(env):
    _calls("v", False, 5, timeout=True)
    for _ in range(6):
        env.clock.advance(breaker.state("v")["cooldown_s"])
        breaker.record(breaker.before_call("v"), ok=False)
    assert breaker.state("v")["cooldown_s"] == 300


def test_backoff_full_jitter_bounds(env):
    for attempt in range(1, 12):
        for _ in range(50):
            d = worker.backoff_delay(attempt)
            assert 0 <= d <= min(300, 2 * 2 ** attempt)


def test_classifier():
    assert classify(VendorTimeout("x")).kind == Kind.TRANSIENT
    assert classify(VendorConnectionError("x")).kind == Kind.TRANSIENT
    assert classify(VendorHTTPError(503)).kind == Kind.TRANSIENT
    e = classify(VendorHTTPError(429, retry_after_s=3))
    assert e.kind == Kind.TRANSIENT and e.retry_after_s == 3
    assert classify(VendorHTTPError(422, {"code": "CORRUPT_FILE"})).code == "CORRUPT_FILE"
    assert classify(VendorHTTPError(400)).kind == Kind.PERMANENT
    assert classify(KeyError("boom")).kind == Kind.UNKNOWN


def test_token_bucket(env):
    spec = ratelimit.BucketSpec("k", rate=2, burst=3)
    assert [ratelimit.take([spec]).allowed for _ in range(4)] == [True, True, True, False]
    d = ratelimit.take([spec])
    assert d.wait_s == pytest.approx(0.5)
    env.clock.advance(0.5)
    assert ratelimit.take([spec]).allowed


def test_multi_bucket_take_is_all_or_nothing(env):
    fast = ratelimit.BucketSpec("fast", rate=100, burst=100)
    hourly = ratelimit.BucketSpec("hourly", rate=0.001, burst=2)
    assert ratelimit.take([fast, hourly]).allowed
    assert ratelimit.take([fast, hourly]).allowed
    assert not ratelimit.take([fast, hourly]).allowed
    with db.read() as conn:
        assert conn.execute("SELECT tokens FROM rate_buckets WHERE key='fast'").fetchone()[0] == pytest.approx(98)


def test_state_guard_rejects_stale_version(env):
    env.create_batch([{"client_file_id": "a", "name": "a.txt", "size_bytes": 1, "sha256": "0" * 64}])
    with db.tx() as conn:
        f = conn.execute("SELECT * FROM files").fetchone()
        assert state.transition(conn, f, "uploading")
        assert not state.transition(conn, f, "uploading")  # stale version: zero rows
        with pytest.raises(state.InvalidTransition):
            state.transition(conn, state.get_file(conn, f["id"]), "completed")
