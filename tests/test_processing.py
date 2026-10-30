"""External processing API: async jobs, webhooks + polling fallback, combined progress."""
import json

from fileuploader import adapters, breaker, db, events, worker
from tests.conftest import TEST_LIMITS, spec, typed


def _uploaded(env, names_data: list[tuple[str, bytes]]) -> list[dict]:
    names_data = [(n, typed(n, d)) for n, d in names_data]
    files = env.create_batch([spec(n, d) for n, d in names_data])["files"]
    for f, (_, d) in zip(files, names_data):
        assert env.upload(f, d).status_code == 200
    return files


def _job(fid: str, stage: str = "process"):
    with db.read() as conn:
        return conn.execute("SELECT * FROM jobs WHERE id = ?", (f"{stage}:{fid}",)).fetchone()


def _stage_events(batch_id: str, fid: str) -> list[dict]:
    events.relay_once()
    return [e["data"] for e in events.read_events(batch_id, 0)
            if e["type"] == "file.stage" and e["data"]["file_id"] == fid]


def test_webhook_completes_before_the_next_poll(env):
    env.faults("processor", {"duration_ms": 30_000})
    f = _uploaded(env, [("doc.pdf", b"x" * 50)])[0]
    p = adapters.get().processor
    env.run()
    job = _job(f["id"])
    assert job["status"] == "polling" and job["vendor_job_id"] and p.calls == 1 and p.polls == 1

    env.clock.advance(31)  # vendor done; our next poll isn't due yet
    env.run()
    assert p.polls == 1  # the webhook got there first
    assert env.file(f["id"])["status"] == "completed"
    with db.read() as conn:
        assert conn.execute("SELECT COUNT(*) FROM webhook_events").fetchone()[0] >= 1

    res = env.client.get(f"/files/{f['id']}/result", headers=env.h()).json()
    assert res["result"]["process"] == {"result_key": f"results/{f['id']}/process.json"}
    assert env.client.get(res["process_result_url"]).json()["kind"] == "processed"


def test_dropped_webhooks_fall_back_to_polling(env):
    env.faults("processor", {"duration_ms": 30_000, "webhook_drop_rate": 1.0})
    f = _uploaded(env, [("a.txt", b"a" * 20)])[0]
    env.drain(step=15)
    p = adapters.get().processor
    assert env.file(f["id"])["status"] == "completed"
    assert p.webhooks_sent == 0 and p.polls >= 2


def test_poll_backoff_from_10s_to_60s(env):
    env.faults("processor", {"duration_ms": 10_000_000, "webhook_drop_rate": 1.0})
    f = _uploaded(env, [("slow.txt", b"s" * 20)])[0]
    env.run()
    waits = []
    for _ in range(7):
        job = _job(f["id"])
        waits.append(round(job["next_run_at"] - env.clock.now(), 1))
        env.clock.advance(job["next_run_at"] - env.clock.now())
        env.run()
    assert waits == [10.0, 15.0, 22.5, 33.8, 50.6, 60.0, 60.0]


def test_duplicate_and_out_of_order_webhooks_are_harmless(env):
    env.faults("processor", {"duration_ms": 50_000, "webhook_duplicate_rate": 1.0, "webhook_out_of_order": True,
                             "webhook_delay_ms": 20_000})
    files = _uploaded(env, [(f"w{i}.txt", f"w{i}".encode() * 9) for i in range(3)])
    env.drain(step=7)
    p = adapters.get().processor
    assert all(env.file(f["id"])["status"] == "completed" for f in files)
    with db.read() as conn:
        unique = conn.execute("SELECT COUNT(*) FROM webhook_events").fetchone()[0]
    assert unique > 3 and p.webhooks_sent == 2 * unique  # every event arrived twice, applied once
    for f in files:
        pcts = [e["processing_pct"] for e in _stage_events(f["batch_id"], f["id"])]
        assert pcts == sorted(pcts)  # stale (out-of-order) statuses never move the bar back


def test_older_or_repeated_vendor_status_is_ignored(env):
    env.faults("processor", {"duration_ms": 100_000, "webhook_drop_rate": 1.0})
    f = _uploaded(env, [("o.txt", b"o" * 30)])[0]
    env.run()
    vid, t = _job(f["id"])["vendor_job_id"], env.clock.now()
    assert worker.apply_vendor_update(vid, {"status": "running", "progress_pct": 60, "at": t + 5}, "e1") == "running"
    assert worker.apply_vendor_update(vid, {"status": "running", "progress_pct": 30, "at": t + 2}, "e2") == "running"
    assert worker.apply_vendor_update(vid, {"status": "queued", "at": t + 9}, "e3") == "running"
    assert worker.apply_vendor_update(vid, {"status": "running", "progress_pct": 90, "at": t + 9}, "e1") == "duplicate"
    job = _job(f["id"])
    assert job["vendor_progress"] == 60 and job["vendor_updated_at"] == t + 5
    assert worker.apply_vendor_update(vid, {"status": "done", "result": {}, "at": t + 20}, "e4") == "done"
    assert worker.apply_vendor_update(vid, {"status": "done", "result": {}, "at": t + 21}, None) == "closed"


def test_webhook_signature_is_verified(env):
    r = env.client.post("/webhooks/processor", content=b'{"job_id": "x", "event_id": "e"}',
                        headers={"X-Processor-Signature": "sha256=00"})
    assert r.status_code == 401 and r.json()["code"] == "INVALID_SIGNATURE"


def test_combined_progress_percent_mode(env):
    env.faults("processor", {"duration_ms": 100_000})
    f = _uploaded(env, [("p.txt", b"p" * 40)])[0]
    env.run()
    for _ in range(8):
        env.clock.advance(10)
        env.run()
    mid = env.client.get(f"/batches/{f['batch_id']}", headers=env.h()).json()["files"][0]
    assert mid["upload_pct"] == 100 and 0 < mid["processing_pct"] < 100  # snapshot after a refresh

    env.drain()
    evs = _stage_events(f["batch_id"], f["id"])
    pcts = [e["processing_pct"] for e in evs]
    assert pcts == sorted(pcts) and max(pcts) < 100  # 100% only on file.completed
    assert any(e["stage"] == "process" and e.get("vendor_status") == "running" for e in evs)
    assert env.file(f["id"])["processing_pct"] == 100


def test_states_only_vendor_is_estimated_and_capped_at_90(env):
    env.settings.process_deadline_s = 10_000
    env.faults("processor", {"duration_ms": 10_000_000, "progress_mode": "states"})
    f = _uploaded(env, [("st.txt", b"s" * 30)])[0]
    for _ in range(12):
        env.run()
        env.clock.advance(60)
    job = _job(f["id"])
    assert job["vendor_status"] == "running" and job["vendor_progress"] == 90.0


def test_queued_at_vendor_holds_the_bar(env):
    env.faults("processor", {"queue_ms": 60_000, "duration_ms": 1000, "webhook_drop_rate": 1.0})
    f = _uploaded(env, [("q.txt", b"q" * 30)])[0]
    env.run()
    job = _job(f["id"])
    assert job["vendor_status"] == "queued" and job["vendor_queue_position"] == 60 and not job["vendor_progress"]
    queued = [e for e in _stage_events(f["batch_id"], f["id"]) if e.get("vendor_status") == "queued"]
    assert queued and queued[-1]["queue_position"] == 60
    env.drain(step=30)
    assert env.file(f["id"])["status"] == "completed"


def test_process_parallel_with_scan_flag(env):
    env.settings.process_parallel_with_scan = True
    f, bad = _uploaded(env, [("par.txt", b"p" * 30), ("EICAR-par.txt", b"e")])
    events.relay_once()
    assert _job(f["id"], "process")["status"] == "queued" and _job(f["id"], "scan")["status"] == "queued"
    env.drain()
    assert env.file(f["id"])["status"] == "completed"
    got = env.file(bad["id"])
    assert got["status"] == "failed" and got["error"]["code"] == "MALWARE_DETECTED"
    with db.read() as conn:
        assert conn.execute("SELECT COUNT(*) FROM jobs WHERE id = ?", (f"finalize:{bad['id']}",)).fetchone()[0] == 0


def test_expired_source_url_is_resigned_on_retry(env):
    env.settings.process_source_url_ttl_s = -10  # a backlog outlived the URL
    f = _uploaded(env, [("late.txt", b"l" * 30)])[0]
    env.run()
    job = _job(f["id"])
    assert job["status"] == "queued" and job["attempt"] == 1 and job["vendor_job_id"] is None
    assert json.loads(job["last_error"])[-1]["code"] == "SOURCE_URL_EXPIRED"
    assert breaker.state("processor")["state"] == "closed"

    env.settings.process_source_url_ttl_s = 3600
    env.drain()
    assert env.file(f["id"])["status"] == "completed"


def test_polls_have_their_own_bucket(env):
    limits = dict(TEST_LIMITS)
    limits["outbound"] = {**limits["outbound"], "processor": [{"rate": 0.001, "burst": 3}],
                          "processor_poll": [{"rate": 0.001, "burst": 1}]}
    env.write_limits(limits)
    files = _uploaded(env, [(f"b{i}.txt", f"b{i}".encode() * 5) for i in range(3)])
    env.run()
    p = adapters.get().processor
    assert p.calls == 3 and p.polls == 1  # polls ran dry; submits weren't touched
    assert all(_job(f["id"])["vendor_job_id"] for f in files)
    env.drain(step=5)  # webhooks still complete every job
    assert all(env.file(f["id"])["status"] == "completed" for f in files)


def test_user_retry_after_process_failure_submits_a_new_vendor_job(env):
    f = _uploaded(env, [("bad.txt", b"b" * 30)])[0]
    env.faults("processor", {"fail_file_ids": [f["id"]]})
    env.drain()
    got = env.file(f["id"])
    assert got["status"] == "failed" and got["error"]["code"] == "CORRUPT_FILE" and got["error"]["retryable_by_user"]

    env.faults("processor", {})
    assert env.client.post(f"/files/{f['id']}/retry", headers=env.h()).status_code == 200
    env.drain()
    p = adapters.get().processor
    assert env.file(f["id"])["status"] == "completed"
    assert len(p.seen_keys) == 2 and adapters.get().scanner.calls == 1


def test_process_deadline_retries_then_dlq_replay(env):
    env.settings.process_deadline_s = 30
    env.settings.breaker_min_calls = 1000
    env.faults("processor", {"duration_ms": 10_000_000})
    f = _uploaded(env, [("stuck.txt", b"s" * 30)])[0]
    env.drain(step=400)
    got = env.file(f["id"])
    assert got["status"] == "failed" and got["error"]["code"] == "RETRIES_EXHAUSTED"
    [dead] = env.client.get("/admin/dlq").json()["items"]
    assert dead["stage"] == "process" and {e["code"] for e in dead["error_chain"]} == {"PROCESS_DEADLINE"}

    env.faults("processor", {})
    assert env.client.post("/admin/dlq/process/replay").json() == {"replayed": 1}
    env.drain()
    assert env.file(f["id"])["status"] == "completed"
    assert _job(f["id"])["generation"] == 1


def test_processing_estimate_learns_from_history(env):
    prior = env.create_batch([spec("e0.txt", b"e")])["files"][0]["processing_estimate_ms"]
    assert prior == 25_500  # scan 5 + max(extract 8, thumbnail 4, process 20) + finalize 0.5
    files = _uploaded(env, [(f"e{i}.txt", f"e{i}".encode()) for i in range(1, 7)])
    env.drain(step=400)
    assert all(env.file(f["id"])["status"] == "completed" for f in files)
    learned = env.create_batch([spec("e9.txt", b"z")])["files"][0]["processing_estimate_ms"]
    assert learned != prior and learned > 0
