"""Pipeline, resilience and idempotency tests, driven through the mocks."""
import asyncio
import json

from fileuploader import adapters, breaker, db, events, worker
from fileuploader.adapters.vendors import EICAR
from tests.conftest import TEST_LIMITS, spec


def _uploaded(env, names_data: list[tuple[str, bytes]]) -> list[dict]:
    files = env.create_batch([spec(n, d) for n, d in names_data])["files"]
    for f, (_, d) in zip(files, names_data):
        assert env.upload(f, d).status_code == 200
    return files


def _events(env, batch_id: str, after: int = 0) -> list[dict]:
    events.relay_once()
    return events.read_events(batch_id, after)


def test_stages_and_events(env):
    f = _uploaded(env, [("pic.png", b"img" * 100)])[0]
    env.drain()
    evs = _events(env, f["batch_id"])
    seqs = [e["seq"] for e in evs]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)
    stages = [e["data"]["stage"] for e in evs if e["type"] == "file.stage"]
    assert stages[0] == "uploaded" and "scan" in stages and "finalize" in stages
    assert {"extract", "thumbnail"} <= set(stages)
    assert evs[-1]["type"] == "batch.summary" and evs[-1]["data"]["text"] == "1 of 1 done"
    assert any(e["type"] == "file.completed" for e in evs)


def test_malware_fails_alone_and_is_not_user_retryable(env):
    good, bad, by_content = _uploaded(env, [("ok.txt", b"hello" * 50), ("EICAR-test.txt", b"x" * 10),
                                            ("sneaky.txt", b"prefix" + EICAR + b"suffix")])
    env.drain()
    assert env.file(good["id"])["status"] == "completed"
    for f in (bad, by_content):
        got = env.file(f["id"])
        assert got["status"] == "failed" and got["error"]["code"] == "MALWARE_DETECTED"
        assert got["error"]["retryable_by_user"] is False
    r = env.client.post(f"/files/{bad['id']}/retry", headers=env.h())
    assert r.status_code == 409 and r.json()["code"] == "NOT_RETRYABLE"
    summary = env.client.get(f"/batches/{good['batch_id']}", headers=env.h()).json()["summary"]
    assert summary["text"] == "1 of 3 done, 2 need attention"
    # Extract / thumbnail never touched the infected files.
    with db.read() as conn:
        assert conn.execute("SELECT COUNT(*) FROM jobs WHERE file_id IN (?, ?) AND stage != 'scan'",
                            (bad["id"], by_content["id"])).fetchone()[0] == 0


def test_flaky_vendors_all_files_still_complete(env):
    for v in ("scanner", "extractor", "thumbnailer"):
        env.faults(v, {"error_rate": 0.3, "error_status": 503})
    files = _uploaded(env, [(f"f{i}.txt", f"file {i}".encode() * 20) for i in range(8)])
    env.drain(rounds=200)
    assert all(env.file(f["id"])["status"] == "completed" for f in files)
    evs = _events(env, files[0]["batch_id"])
    assert any(e["type"] == "file.retrying" for e in evs)


def test_vendor_down_parks_then_resumes(env):
    env.settings.breaker_min_calls = 3
    env.faults("scanner", {"down": True})
    files = _uploaded(env, [(f"f{i}.txt", f"d{i}".encode() * 10) for i in range(5)])
    for _ in range(6):
        env.run()
        env.clock.advance(20)  # past backoff, short of the 30 s breaker cool-down
    assert breaker.state("scanner")["state"] == "open"
    statuses = {env.file(f["id"])["status"] for f in files}
    assert "waiting_vendor" in statuses and "failed" not in statuses
    evs = _events(env, files[0]["batch_id"])
    assert any(e["type"] == "file.waiting" and e["data"]["vendor"] == "scanner" for e in evs)

    calls_before = adapters.get().scanner.calls
    env.run()
    assert adapters.get().scanner.calls == calls_before  # open breaker: nobody calls the vendor

    env.faults("scanner", {})
    env.drain()
    assert all(env.file(f["id"])["status"] == "completed" for f in files)
    assert breaker.state("scanner")["state"] == "closed"


def test_permanent_vendor_error_and_user_retry_reruns_only_failed_stage(env):
    f = _uploaded(env, [("doc.pdf", b"%PDF" * 100)])[0]
    env.faults("extractor", {"fail_file_ids": [f["id"]]})
    env.drain()
    got = env.file(f["id"])
    assert got["status"] == "failed" and got["error"]["code"] == "CORRUPT_FILE" and got["error"]["retryable_by_user"]
    assert breaker.state("extractor")["state"] == "closed"  # a 4xx is the file's fault, not the vendor's

    scans = adapters.get().scanner.calls
    env.faults("extractor", {})
    r = env.client.post(f"/files/{f['id']}/retry", headers=env.h())
    assert r.status_code == 200 and r.json()["status"] == "processing"
    env.drain()
    assert env.file(f["id"])["status"] == "completed"
    # The completed scan stage is kept; the thumbnail (skipped or done) runs at most once overall.
    assert adapters.get().scanner.calls == scans == 1
    assert adapters.get().thumbnailer.calls == 1


def test_retries_exhausted_go_to_dlq_and_replay(env):
    env.settings.max_attempts = 3
    env.settings.breaker_min_calls = 1000  # keep the breaker out of this test
    env.faults("thumbnailer", {"error_rate": 1.0, "error_status": 502})
    f = _uploaded(env, [("a.png", b"png" * 30)])[0]
    env.drain()
    got = env.file(f["id"])
    assert got["status"] == "failed" and got["error"]["code"] == "RETRIES_EXHAUSTED"
    dlq = env.client.get("/admin/dlq").json()["items"]
    assert len(dlq) == 1 and dlq[0]["stage"] == "thumbnail" and len(dlq[0]["error_chain"]) == 3

    env.faults("thumbnailer", {})
    assert env.client.post("/admin/dlq/thumbnail/replay").json() == {"replayed": 1}
    env.drain()
    assert env.file(f["id"])["status"] == "completed"
    assert env.client.get("/admin/dlq").json()["items"] == []


def test_hanging_vendor_times_out_and_retries(env):
    env.settings.stages["thumbnail"].timeout_s = 0.05
    env.faults("thumbnailer", {"hang": True})
    f = _uploaded(env, [("h.png", b"h" * 10)])[0]
    env.run()
    with db.read() as conn:
        job = conn.execute("SELECT * FROM jobs WHERE id = ?", (f"thumbnail:{f['id']}",)).fetchone()
    assert job["status"] == "queued" and job["attempt"] == 1
    assert json.loads(job["last_error"])[-1]["code"] == "VENDOR_TIMEOUT"
    assert env.file(f["id"])["status"] == "failed_retryable"
    env.faults("thumbnailer", {})
    env.drain()
    assert env.file(f["id"])["status"] == "completed"


def test_vendor_rate_limit_delays_without_spending_attempts(env):
    limits = dict(TEST_LIMITS)
    limits["outbound"] = {**limits["outbound"], "scanner": [{"rate": 0.5, "burst": 1}]}
    env.write_limits(limits)
    files = _uploaded(env, [("a.txt", b"a" * 5), ("b.txt", b"b" * 5), ("c.txt", b"c" * 5)])
    env.run()
    with db.read() as conn:
        scan_jobs = conn.execute("SELECT status, attempt FROM jobs WHERE stage = 'scan'").fetchall()
    assert sum(j["status"] == "done" for j in scan_jobs) == 1
    assert all(j["attempt"] == 0 for j in scan_jobs)
    env.drain(step=2)
    assert all(env.file(f["id"])["status"] == "completed" for f in files)


def test_vendor_429_honours_retry_after(env):
    env.faults("extractor", {"rate_limit_rps": 1})
    files = _uploaded(env, [(f"r{i}.txt", f"r{i}".encode()) for i in range(3)])
    env.drain(step=5)
    assert all(env.file(f["id"])["status"] == "completed" for f in files)


def test_every_job_run_twice_has_no_duplicate_effects(env):
    f = _uploaded(env, [("twice.pdf", b"t" * 300)])[0]
    env.drain()
    vendors = adapters.get()
    calls = (vendors.scanner.calls, vendors.extractor.calls, vendors.thumbnailer.calls)
    with db.read() as conn:
        seq_before = conn.execute("SELECT last_seq FROM batches WHERE id = ?", (f["batch_id"],)).fetchone()[0]

    # Re-deliver every job (at-least-once queue) and re-publish every outbox row.
    with db.tx() as conn:
        conn.execute("UPDATE jobs SET status='queued', next_run_at=0")
        conn.execute("UPDATE outbox SET published_at = NULL")
    env.drain()

    assert (vendors.scanner.calls, vendors.extractor.calls, vendors.thumbnailer.calls) == calls
    with db.read() as conn:
        assert conn.execute("SELECT COUNT(*) FROM jobs WHERE file_id = ?", (f["id"],)).fetchone()[0] == 4
        assert conn.execute("SELECT COUNT(*) FROM vendor_calls").fetchone()[0] == 3
        assert conn.execute("SELECT status FROM files WHERE id = ?", (f["id"],)).fetchone()[0] == "completed"
        # Duplicate events got no new seqs; replays of the outbox were no-ops.
        assert conn.execute("SELECT last_seq FROM batches WHERE id = ?", (f["batch_id"],)).fetchone()[0] == seq_before
    # Every vendor saw a stable idempotency key exactly once.
    assert all(n == 1 for n in vendors.scanner.seen_keys.values())


def test_fan_in_enqueues_finalize_once(env):
    f = _uploaded(env, [("fan.png", b"f" * 64)])[0]
    env.run()
    with db.read() as conn:
        rows = conn.execute("SELECT payload FROM outbox WHERE topic = 'enqueue'").fetchall()
    finals = [r for r in rows if json.loads(r["payload"])["stage"] == "finalize"]
    assert len(finals) == 1
    assert env.file(f["id"])["status"] == "completed"


def test_stalled_job_is_rescued_and_late_worker_is_a_noop(env):
    f = _uploaded(env, [("s.txt", b"s" * 64)])[0]
    events.relay_once()
    [crashed] = worker.claim("scan", 1, "worker-a")  # worker-a claims, then "dies"
    assert worker.claim("scan", 1, "worker-b") == []  # lease still held
    env.clock.advance(env.settings.job_lock_s + 1)
    [rescued] = worker.claim("scan", 1, "worker-b")
    asyncio.run(worker.process(rescued))
    # worker-a wakes up and tries to finish: guarded update affects nothing.
    worker._on_success(crashed, {"verdict": "clean"})
    with db.read() as conn:
        assert conn.execute("SELECT COUNT(*) FROM outbox WHERE topic='enqueue' AND payload LIKE '%extract%'"
                            ).fetchone()[0] == 1
    env.drain()
    assert env.file(f["id"])["status"] == "completed"


def test_per_user_fairness_cap(env):
    env.settings.max_active_jobs_per_user = 2
    _uploaded(env, [(f"u{i}.txt", f"u{i}".encode()) for i in range(5)])
    events.relay_once()
    assert len(worker.claim("scan", 8, "w")) == 2


def test_cancelled_file_jobs_do_not_run(env):
    f = _uploaded(env, [("c.txt", b"c" * 20)])[0]
    env.client.post(f"/files/{f['id']}/cancel", headers=env.h())
    env.drain()
    assert env.file(f["id"])["status"] == "cancelled"
    assert adapters.get().scanner.calls == 0


def test_sse_snapshot_and_replay(env):
    f = _uploaded(env, [("e.txt", b"e" * 20)])[0]
    env.drain()
    bid = f["batch_id"]
    with env.client.stream("GET", f"/batches/{bid}/events?once=true", headers=env.h()) as r:
        body = "".join(r.iter_text())
    assert "event: snapshot" in body

    evs = events.read_events(bid, 0)
    mid = evs[len(evs) // 2]["seq"]
    with env.client.stream("GET", f"/batches/{bid}/events?once=true",
                           headers=env.h(**{"Last-Event-ID": str(mid)})) as r:
        body = "".join(r.iter_text())
    ids = [int(line[4:]) for line in body.splitlines() if line.startswith("id: ")]
    assert "snapshot" not in body and ids == [e["seq"] for e in evs if e["seq"] > mid]

    # Stream expired (older than 1 h) -> snapshot instead of replay.
    with db.tx() as conn:
        conn.execute("DELETE FROM events WHERE batch_id = ? AND seq <= ?", (bid, mid + 1))
    with env.client.stream("GET", f"/batches/{bid}/events?once=true",
                           headers=env.h(**{"Last-Event-ID": str(mid)})) as r:
        assert "event: snapshot" in "".join(r.iter_text())


def test_sweeper_aborts_idle_multipart(env):
    from fileuploader.sweeper import sweep_once
    f = env.create_batch([spec("idle.txt", b"i" * 3000)])["files"][0]
    env.client.post(f"/files/{f['id']}/multipart", headers=env.h())
    env.clock.advance(env.settings.multipart_idle_s + 1)
    assert sweep_once()["aborted_uploads"] == 1
    got = env.file(f["id"])
    assert got["status"] == "failed" and got["error"]["code"] == "UPLOAD_EXPIRED"
    assert env.client.post(f"/files/{f['id']}/retry", headers=env.h()).json()["status"] == "pending"


def test_metrics_endpoint(env):
    _uploaded(env, [("m.txt", b"m")])
    env.drain()
    m = env.client.get("/admin/metrics").json()
    assert m["file_outcomes"] == {"completed": 1} and m["dlq_size"] == 0
