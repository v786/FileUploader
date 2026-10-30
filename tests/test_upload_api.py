import hashlib
import uuid

from fileuploader import db
from tests.conftest import TEST_LIMITS, spec


def test_error_contract_shape(env):
    r = env.client.get("/files/nope", headers=env.h())
    assert r.status_code == 404
    assert set(r.json()) == {"code", "message", "retryable", "retry_after_ms", "request_id"}
    assert r.json()["code"] == "NOT_FOUND"
    assert env.client.get("/files/nope").status_code == 401


def test_multipart_upload_end_to_end(env):
    data = (b"%PDF" + bytes(range(252))) * 10  # 2560 bytes -> 3 parts of 1 KiB
    b = env.create_batch([spec("report.pdf", data)])
    f = b["files"][0]
    # One call to start: the multipart upload exists and the first parts are already signed.
    start = f["upload"].pop("start")
    assert f["status"] == "uploading" and f["upload"] == {"mode": "multipart", "part_size": 1024, "part_count": 3,
                                                         "dedupe_candidate": False, "confirmed_bytes": 0}
    assert start["upload_id"] and sorted(start["urls"]) == ["1", "2", "3"]
    assert f["processing_estimate_ms"] > 0 and f["upload_pct"] == 0 and f["processing_pct"] == 0
    r = env.upload(f, data)
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "uploaded"

    env.drain()
    done = env.file(f["id"])
    assert done["status"] == "completed" and done["progress_pct"] == 100.0

    res = env.client.get(f"/files/{f['id']}/result", headers=env.h()).json()
    assert res["result"]["scan"]["verdict"] == "clean"
    assert res["result"]["metadata"]["kind"] == "document"
    assert env.client.get(res["download_url"]).content == data
    assert env.client.get(res["thumbnail_url"]).content.startswith(b"\x89PNG")


def test_small_file_uses_single_put(env):
    data = b"hello world"
    f = env.create_batch([spec("a.txt", data)])["files"][0]
    info = env.client.post(f"/files/{f['id']}/multipart", headers=env.h()).json()
    assert info["mode"] == "single"
    assert env.put(info["url"], data).status_code == 200
    assert env.client.post(f"/files/{f['id']}/complete", headers=env.h()).json()["status"] == "uploaded"


def test_zero_byte_file(env):
    f = env.create_batch([spec("empty.txt", b"")])["files"][0]
    assert env.upload(f, b"").status_code == 200
    env.drain()
    assert env.file(f["id"])["status"] == "completed"


def test_batch_idempotency(env):
    files = [spec("a.txt", b"abc")]
    key = str(uuid.uuid4())
    h = env.h(**{"Idempotency-Key": key})
    r1 = env.client.post("/batches", json={"files": files}, headers=h)
    r2 = env.client.post("/batches", json={"files": files}, headers=h)
    assert r1.status_code == r2.status_code == 201
    assert r1.json()["id"] == r2.json()["id"]
    assert r2.headers.get("Idempotent-Replayed") == "true"

    r3 = env.client.post("/batches", json={"files": [spec("b.txt", b"xyz")]}, headers=h)
    assert r3.status_code == 422 and r3.json()["code"] == "IDEMPOTENCY_KEY_REUSED"

    r4 = env.client.post("/batches", json={"files": files}, headers=env.h())
    assert r4.status_code == 400 and r4.json()["code"] == "IDEMPOTENCY_KEY_REQUIRED"

    with db.read() as conn:
        assert conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1


def test_idempotency_in_flight_returns_409(env):
    key = str(uuid.uuid4())
    files = [spec("a.txt", b"abc")]
    import json as _json

    from fileuploader.api.routes import BatchIn
    from fileuploader.ids import sha256_hex
    body = _json.dumps(BatchIn(files=files).model_dump(), sort_keys=True)
    with db.tx() as conn:
        conn.execute("INSERT INTO idempotency_keys (user_id, key, request_hash, state, created_at) "
                     "VALUES (?, ?, ?, 'in_flight', ?)",
                     (env.user, key, sha256_hex("POST", "/batches", body), env.clock.now()))
    r = env.client.post("/batches", json={"files": files}, headers=env.h(**{"Idempotency-Key": key}))
    assert r.status_code == 409 and r.headers["Retry-After"] == "1"


def test_bad_files_are_isolated_not_batch_fatal(env):
    big = {"client_file_id": "big", "name": "huge.mp4", "size_bytes": env.settings.max_file_size + 1, "sha256": "0" * 64}
    exe = spec("virus.exe", b"MZ")
    ok = spec("ok.txt", b"fine")
    b = env.create_batch([big, exe, ok])
    by = {f["client_file_id"]: f for f in b["files"]}
    assert by["big"]["status"] == "failed" and by["big"]["error"]["code"] == "FILE_TOO_LARGE"
    assert by["virus.exe"]["error"]["code"] == "UNSUPPORTED_TYPE"
    assert by["ok.txt"]["status"] == "uploading"
    assert b["summary"]["need_attention"] == 2
    assert by["big"]["error"]["fix"] == {"action": "split", "label": "Split into parts under 5 GB"}


def test_too_many_files(env):
    files = [spec(f"f{i}.txt", str(i).encode()) for i in range(51)]
    r = env.client.post("/batches", json={"files": files}, headers=env.h(**{"Idempotency-Key": "k"}))
    assert r.status_code == 400 and r.json()["code"] == "TOO_MANY_FILES"


def test_dedupe_by_content(env):
    data = b"same bytes" * 50
    f1 = env.create_batch([spec("one.txt", data)])["files"][0]
    env.upload(f1, data)
    env.drain()
    f2 = env.create_batch([spec("two.txt", data)])["files"][0]
    assert f2["status"] == "completed" and f2["dedup_of"] == f1["id"]
    assert env.client.get(f"/files/{f2['id']}/result", headers=env.h()).status_code == 200


def test_resume_lists_missing_parts(env):
    data = b"x" * 3000
    f = env.create_batch([spec("big.txt", data)])["files"][0]
    fid = f["id"]
    env.client.post(f"/files/{fid}/multipart", headers=env.h())
    # Calling multipart again returns the same upload id.
    up1 = env.client.post(f"/files/{fid}/multipart", headers=env.h()).json()["upload_id"]
    up2 = env.client.post(f"/files/{fid}/multipart", headers=env.h()).json()["upload_id"]
    assert up1 == up2
    url = env.client.post(f"/files/{fid}/parts:sign", json={"part_numbers": [2]}, headers=env.h()).json()["urls"]["2"]
    pr = env.put(url, data[1024:2048])
    env.client.post(f"/files/{fid}/parts/2", json={"etag": pr.headers["ETag"], "size_bytes": 1024}, headers=env.h())

    # "Refresh": the client asks what is confirmed.
    parts = env.client.get(f"/files/{fid}/parts", headers=env.h()).json()
    assert parts["missing"] == [1, 3] and parts["confirmed_bytes"] == 1024
    assert env.client.get("/files?unfinished=true", headers=env.h()).json()["files"][0]["id"] == fid

    r = env.client.post(f"/files/{fid}/complete", headers=env.h())
    assert r.status_code == 409 and r.json()["code"] == "UPLOAD_INCOMPLETE"

    urls = env.client.post(f"/files/{fid}/parts:sign", json={"part_numbers": [1, 3]}, headers=env.h()).json()["urls"]
    for n, chunk in ((1, data[:1024]), (3, data[2048:])):
        pr = env.put(urls[str(n)], chunk)
        env.client.post(f"/files/{fid}/parts/{n}", json={"etag": pr.headers["ETag"], "size_bytes": len(chunk)},
                        headers=env.h())
    assert env.client.post(f"/files/{fid}/complete", headers=env.h()).json()["status"] == "uploaded"
    # Completing twice is harmless.
    assert env.client.post(f"/files/{fid}/complete", headers=env.h()).json()["status"] == "uploaded"


def test_part_report_must_match_storage(env):
    data = b"y" * 2048
    f = env.create_batch([spec("y.txt", data)])["files"][0]
    env.client.post(f"/files/{f['id']}/multipart", headers=env.h())
    r = env.client.post(f"/files/{f['id']}/parts/1", json={"etag": "deadbeef", "size_bytes": 1024}, headers=env.h())
    assert r.status_code == 400


def test_bad_part_checksum_rejected_by_storage(env):
    data = b"z" * 2048
    f = env.create_batch([spec("z.txt", data)])["files"][0]
    env.client.post(f"/files/{f['id']}/multipart", headers=env.h())
    url = env.client.post(f"/files/{f['id']}/parts:sign", json={"part_numbers": [1]}, headers=env.h()).json()["urls"]["1"]
    r = env.client.put(url, content=data[:1024], headers={"x-amz-checksum-sha256": "AAAA"})
    assert r.status_code == 400 and b"BadDigest" in r.content


def test_tampered_presigned_url_rejected(env):
    f = env.create_batch([spec("t.txt", b"abc")])["files"][0]
    url = env.client.post(f"/files/{f['id']}/multipart", headers=env.h()).json()["url"]
    assert env.client.put(url.replace("X-Signature=", "X-Signature=0"), content=b"abc").status_code == 403


def test_checksum_mismatch_fails_file_and_user_can_retry(env):
    real = b"a" * 2048
    wrong = dict(spec("c.txt", real), sha256=hashlib.sha256(b"other").hexdigest())
    f = env.create_batch([wrong])["files"][0]
    r = env.upload(f, real)
    assert r.status_code == 422 and r.json()["code"] == "CHECKSUM_MISMATCH"
    got = env.file(f["id"])
    assert got["status"] == "failed" and got["error"]["retryable_by_user"] is True

    r = env.client.post(f"/files/{f['id']}/retry", headers=env.h())
    assert r.status_code == 200 and r.json()["status"] == "pending"
    assert env.client.get(f"/files/{f['id']}/parts", headers=env.h()).json()["parts"] == []


def test_sign_page_limit(env):
    f = env.create_batch([spec("p.txt", b"p" * 2048)])["files"][0]
    env.client.post(f"/files/{f['id']}/multipart", headers=env.h())
    r = env.client.post(f"/files/{f['id']}/parts:sign", json={"part_numbers": list(range(1, 102))}, headers=env.h())
    assert r.status_code == 400


def test_other_users_cannot_see_files(env):
    f = env.create_batch([spec("a.txt", b"abc")])["files"][0]
    assert env.client.get(f"/files/{f['id']}", headers={"X-User-Id": "someone-else"}).status_code == 404


def test_cancel(env):
    data = b"q" * 2048
    f = env.create_batch([spec("q.txt", data)])["files"][0]
    env.client.post(f"/files/{f['id']}/multipart", headers=env.h())
    assert env.client.post(f"/files/{f['id']}/cancel", headers=env.h()).json()["status"] == "cancelled"
    assert env.client.post(f"/files/{f['id']}/complete", headers=env.h()).status_code == 409


def test_inbound_rate_limit(env):
    limits = dict(TEST_LIMITS)
    limits["inbound"] = {**limits["inbound"], "api": {"rate": 0.001, "burst": 3}}
    env.write_limits(limits)
    codes = [env.client.get("/files", headers=env.h()) for _ in range(4)]
    assert [r.status_code for r in codes] == [200, 200, 200, 429]
    assert codes[0].headers["RateLimit-Limit"] == "3"
    assert codes[3].json()["code"] == "RATE_LIMITED" and int(codes[3].headers["Retry-After"]) > 0


def test_backpressure_returns_503(env):
    env.settings.queue_high_water = 0
    f = env.create_batch([spec("a.txt", b"abc")])["files"][0]
    env.upload(f, b"abc")
    from fileuploader import events
    events.relay_once()
    r = env.client.post("/batches", json={"files": [spec("b.txt", b"b")]}, headers=env.h(**{"Idempotency-Key": "x"}))
    assert r.status_code == 503 and r.json()["code"] == "BUSY" and "Retry-After" in r.headers
