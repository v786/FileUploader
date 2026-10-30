"""Starting uploads fast: one call to start, hashing while uploading, fingerprint
dedupe, fail-fast checks, adaptive part sizes, ListParts and the interactive lane."""
import hashlib

from fileuploader import events, worker
from tests.conftest import spec, typed


def _fp(name: str, data: bytes) -> dict:
    """A file announced by fingerprint only; the full hash comes at complete."""
    return {"client_file_id": name, "name": name, "size_bytes": len(data),
            "fingerprint": f"{len(data)}-{hashlib.md5(data[:64] + data[-64:]).hexdigest()}"}


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_one_call_to_start_returns_signed_first_parts(env):
    data = b"s" * 20 * 1024  # 20 parts of 1 KiB
    f = env.create_batch([spec("big.txt", data)])["files"][0]
    start = f["upload"]["start"]
    assert f["status"] == "uploading" and start["part_count"] == 20
    assert sorted(map(int, start["urls"])) == list(range(1, 9))  # the first 8 parts, signed already
    assert env.put(start["urls"]["1"], data[:1024]).status_code == 200
    small = env.create_batch([spec("small.txt", b"tiny")])["files"][0]
    assert small["upload"]["start"]["mode"] == "single" and small["upload"]["start"]["url"]


def test_hash_arrives_at_complete(env):
    data = b"hello" * 100
    f = env.create_batch([_fp("a.txt", data)])["files"][0]
    assert f["sha256"] is None and f["status"] == "uploading"
    assert env.put(f["upload"]["start"]["url"], data).status_code == 200
    r = env.client.post(f"/files/{f['id']}/complete", headers=env.h())
    assert r.status_code == 400 and r.json()["code"] == "VALIDATION_ERROR"
    r = env.client.post(f"/files/{f['id']}/complete", json={"sha256": _sha(data)}, headers=env.h())
    assert r.status_code == 200 and r.json()["status"] == "uploaded" and r.json()["sha256"] == _sha(data)
    r = env.client.post("/batches", json={"files": [{"client_file_id": "x", "name": "x.txt", "size_bytes": 1}]},
                        headers=env.h(**{"Idempotency-Key": "no-hash"}))
    assert r.status_code == 400


def test_fingerprint_dedupe_is_confirmed_by_the_full_hash(env):
    data = b"dup" * 200
    first = env.create_batch([_fp("one.txt", data)])["files"][0]
    env.put(first["upload"]["start"]["url"], data)
    env.client.post(f"/files/{first['id']}/complete", json={"sha256": _sha(data)}, headers=env.h())
    env.drain()

    again = env.create_batch([_fp("two.txt", data)])["files"][0]
    assert again["status"] == "pending" and again["upload"]["dedupe_candidate"] and "start" not in again["upload"]
    r = env.client.post(f"/files/{again['id']}/complete", json={"sha256": _sha(data)}, headers=env.h())
    assert r.status_code == 200 and r.json()["status"] == "completed" and r.json()["dedup_of"] == first["id"]
    res = env.client.get(f"/files/{again['id']}/result", headers=env.h()).json()
    assert res["process_result_url"] and res["result"]["process"]

    # Same fingerprint, different bytes: not a duplicate after all, so it uploads.
    other = b"dUp" * 200
    f = env.create_batch([dict(_fp("three.txt", other), fingerprint=again["fingerprint"])])["files"][0]
    r = env.client.post(f"/files/{f['id']}/complete", json={"sha256": _sha(other)}, headers=env.h())
    assert r.status_code == 409 and r.json()["code"] == "UPLOAD_REQUIRED"
    info = env.client.post(f"/files/{f['id']}/multipart", headers=env.h()).json()
    env.put(info["url"], other)
    assert env.client.post(f"/files/{f['id']}/complete", headers=env.h()).json()["status"] == "uploaded"


def test_type_is_checked_by_content(env):
    b = env.create_batch([
        dict(spec("x.png", b"GIF89a..."), head_hex=b"GIF89a".hex()),
        dict(spec("notes.txt", b"%PDF-1.4"), head_hex=b"%PDF-1.4".hex()),
        dict(spec("ok.png", typed("ok.png", b"..")), head_hex=typed("ok.png").hex()),
    ])
    by = {f["name"]: f for f in b["files"]}
    assert by["x.png"]["error"]["code"] == "TYPE_MISMATCH" and by["x.png"]["error"]["fix"]["action"] == "replace"
    assert by["notes.txt"]["error"]["code"] == "TYPE_MISMATCH"
    assert by["ok.png"]["status"] == "uploading"

    # The client didn't declare its first bytes (or lied): caught at complete.
    data = b"not really a png"
    f = env.create_batch([spec("liar.png", data)])["files"][0]
    r = env.upload(f, data)
    assert r.status_code == 422 and r.json()["code"] == "TYPE_MISMATCH"
    got = env.file(f["id"])
    assert got["status"] == "failed" and got["error"]["retryable_by_user"] is False


def test_duplicate_names_and_quota_fail_fast(env):
    env.settings.user_quota_bytes = 1000
    b = env.create_batch([spec("a.txt", b"a" * 600), spec("A.TXT", b"b" * 10, cid="a2"),
                          spec("b.txt", b"c" * 600), spec("c.txt", b"d" * 300)])
    codes = {f["client_file_id"]: (f["error"] or {}).get("code") for f in b["files"]}
    assert codes == {"a.txt": None, "a2": "DUPLICATE_NAME", "b.txt": "QUOTA_EXCEEDED", "c.txt": None}


def test_adaptive_part_size(env):
    data = b"p" * 5000
    f = env.create_batch([dict(spec("p.txt", data), part_size=2048)])["files"][0]
    assert f["upload"]["part_size"] == 2048 and f["upload"]["part_count"] == 3
    assert env.upload(f, data).status_code == 200

    r = env.client.post("/batches", json={"files": [dict(spec("q.txt", data), part_size=100)]},
                        headers=env.h(**{"Idempotency-Key": "too-small"}))
    assert r.status_code == 400
    env.settings.max_parts = 2
    r = env.client.post("/batches", json={"files": [dict(spec("r.txt", data), part_size=2048)]},
                        headers=env.h(**{"Idempotency-Key": "too-many"}))
    assert r.status_code == 400
    auto = env.create_batch([spec("s.txt", data)])["files"][0]  # never more than max_parts
    assert auto["upload"]["part_size"] == 2500 and auto["upload"]["part_count"] == 2


def test_complete_uses_list_parts_without_part_reports(env):
    data = b"l" * 3000
    f = env.create_batch([spec("l.txt", data)])["files"][0]
    urls = f["upload"]["start"]["urls"]
    for n in (1, 2):
        env.put(urls[str(n)], data[(n - 1) * 1024: n * 1024])
    parts = env.client.get(f"/files/{f['id']}/parts", headers=env.h()).json()
    assert parts["missing"] == [3] and parts["confirmed_bytes"] == 2048
    r = env.client.post(f"/files/{f['id']}/complete", headers=env.h())
    assert r.status_code == 409 and r.json()["code"] == "UPLOAD_INCOMPLETE"

    etag3 = env.put(urls["3"], data[2048:]).headers["ETag"]
    bad = [{"part_number": 1, "etag": "nope"}, {"part_number": 2, "etag": "nope"}, {"part_number": 3, "etag": etag3}]
    r = env.client.post(f"/files/{f['id']}/complete", json={"parts": bad}, headers=env.h())
    assert r.status_code == 409
    assert env.client.post(f"/files/{f['id']}/complete", headers=env.h()).json()["status"] == "uploaded"


def test_watched_batches_go_first(env):
    def uploaded(name: str) -> dict:
        f = env.create_batch([spec(name, b"x" * 10)])["files"][0]
        env.upload(f, b"x" * 10)
        events.relay_once()
        env.clock.advance(5)
        return f

    bulk1, bulk2, mine = uploaded("bulk1.txt"), uploaded("bulk2.txt"), uploaded("mine.txt")
    [first] = worker.claim("scan", 1, "w")
    assert first.file_id == bulk1["id"]  # nobody watching: oldest first
    with env.client.stream("GET", f"/batches/{mine['batch_id']}/events?once=true", headers=env.h()) as r:
        "".join(r.iter_text())
    [second] = worker.claim("scan", 1, "w")
    assert second.file_id == mine["id"]  # an open stream jumps the older bulk2
