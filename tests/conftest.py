from __future__ import annotations

import asyncio
import base64
import hashlib
import random
import uuid
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from fileuploader import adapters, clock, db
from fileuploader.config import Settings, set_settings

TEST_LIMITS = {
    "inbound": {
        "api": {"rate": 1000, "burst": 10000},
        "part_signing": {"rate": 1000, "burst": 10000},
        "batch_creation": {"rate": 1000, "burst": 10000},
    },
    "outbound": {
        "scanner": [{"rate": 1000, "burst": 1000}],
        "extractor": [{"rate": 1000, "burst": 1000}],
        "thumbnailer": [{"rate": 1000, "burst": 1000}],
        "processor": [{"rate": 1000, "burst": 1000}],
        "processor_poll": [{"rate": 1000, "burst": 1000}],
    },
}


class Env:
    def __init__(self, tmp: Path, settings: Settings, fake: clock.FakeClock, client: TestClient) -> None:
        self.tmp = tmp
        self.settings = settings
        self.clock = fake
        self.client = client
        self.user = "user-1"

    # ---- helpers ----
    def h(self, **extra) -> dict:
        return {"X-User-Id": self.user, **extra}

    def write_limits(self, limits: dict) -> None:
        Path(self.settings.limits_path).write_text(yaml.safe_dump(limits))

    def create_batch(self, files: list[dict], key: str | None = None) -> dict:
        r = self.client.post("/batches", json={"files": files}, headers=self.h(**{"Idempotency-Key": key or str(uuid.uuid4())}))
        assert r.status_code == 201, r.text
        return r.json()

    def put(self, url: str, data: bytes, checksum: bool = True):
        headers = {}
        if checksum:
            headers["x-amz-checksum-sha256"] = base64.b64encode(hashlib.sha256(data).digest()).decode()
        return self.client.put(url, content=data, headers=headers)

    def upload(self, file: dict, data: bytes, complete: bool = True):
        fid = file["id"]
        r = self.client.post(f"/files/{fid}/multipart", headers=self.h())
        assert r.status_code == 200, r.text
        info = r.json()
        if info["mode"] == "single":
            assert self.put(info["url"], data).status_code == 200
        else:
            size = info["part_size"]
            n = info["part_count"]
            urls = self.client.post(f"/files/{fid}/parts:sign", json={"part_numbers": list(range(1, n + 1))},
                                    headers=self.h()).json()["urls"]
            for i in range(1, n + 1):
                chunk = data[(i - 1) * size: i * size]
                pr = self.put(urls[str(i)], chunk)
                assert pr.status_code == 200, pr.text
                rr = self.client.post(f"/files/{fid}/parts/{i}", json={"etag": pr.headers["ETag"], "size_bytes": len(chunk)},
                                      headers=self.h())
                assert rr.status_code == 200, rr.text
        if complete:
            return self.client.post(f"/files/{fid}/complete", headers=self.h())
        return None

    def run(self) -> None:
        from fileuploader import worker
        asyncio.run(worker.run_until_idle())

    def drain(self, rounds: int = 60, step: float = 400.0) -> None:
        """Run the pipeline, moving the clock past backoffs / cool-downs, until nothing is pending."""
        for _ in range(rounds):
            self.run()
            with db.read() as conn:
                left = conn.execute("SELECT COUNT(*) FROM jobs WHERE status IN "
                                    "('queued','waiting_vendor','running','polling')").fetchone()[0]
                outbox = conn.execute("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL").fetchone()[0]
            if left == 0 and outbox == 0:
                return
            self.clock.advance(step)
        raise AssertionError("pipeline did not drain")

    def file(self, fid: str) -> dict:
        return self.client.get(f"/files/{fid}", headers=self.h()).json()

    def faults(self, vendor: str, profile: dict) -> None:
        assert self.client.post(f"/__faults/{vendor}", json=profile).status_code == 200


def typed(name: str, data: bytes = b"") -> bytes:
    """Bytes whose magic matches the extension: type is checked by content, not by name."""
    from fileuploader.filetypes import MAGIC, ext
    return MAGIC.get(ext(name), (b"",))[0] + data


def spec(name: str, data: bytes, cid: str | None = None) -> dict:
    return {"client_file_id": cid or name, "name": name, "size_bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest()}


@pytest.fixture
def env(tmp_path: Path, monkeypatch):
    for v in ("SCANNER", "EXTRACTOR", "THUMBNAILER", "PROCESSOR"):
        monkeypatch.delenv(f"MOCK_{v}_FAULTS", raising=False)
    random.seed(1234)
    limits = tmp_path / "limits.yaml"
    limits.write_text(yaml.safe_dump(TEST_LIMITS))
    s = Settings(db_path=str(tmp_path / "app.db"), storage_dir=str(tmp_path / "objects"),
                 public_base_url="http://testserver", limits_path=str(limits))
    s.chunk_size = 1024  # small parts so multipart tests use tiny payloads
    s.min_part_size, s.max_part_size = 512, 8192
    s.max_file_size = 64 * 1024
    set_settings(s)
    fake = clock.FakeClock()
    clock.set_clock(fake)
    adapters.reset()
    db.init_db()
    from fileuploader.api import create_app
    with TestClient(create_app(run_workers=False)) as client:
        # The mock processor delivers its webhooks to our endpoint through the test client.
        adapters.get().processor.sender = lambda body, headers: client.post(
            "/webhooks/processor", content=body, headers=headers).status_code
        yield Env(tmp_path, s, fake, client)
    clock.set_clock(clock.Clock())
    adapters.reset()
