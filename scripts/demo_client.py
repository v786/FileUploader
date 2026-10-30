"""A reference client that behaves like the browser described in the doc:
3 files uploading at once, 4 parts per file in parallel, per-part SHA-256
checksums, resume from confirmed parts, and live progress over SSE.

  python scripts/demo_client.py fixtures/*            # upload files
  python scripts/demo_client.py --user alice fixtures/sample.pdf
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import sys
import uuid

import httpx

FILE_CONCURRENCY = 3
PART_CONCURRENCY = 4
PART_RETRIES = 5


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


async def request(client: httpx.AsyncClient, method: str, url: str, **kw) -> httpx.Response:
    """Honours 429/503 Retry-After by pausing only this call."""
    for _ in range(10):
        r = await client.request(method, url, **kw)
        if r.status_code in (429, 503):
            wait = float(r.headers.get("Retry-After", "1"))
            print(f"  paused — resuming in {wait:.0f}s ({r.json().get('code')})")
            await asyncio.sleep(wait)
            continue
        return r
    return r


async def put_part(client: httpx.AsyncClient, url: str, data: bytes) -> str:
    checksum = base64.b64encode(hashlib.sha256(data).digest()).decode()
    for attempt in range(PART_RETRIES):
        try:
            r = await client.put(url, content=data, headers={"x-amz-checksum-sha256": checksum})
            if r.status_code == 200:
                return r.headers["ETag"]
        except httpx.TransportError:
            pass
        await asyncio.sleep(min(8, 0.5 * 2 ** attempt))
    raise RuntimeError("part failed after retries")


async def upload_file(api: httpx.AsyncClient, s3: httpx.AsyncClient, f: dict, path: str) -> None:
    fid = f["id"]
    if f["status"] != "pending" and f["status"] != "uploading":
        print(f"  {f['name']}: {f['status']} (no upload needed)")
        return
    info = (await request(api, "POST", f"/files/{fid}/multipart")).json()
    if info["mode"] == "single":
        with open(path, "rb") as fh:
            await put_part(s3, info["url"], fh.read())
    else:
        have = {p["part_number"] for p in (await request(api, "GET", f"/files/{fid}/parts")).json()["parts"]}
        missing = [n for n in range(1, info["part_count"] + 1) if n not in have]
        sem = asyncio.Semaphore(PART_CONCURRENCY)
        size = info["part_size"]

        async def one(n: int, url: str) -> None:
            async with sem:
                with open(path, "rb") as fh:
                    fh.seek((n - 1) * size)
                    data = fh.read(size)
                etag = await put_part(s3, url, data)
                r = await request(api, "POST", f"/files/{fid}/parts/{n}", json={"etag": etag, "size_bytes": len(data)})
                print(f"  {f['name']}: part {n}/{info['part_count']} — {r.json()['progress_pct']:.0f}% uploaded")

        for i in range(0, len(missing), 100):  # sign in pages of up to 100
            page = missing[i:i + 100]
            urls = (await request(api, "POST", f"/files/{fid}/parts:sign", json={"part_numbers": page})).json()["urls"]
            await asyncio.gather(*(one(n, urls[str(n)]) for n in page))
    r = await request(api, "POST", f"/files/{fid}/complete")
    body = r.json()
    print(f"  {f['name']}: upload complete -> {body.get('status') or body.get('code')}")


async def follow(api: httpx.AsyncClient, batch_id: str, total: int) -> None:
    last = None
    while True:
        headers = {"Last-Event-ID": str(last)} if last is not None else {}
        try:
            async with api.stream("GET", f"/batches/{batch_id}/events", headers=headers, timeout=None) as r:
                event = None
                async for line in r.aiter_lines():
                    if line.startswith("id: "):
                        last = int(line[4:])
                    elif line.startswith("event: "):
                        event = line[7:]
                    elif line.startswith("data: "):
                        data = json.loads(line[6:])
                        if event == "snapshot":
                            print(f"[snapshot] {data['summary']['text']}")
                        elif event == "batch.summary":
                            print(f"[summary] {data['text']}")
                            c = data["counts"]
                            if sum(c.get(k, 0) for k in ("completed", "failed", "cancelled")) == total:
                                return
                        else:
                            print(f"[{event}] {json.dumps({k: v for k, v in data.items() if k != 'seq'})}")
        except httpx.TransportError:
            await asyncio.sleep(1)  # reconnect with Last-Event-ID


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("paths", nargs="+")
    p.add_argument("--api", default=os.environ.get("API", "http://127.0.0.1:8000"))
    p.add_argument("--user", default="demo-user")
    args = p.parse_args()

    files = []
    for i, path in enumerate(args.paths):
        files.append({"client_file_id": f"{i}-{os.path.basename(path)}", "name": os.path.basename(path),
                      "size_bytes": os.path.getsize(path), "sha256": sha256_file(path)})
    by_cid = dict(zip((f["client_file_id"] for f in files), args.paths))

    async with httpx.AsyncClient(base_url=args.api, headers={"X-User-Id": args.user}, timeout=60) as api, \
            httpx.AsyncClient(timeout=120) as s3:
        r = await request(api, "POST", "/batches", json={"files": files},
                          headers={"Idempotency-Key": str(uuid.uuid4())})
        if r.status_code != 201:
            sys.exit(f"batch create failed: {r.status_code} {r.text}")
        batch = r.json()
        print(f"batch {batch['id']}: {batch['summary']['text']}")
        watcher = asyncio.create_task(follow(api, batch["id"], len(files)))

        sem = asyncio.Semaphore(FILE_CONCURRENCY)

        async def guarded(f: dict) -> None:
            async with sem:
                try:
                    await upload_file(api, s3, f, by_cid[f["client_file_id"]])
                except Exception as exc:  # one bad file never stops the rest
                    print(f"  {f['name']}: upload error {exc}")

        await asyncio.gather(*(guarded(f) for f in batch["files"]))
        await asyncio.wait_for(watcher, timeout=600)
        final = (await api.get(f"/batches/{batch['id']}")).json()
        print("\nfinal:")
        for f in final["files"]:
            err = f" — {f['error']['code']}: {f['error']['message']}" if f["error"] else ""
            print(f"  {f['status']:>10}  {f['name']}{err}")


if __name__ == "__main__":
    asyncio.run(main())
