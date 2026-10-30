"""HTTP face of the mock S3: browsers PUT parts / objects here with presigned
URLs and read results with presigned GETs. Stands in for MinIO."""
from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, Response

from .. import adapters
from ..adapters.object_store import S3Error

router = APIRouter(prefix="/mock-s3")


def _err(e: S3Error) -> Response:
    body = f"<?xml version=\"1.0\"?><Error><Code>{e.code}</Code><Message>{e}</Message></Error>"
    return Response(body, status_code=e.status, media_type="application/xml")


@router.put("/{bucket}/{key:path}")
async def put_object(bucket: str, key: str, request: Request):
    store = adapters.get().store
    q = request.query_params
    try:
        if bucket != store.bucket:
            raise S3Error(404, "NoSuchBucket")
        upload_id = q.get("uploadId", "")
        part = int(q.get("partNumber", "0"))
        store.verify("PUT", key, int(q.get("X-Expires", "0")), q.get("X-Signature", ""), upload_id, part)
        data = await request.body()
        checksum = request.headers.get("x-amz-checksum-sha256")
        if upload_id:
            etag = await asyncio.to_thread(store.upload_part, key, upload_id, part, data, checksum)
        else:
            etag = await asyncio.to_thread(store.put, key, data, checksum)
    except S3Error as e:
        return _err(e)
    except ValueError:
        return _err(S3Error(400, "InvalidArgument"))
    return Response(status_code=200, headers={"ETag": f'"{etag}"'})


@router.get("/{bucket}/{key:path}")
async def get_object(bucket: str, key: str, request: Request):
    store = adapters.get().store
    q = request.query_params
    try:
        if bucket != store.bucket:
            raise S3Error(404, "NoSuchBucket")
        store.verify("GET", key, int(q.get("X-Expires", "0")), q.get("X-Signature", ""))
        if store.head(key) is None:
            raise S3Error(404, "NoSuchKey")
    except S3Error as e:
        return _err(e)
    except ValueError:
        return _err(S3Error(400, "InvalidArgument"))
    return FileResponse(store._obj(key))
