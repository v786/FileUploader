# Multi-File Upload & Processing Service — Python + SQLite backend

Backend for the design in `Multi-File Upload & Processing Service — Implementation Doc.md`,
built with **Python (FastAPI) and SQLite**. S3 and every third-party vendor are mocked, so it
runs fully offline with no containers.

## Quick start

```bash
make install        # venv + deps
make test           # 47 tests: unit, API, pipeline, resilience, idempotency, chaos
make run            # API + workers on :8000 (single process)
make demo           # in another terminal: uploads fixtures/* and streams progress
```

Run API and workers as separate processes (they share state only through SQLite):

```bash
make api            # terminal 1
make worker         # terminal 2 (start several to scale out)
```

Env: `DB_PATH`, `STORAGE_DIR`, `PUBLIC_BASE_URL` (host used in presigned URLs — must match the
port you run on), `SIGNING_SECRET`, `VENDOR_MODE=mock`, `LIMITS_PATH`,
`MOCK_<SCANNER|EXTRACTOR|THUMBNAILER>_FAULTS` (JSON fault profile).

## How the doc's stack maps here

| Doc | Here |
| --- | --- |
| Fastify API | FastAPI (`fileuploader/api/`) |
| Postgres | SQLite, WAL mode. `BEGIN IMMEDIATE` takes the write lock, standing in for `SELECT … FOR UPDATE` |
| Redis (token buckets, breaker state, streams) | SQLite tables `rate_buckets`, `breakers`, `vendor_call_log`, `events`, shared by every process |
| BullMQ queues | The `jobs` table *is* the queue: job id `<stage>:<file_id>`, leases (`locked_until`, `lease_id`) for stalled-job rescue, per-stage worker pools |
| MinIO / S3 | `LocalObjectStore`: filesystem-backed, with multipart, HMAC-presigned URLs, ETags, `x-amz-checksum-sha256` and ranged reads. Browsers PUT straight to `/mock-s3/...` |
| mock-scanner / mock-extract / mock-thumbs | In-process mocks behind the `Scanner` / `Extractor` / `Thumbnailer` ports, with the doc's fault profiles |
| `config/limits.yaml` | Same file; hot-reloaded on change |

## Layout

```
fileuploader/
  config.py        settings + limits.yaml loader
  db.py            schema (batches, files, file_parts, jobs, vendor_calls, outbox, events, dlq, ...)
  state.py         file state machine, version-guarded transitions
  errors.py        error contract, stable codes, classifier (transient / dependency / permanent / unknown)
  ratelimit.py     atomic token buckets (inbound per user, outbound per vendor)
  breaker.py       shared circuit breaker + retry budget
  vendor_call.py   call_vendor(): cache → breaker → budget → token → timeout → call → classify → record → cache
  events.py        transactional outbox, relay, per-batch event stream with seq
  worker.py        claim / process / retry with full-jitter backoff / park / DLQ / fan-in
  sweeper.py       abort idle multipart uploads (24 h), expire events (1 h) and idempotency keys (24 h)
  ports.py         ObjectStore, Scanner, Extractor, Thumbnailer
  adapters/        mock S3 + mock vendors with fault injection
  api/             routes, SSE, admin + faults, mock-S3 HTTP endpoints
scripts/demo_client.py   reference client (3 files × 4 parts, checksums, resume, SSE with Last-Event-ID)
scripts/make_fixtures.py sample files, EICAR, zero-byte, 20 MB multipart, optional 6 GB sparse
```

## API

Mock auth: every request carries `X-User-Id` (or `?user_id=` for EventSource). Every error is
`{code, message, retryable, retry_after_ms, request_id}`. Every authenticated response carries
`RateLimit-Limit/Remaining/Reset`.

| Method & path | Purpose |
| --- | --- |
| `POST /batches` (`Idempotency-Key` required) | Create a batch of ≤ 50 files `{client_file_id, name, size_bytes, sha256}`. Invalid files are marked `failed` individually; content duplicates complete instantly. `503 BUSY` above the queue high-water mark |
| `GET /batches/:id` | Snapshot: files, summary ("42 of 50 done, 2 need attention"), `seq` |
| `GET /batches/:id/events` | SSE (`file.stage`, `file.retrying`, `file.waiting`, `file.completed`, `file.failed`, `file.cancelled`, `batch.summary`); replay from `Last-Event-ID` or a `snapshot` |
| `POST /files/:id/multipart` | Create the multipart upload once (or a single presigned PUT for files < 8 MB) |
| `POST /files/:id/parts:sign` | `{part_numbers}` (≤ 100) → URLs valid for 15 min |
| `POST /files/:id/parts/:n` | Confirm a part `{etag, size_bytes}`, verified against storage |
| `GET /files/:id/parts` | Confirmed parts + `missing` list, for resume |
| `POST /files/:id/complete` | Complete, verify size + SHA-256, move to `uploaded`, enqueue scan (one tx) |
| `POST /files/:id/retry` | Re-run only the failed stage (or reset an upload failure); refused for malware |
| `POST /files/:id/cancel` | Cancel any non-final file (aborts its multipart upload) |
| `GET /files?unfinished=true` | Unfinished uploads to offer "Resume" |
| `GET /files/:id`, `GET /files/:id/result` | File view; result with presigned download + thumbnail URLs |
| `GET /admin/metrics`, `/admin/breakers`, `/admin/dlq` | Queue depth/age, outcomes by error code, breaker states, DLQ |
| `POST /admin/dlq/:stage/replay` | Re-drive dead jobs |
| `GET/POST/DELETE /__faults/:vendor` | Fault profile: `latency_ms`, `latency_jitter_ms`, `error_rate`, `error_status`, `rate_limit_rps`, `down`, `hang`, `fail_file_ids` |

Try a vendor outage against a running server:

```bash
curl -X POST localhost:8000/__faults/scanner -H 'content-type: application/json' -d '{"down": true}'
make demo                                   # files show file.waiting once the breaker opens
curl -X DELETE localhost:8000/__faults/scanner   # they resume on their own after the cool-down
```

## Behaviour notes

- **Pipeline**: `scan` → (`extract` ∥ `thumbnail`) → `finalize`. Concurrency per stage is 8/4/6/16 and timeouts are 30/60/45/5 s; a job's own timeout is 2× its call timeout. Each user can have at most 10 active jobs.
- **Fan-in**: the transaction that finishes extract or thumbnail counts the remaining jobs under the write lock. Only the one that sees zero enqueues `finalize:<file_id>`.
- **Errors**:
  - Transient errors retry with full-jitter backoff (base 2 s, cap 5 min, 6 attempts) and honour a vendor's `Retry-After`.
  - When the breaker is open, the job is parked as `waiting_vendor`, which costs no attempt.
  - Permanent errors set the file to `failed` with a plain-language message.
  - Unknown errors get 3 attempts and then go to the DLQ.
  - When a file fails, sibling stages still queued are `skipped`, and a user retry re-runs them.
- **Idempotency**:
  - Request keys: same body replays the stored response, a different body gets `422`, a request still in flight gets `409`.
  - Unique constraints on batch and file rows, deterministic S3 keys and job ids.
  - Vendor results are cached in `vendor_calls`.
  - Each event's `seq` is assigned at write time, so re-publishing the outbox can't create duplicate events.
- **Deviation from the doc**: the doc's multipart checksum is S3's composite checksum. The mock instead streams the assembled object with ranged reads and compares its full SHA-256 to the one the client declared.
- **Live adapters**: `VENDOR_MODE=live` raises until real vendors are chosen (an open question in the doc). They plug in behind `ports.py` in `adapters/__init__.py`.
