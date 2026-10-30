# Multi-File Upload & Processing Service — Python + SQLite backend

Backend for the design in `Multi-File Upload & Processing Service — Implementation Doc.md`,
built with **Python (FastAPI) and SQLite**. S3 and every third-party vendor are mocked, so it
runs fully offline with no containers.

## Quick start

```bash
make install        # venv + deps
make test           # 70 tests: unit, API, pipeline, processing API, resilience, idempotency, chaos
make run            # API + workers on :8000 (single process)
make demo           # in another terminal: uploads fixtures/* and streams progress
```

Run API and workers as separate processes (they share state only through SQLite):

```bash
make api            # terminal 1
make worker         # terminal 2 (start several to scale out)
```

Env: `DB_PATH`, `STORAGE_DIR`, `PUBLIC_BASE_URL` (host used in presigned URLs and for the mock
processor's webhooks — must match the port you run on), `SIGNING_SECRET`, `PROCESSOR_WEBHOOK_SECRET`,
`PROCESS_PARALLEL_WITH_SCAN=1` (trusted processing vendor only; off by default), `VENDOR_MODE=mock`,
`LIMITS_PATH`, `MOCK_<SCANNER|EXTRACTOR|THUMBNAILER|PROCESSOR>_FAULTS` (JSON fault profile).

The schema changed with the processing API. A database from an older version is refused at
startup; run `make clean` to start fresh.

## How the doc's stack maps here

| Doc | Here |
| --- | --- |
| Fastify API | FastAPI (`fileuploader/api/`) |
| Postgres | SQLite, WAL mode. `BEGIN IMMEDIATE` takes the write lock, standing in for `SELECT … FOR UPDATE` |
| Redis (token buckets, breaker state, streams) | SQLite tables `rate_buckets`, `breakers`, `vendor_call_log`, `events`, shared by every process |
| BullMQ queues | The `jobs` table *is* the queue: job id `<stage>:<file_id>`, leases (`locked_until`, `lease_id`) for stalled-job rescue, per-stage worker pools |
| MinIO / S3 | `LocalObjectStore`: filesystem-backed, with multipart, HMAC-presigned URLs, ETags, `x-amz-checksum-sha256` and ranged reads. Browsers PUT straight to `/mock-s3/...` |
| mock-scanner / mock-extract / mock-thumbs | In-process mocks behind the `Scanner` / `Extractor` / `Thumbnailer` ports, with the doc's fault profiles |
| mock-processor | `MockProcessor` behind the `Processor` port: async job API, progress curve, HMAC-signed webhooks POSTed to `/webhooks/processor`, webhook faults. Its jobs live in SQLite so API and worker processes share them |
| `config/limits.yaml` | Same file; hot-reloaded on change |

## Layout

```
fileuploader/
  config.py        settings + limits.yaml loader
  db.py            schema (batches, files, file_parts, jobs, vendor_calls, outbox, events, dlq, ...)
  state.py         file state machine, version-guarded transitions
  errors.py        error contract, stable codes, classifier (transient / dependency / permanent / unknown)
  ratelimit.py     atomic token buckets (inbound per user, outbound per vendor, separate poll bucket)
  breaker.py       shared circuit breaker + retry budget
  vendor_call.py   call_vendor(): cache → breaker → budget → token → timeout → call → classify → record → cache
  events.py        transactional outbox, relay, per-batch event stream with seq
  worker.py        claim / process / retry with full-jitter backoff / park / DLQ / fan-in;
                   process stage: submit, poll, apply_vendor_update (shared with the webhook)
  estimates.py     rolling p50 per stage → processing_estimate_ms and processing_pct
  filetypes.py     file families, size buckets, magic-byte sniffing
  sweeper.py       abort idle multipart uploads (24 h), expire events (1 h) and idempotency keys (24 h)
  ports.py         ObjectStore, Scanner, Extractor, Thumbnailer, Processor
  adapters/        mock S3 + mock vendors with fault injection
  api/             routes, SSE, processor webhook, admin + faults, mock-S3 HTTP endpoints
scripts/demo_client.py   reference client (3 files × 4 parts, checksums, resume, SSE with Last-Event-ID)
scripts/make_fixtures.py sample files, EICAR, zero-byte, 20 MB multipart, optional 6 GB sparse
```

## API

Mock auth: every request carries `X-User-Id` (or `?user_id=` for EventSource). Every error is
`{code, message, retryable, retry_after_ms, request_id}`. Every authenticated response carries
`RateLimit-Limit/Remaining/Reset`.

| Method & path | Purpose |
| --- | --- |
| `POST /batches` (`Idempotency-Key` required) | Create a batch of ≤ 50 files `{client_file_id, name, size_bytes, sha256?, fingerprint?, head_hex?, part_size?}` (send `sha256` or `fingerprint`). One call to start: each file's upload is created and `upload.start` carries the signed URLs of its first 8 parts. Each file gets `processing_estimate_ms`. Bad type (extension or magic bytes), size, duplicate name or quota fails that file only; a `sha256` duplicate completes instantly; a `fingerprint` hit is a `dedupe_candidate`. `503 BUSY` above the queue high-water mark |
| `GET /batches/:id` | Snapshot: files (with `upload_pct` / `processing_pct`), summary ("42 of 50 done, 2 need attention"), `seq` |
| `GET /batches/:id/events` | SSE (`file.stage` with `processing_pct`, `file.retrying`, `file.waiting`, `file.completed`, `file.failed`, `file.cancelled`, `batch.summary`); replay from `Last-Event-ID` or a `snapshot`. An open stream puts the batch in the interactive lane |
| `POST /files/:id/multipart` | Create the upload once (multipart, or a single presigned PUT for files < 8 MB). Batch create already did this; still useful after a retry |
| `POST /files/:id/parts:sign` | `{part_numbers}` (≤ 100) → URLs valid for 15 min |
| `POST /files/:id/parts/:n` | Optional progress report `{etag, size_bytes}`, verified against storage |
| `GET /files/:id/parts` | Parts in storage (S3 ListParts) + `missing` list, for resume |
| `POST /files/:id/complete` | `{sha256?, parts?}`. The full hash may arrive here. Confirms a fingerprint dedupe (or `409 UPLOAD_REQUIRED`), else completes from `parts` or ListParts, verifies size, SHA-256 and magic bytes, moves to `uploaded`, enqueues scan (one tx) |
| `POST /webhooks/processor` | Processing-vendor webhook: HMAC (`X-Processor-Signature`), deduped by `event_id`, stale statuses ignored |
| `POST /files/:id/retry` | Re-run only the failed stage (or reset an upload failure); refused for malware |
| `POST /files/:id/cancel` | Cancel any non-final file (aborts its multipart upload) |
| `GET /files?unfinished=true` | Unfinished uploads to offer "Resume" |
| `GET /files/:id`, `GET /files/:id/result` | File view; result with presigned download + thumbnail URLs |
| `GET /admin/metrics`, `/admin/breakers`, `/admin/dlq` | Queue depth/age, outcomes by error code, breaker states, DLQ |
| `POST /admin/dlq/:stage/replay` | Re-drive dead jobs |
| `GET/POST/DELETE /__faults/:vendor` | Fault profile: `latency_ms`, `latency_jitter_ms`, `error_rate`, `error_status`, `rate_limit_rps`, `down`, `hang`, `fail_file_ids`; for `processor` also `duration_ms`, `queue_ms`, `progress_mode` (`percent`/`states`/`none`), `progress_curve`, `webhook_delay_ms`, `webhook_duplicate_rate`, `webhook_out_of_order`, `webhook_drop_rate` |

Try a vendor outage against a running server:

```bash
curl -X POST localhost:8000/__faults/scanner -H 'content-type: application/json' -d '{"down": true}'
make demo                                   # files show file.waiting once the breaker opens
curl -X DELETE localhost:8000/__faults/scanner   # they resume on their own after the cool-down
```

## Behaviour notes

- **Pipeline**: `scan` → (`extract` ∥ `thumbnail` ∥ `process`) → `finalize`. Concurrency per stage is 8/4/6/8/16 and timeouts are 30/60/45/10/5 s; a job's own timeout is 2× its call timeout. Each user can have at most 10 active jobs. Jobs of batches with an open SSE stream are claimed first.
- **Processing API (`process`)**:
  - The worker submits with `Idempotency-Key = sha256(file_id + stage + vendor)` and a freshly signed GET URL (1 h; an expired URL is a transient error, so the retry signs a new one).
  - The vendor's job id is stored on the job row before the worker acks. A replay then resumes polling instead of submitting again.
  - The job then waits in `polling` without holding a worker slot. It polls at 10 s, backing off to 60 s, through `call_vendor` with its own `processor_poll` bucket.
  - The webhook usually arrives first. Webhook and poller run the same transaction: store `results/<file_id>/process.json`, mark the job done, fan in.
  - Overall deadline 15 min per file, then backoff and retry. A user retry or DLQ replay of a failed process job submits a new vendor job.
- **Fan-in**: the transaction that finishes extract, thumbnail or process counts the done jobs under the write lock. Only the one that sees all three enqueues `finalize:<file_id>`.
- **Combined progress**: the server half of the bar is `processing_pct`, the post-upload stages weighted by each stage's rolling p50 (by file type and size bucket).
  - Within `process` it uses the vendor's percent if it reports one. Otherwise it estimates elapsed ÷ p50, capped at 90%. While the file is queued at the vendor, the bar holds still.
  - It never moves backwards and reaches 100 only on completion.
  - `processing_estimate_ms` (p50 of upload → completed, or the critical path through stage priors) lets the client fix its weights.
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
- **Not in this backend**: the doc's client-side UX work (instant previews, smallest-first queue, AIMD parallelism, upload tray, `FileSystemFileHandle` resume, notifications, ETA smoothing). It also leaves out infra-level changes from "Scaling to 100k users": Transfer Acceleration, warm pools, partitioning, PgBouncer, split Redis, multiple relays and KEDA. The backend provides what those need: `upload.start`, `fingerprint` on file views for re-pick matching, client-chosen `part_size`, `fix` on every error, and ListParts-based resume.
- **Live adapters**: `VENDOR_MODE=live` raises until real vendors are chosen (an open question in the doc). They plug in behind `ports.py` in `adapters/__init__.py`.
