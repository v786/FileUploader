# Multi-File Upload & Processing Service — Implementation Doc

Oct 30, 2026 · @Muskan

## Overview

We will build a web app where a user drops up to 50 files at once, watches each file's progress in real time, and gets every file stored in S3 and processed by third-party vendors (virus scan, metadata extraction, thumbnails). Uploads go straight from the browser to S3 in resumable chunks. Processing runs on a queue with bounded concurrency, idempotent steps, per-vendor rate limits and circuit breakers. One bad file, or one failing vendor, never fails the batch.

**Must-haves**

1. Per-file progress the user can see: upload percent, then each processing stage, then a final status.
2. Graceful error handling: retry what is transient, isolate what is permanent, tell the user plainly, and let them retry one file without re-sending the rest.

**Engineering requirements**

| Requirement | Target |
| --- | --- |
| Concurrency | 4 chunk uploads per file, 3 files at once per browser tab; worker pool sized per stage |
| Idempotency | Any request or job may run twice with no duplicate file, charge or side effect |
| Rate limiting | Per-user API limit, per-vendor token bucket shared across all workers |
| Circuit breaking | One breaker per vendor; open breaker parks jobs instead of burning retries |
| File size | Up to 5 GB per file, 8 MB chunks |
| Local dev | Runs fully offline: S3 and all vendors are mocked |

**Non-goals:** sharing or permissions on uploaded files, a file browser, billing, and multi-region storage.

## Architecture

Six runtime pieces: a React client, a stateless Fastify API, BullMQ workers, Postgres for state, Redis for queues and coordination, and S3 for bytes. File bytes never touch the API, and workers reach vendors only through one wrapper that applies the breaker, rate limit and timeout.

&#91;embedded content: system architecture · 7 components\]

The API and workers scale independently and hold no state of their own, so any pod can die mid-request and another picks up from Postgres.

## Data model

Postgres is the source of truth for every file's state; Redis holds only queues, rate-limit buckets, breaker state and progress fan-out. Each file moves through one state machine, and every transition is a guarded `UPDATE ... WHERE status = <expected>` so two workers can never both advance it.

| Table | Key columns | Notes |
| --- | --- | --- |
| `batches` | `id`, `user_id`, `idempotency_key` (unique per user), `created_at` | One per drop of files. Retrying the create call returns the same batch. |
| `files` | `id`, `batch_id`, `client_file_id`, `name`, `size_bytes`, `sha256`, `status`, `stage`, `progress_pct`, `attempts`, `error_code`, `error_message`, `s3_key`, `s3_upload_id`, `version` | Unique on (`batch_id`, `client_file_id`). `version` is an optimistic-lock counter. |
| `file_parts` | `file_id`, `part_number`, `etag`, `size_bytes`, `checksum` | Primary key (`file_id`, `part_number`). Lets an upload resume after a refresh. |
| `jobs` | `id`, `file_id`, `stage`, `status`, `attempt`, `next_run_at`, `last_error` | One row per stage per file. Unique on (`file_id`, `stage`). |
| `vendor_calls` | `idempotency_key` (PK), `vendor`, `request_hash`, `response_json`, `status` | Caches vendor results so a replayed job never calls a vendor twice. |
| `outbox` | `id`, `topic`, `payload`, `published_at` | Transactional outbox for progress events and queue enqueues. |

**File states:** `pending` → `uploading` → `uploaded` → `processing` → `completed`. From `uploading` or `processing` a file can go to `failed_retryable` (back to its stage after backoff), `waiting_vendor` (breaker open; resumes when it closes) or `failed` (permanent, user action needed). `cancelled` is reachable from any non-final state. A diagram of these transitions is in the Error handling section.

## Upload flow

Bytes never pass through our API: the browser uploads 8 MB chunks straight to S3 with presigned multipart URLs, and the API only signs, records and completes. This keeps API pods small, makes each chunk independently retryable, and lets an upload resume after a refresh or network drop.

1. **Validate on the client.** Check type allow-list, size limit (5 GB) and count (50) before any request. Compute SHA-256 in a Web Worker so the UI thread stays responsive.
2. **Create the batch.** `POST /batches` with header `Idempotency-Key: <uuid>` and the file list (`client_file_id`, name, size, sha256). The API inserts batch and file rows in one transaction and returns file ids. A retried call returns the stored response.
3. **Dedupe by content.** If a completed file with the same `sha256` already exists for this user, the API marks the new row `completed` and links the existing object. No bytes move.
4. **Start multipart.** `POST /files/:id/multipart` calls S3 `CreateMultipartUpload` once and stores `s3_upload_id`. A second call returns the stored id.
5. **Sign parts in pages.** `POST /files/:id/parts:sign` with part numbers 1–N (max 100 per call) returns URLs valid for 15 minutes.
6. **Upload chunks.** The client `PUT`s up to 4 parts per file in parallel, with an `x-amz-checksum-sha256` per part. Each finished part is reported with `POST /files/:id/parts/:n` (`etag`, size). Progress = confirmed bytes / total bytes.
7. **Resume.** On reload or reconnect, `GET /files/:id/parts` returns confirmed parts; the client uploads only the missing ones. Expired URLs are re-signed transparently.
8. **Complete.** `POST /files/:id/complete` calls `CompleteMultipartUpload` with the stored part list, verifies the object size and checksum, moves the file to `uploaded`, and writes an outbox row that enqueues processing — all in one transaction.
9. **Clean up.** A sweeper aborts multipart uploads idle for 24 hours, and an S3 lifecycle rule does the same as a backstop.

Files under 8 MB skip multipart and use a single presigned `PUT`, but report progress the same way.

## Per-file progress

Each file shows its own progress bar, stage and status, driven by two sources: the browser's own byte counts during upload, and server-sent events (SSE) once the server owns the file. SSE is enough because progress only flows server → client; it reconnects on its own and passes through proxies more easily than WebSockets.

**Upload phase (client-side).** `XMLHttpRequest.upload.onprogress` per chunk gives byte-level progress. File percent = (confirmed parts' bytes + in-flight bytes) / size, throttled to one UI update per 100 ms per file.

**Processing phase (server-side).** `GET /batches/:id/events` opens an SSE stream. Workers publish events to a Redis channel `batch:<id>`; the API relays them to connected clients.

| Event | Payload | UI |
| --- | --- | --- |
| `file.stage` | `file_id`, `stage` (scan, extract, thumbnail), `progress_pct` | Bar advances; stage label changes |
| `file.retrying` | `file_id`, `attempt`, `next_retry_at`, `reason` | Amber "Retrying in 8s" |
| `file.waiting` | `file_id`, `vendor` | Grey "Waiting for scanner — will resume automatically" |
| `file.completed` | `file_id`, `result_url` | Green check, link to result |
| `file.failed` | `file_id`, `error_code`, `message`, `retryable_by_user` | Red, plain-language reason, Retry button |
| `batch.summary` | counts per status | Header: "42 of 50 done, 2 need attention" |

**No lost events.** Every event carries a monotonic `seq` per batch, also stored with the file row. On reconnect the client sends `Last-Event-ID`; the API replays from a Redis stream (kept 1 hour) or, if that is gone, sends a snapshot from `GET /batches/:id`. The UI applies an event only if its `seq` is newer than what it has, so replays and out-of-order delivery are harmless.

**Client state.** A per-file reducer (`idle → hashing → uploading → processing → done | error`) in a store keyed by `file_id`. The list is virtualised so 50 rows animating stay smooth. Per-file actions: pause, resume, cancel, retry.

## External processing API: sequencing and combined progress

**Recommendation: send each file to the processing API after it is complete in S3 and has passed the malware scan, not in parallel with the browser upload.** For most files, sending in parallel saves only seconds, and it costs durability, retries and security. The user still sees one progress bar per file, covering upload and processing.

### After S3 or in parallel?

The slow step is the user's upload, not the transfer to the vendor. A 100 MB file takes about 40 s to upload on a 20 Mbps connection. The vendor can then pull it from S3 in about 1–2 s. Most processing APIs also need the whole file before they start, so sending in parallel saves only that 1–2 s transfer.

| Option | How it works | Time saved | If the processing API fails | Cost to the user | Risk | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| A. Browser sends in parallel | Browser streams each file to both S3 and the processing API | \~1–2 s per file | Needs the user's browser to re-send; impossible after the tab closes | Uploads every byte twice, so it can take up to twice as long on slow connections | Unscanned files reach a third party; vendor credentials or tokens in the browser; two copies can disagree | Avoid |
| B. After S3 (recommended) | Worker sends a presigned GET URL (valid 1 h) after `scan` passes; the vendor pulls from S3 | None | Retried by the server from the stored object; the user can close the tab | None | Only scanned files leave; one source of truth | Default |
| C. Pipelined, server-side | As each 8 MB part lands in S3, a worker forwards it to the vendor's chunked or resumable upload | Lets processing begin before the upload ends, but only if the vendor processes streams | Resumable by part number, but more complex | None | Unscanned bytes reach the vendor unless scanning is also streamed | Only for files over \~1 GB when the vendor processes streams (video, per-page OCR) |

If the processing vendor is trusted and scans files itself, `process` can run in parallel with `scan`, since both read from S3 once the upload completes. That saves the scan time (\~5–30 s) without the drawbacks of option A. Make it a config flag, off by default.

### How the processing API fits the pipeline

- **New stage `process`.** It runs after `scan`, alongside `extract` and `thumbnail`, and `finalize` waits for all three. Concurrency 8, submit timeout 10 s, overall deadline 15 min per file.
- **Async job model.** `POST /jobs` with `Idempotency-Key = sha256(file_id + "process")` and `source_url` = presigned GET. The vendor's `job_id` is stored on the `jobs` row before the worker acks. A replay finds the stored id and resumes polling instead of submitting again.
- **Completion.** The primary signal is a webhook to `POST /webhooks/processor`. Verify its HMAC signature, dedupe by vendor `event_id`, and ignore any status older than the stored one. As a fallback, poll every 10 s, backing off to 60 s, so a lost webhook never strands a file. Polls go through `callVendor`, so they count against the rate limit and the breaker.
- **Expired URLs.** If a backlog delays submission past the URL's lifetime, the retry signs a fresh URL. Signing is cheap and never cached.
- **Results.** Store the vendor's result in S3 under `results/<file_id>/process.json` and its pointer on the file row. Never store only the vendor's URL, which may expire.

### One combined progress bar per file

Each file shows one bar with two segments, upload then processing, plus a stage label and ETA. The client builds the bar: it measures the upload itself and gets processing progress from the server's SSE events.

**Formula.** `overall = w_upload × upload_pct + w_process × processing_pct`, where `w_upload = est_upload_s / (est_upload_s + est_processing_s)` and `w_process = 1 − w_upload`.

- `est_upload_s` = file size ÷ the client's measured throughput, a moving average over the first 2 parts.
- `est_processing_s` comes from the server in the batch-create response (`processing_estimate_ms` per file). It is the rolling p50 duration for that file type and size bucket, across all post-upload stages.
- The weights are fixed when the upload starts, so the bar never jumps when estimates change.

**Processing percent.** The server sends `processing_pct` (0–100) in `file.stage` events, combining the post-upload stages weighted by each stage's p50 duration. Within the `process` stage:

- If the vendor reports a percent, use it.
- If the vendor reports only states (queued, running, done), estimate from elapsed time ÷ p50, capped at 90% until it finishes.
- If the vendor reports a queue position, show "Queued at vendor" with the bar held still.

**Example.** A 100 MB PDF on a 20 Mbps connection gives `est_upload_s` = 40 s, and p50 processing is 20 s. The weights are 0.67 and 0.33, so the bar stands at 67% the moment the upload finishes, then fills as processing reports.

| Moment | Label | Bar |
| --- | --- | --- |
| Upload half done | Uploading · 50% · \~20 s left | 33% |
| Upload complete, scan running | Scanning | 67% → 70% |
| Vendor at 60% | Processing · \~8 s left | 87% |
| Vendor retrying after `503` | Retrying (2 of 6) | held at 87%, amber |
| Breaker open | Waiting for processor — resumes automatically | held, grey |
| Done | Done | 100%, green |

**Rules that keep it honest.**

- The bar never moves backwards. A retry changes the label and colour, not the fill.
- It reaches 100% only on `file.completed`.
- On failure it freezes and turns red, with the reason and a Retry button.
- After a page refresh, the `GET /batches/:id` snapshot sets the upload segment to 100% for uploaded files and restores `processing_pct`.

**Mocking.** Add `mock-processor` with the same async job API. It reports percent progress on a configurable curve and delivers webhooks. Its fault profile adds `webhook_delay_ms`, `webhook_duplicate_rate`, `webhook_out_of_order` and `webhook_drop_rate`, the last of which proves the polling fallback. It also supports `progress_mode: percent|states|none`, so all three progress strategies are tested.

## User experience efficiency

The user's experience comes down to three things: how soon the first progress appears, how soon each file is done, and whether they ever have to wait, watch or redo work. The changes below target those three, ordered by user-visible gain per unit of effort. Gains are estimates to confirm with the metrics at the end of this section.

### Start instantly

| Change | How | User-visible gain | Effort |
| --- | --- | --- | --- |
| Hash while uploading, not before | Compute SHA-256 incrementally as each chunk is read for upload; send the full hash on `complete`. Pre-upload dedupe uses a quick fingerprint (size + hash of first and last 1 MB) and is confirmed by the full hash at `complete`. This replaces steps 1 and 3 of the Upload flow | Upload of a 2 GB file starts in under 1 s instead of after \~5–10 s of hashing | Low |
| One call to start | `POST /batches` also creates each multipart upload and returns signed URLs for the first 8 parts of every file | Two fewer round trips per file; first bytes move \~300–600 ms sooner on mobile | Low |
| Fail fast, before any bytes | Check type by magic bytes (not extension), size, user quota and duplicate names in the browser, and again in the create call | Bad files are rejected in under 1 s, not after a 5-minute upload | Low |
| Instant local preview | Show a client-side thumbnail (`URL.createObjectURL`, first PDF page via pdf.js) in the row as soon as a file is dropped | The list looks alive immediately; users can spot a wrong file before it uploads | Low |

### Finish sooner

| Change | How | User-visible gain | Effort |
| --- | --- | --- | --- |
| Smallest files first | Client queue orders by size ascending; the user can drag a file to the top or press "Upload next" | First completions in seconds, so the batch feels fast even when the total time is unchanged | Low |
| Adaptive chunks and parallelism | Chunk size 5–64 MB, chosen from file size (never more than 10,000 parts) and measured throughput. Parallel parts grow while throughput rises and halve on errors or timeouts (AIMD), with 2–12 connections in total | Fast connections run 2–3× faster; slow mobile connections stop thrashing | Medium |
| Upload close to the user | S3 Transfer Acceleration, or a bucket per region chosen at batch create | 20–50% faster uploads for users far from the region | Low (cost per GB) |
| Interactive before bulk | Two priority lanes per stage: files a user is watching (SSE stream open) go ahead of API or bulk imports | Watched files are not stuck behind someone's 10,000-file import | Medium |
| Instant result for duplicates | A content-hash hit links the existing object and copies its processing results | Re-uploaded files finish instantly, with no vendor cost | Low (already designed) |
| Warm workers | Keep a minimum pool per stage and pre-open vendor HTTP connections | Removes cold-start delays of a few seconds after quiet periods | Low |

### Never wait, watch or redo

| Change | How | User-visible gain | Effort |
| --- | --- | --- | --- |
| Keep working while uploading | A global upload tray that persists across in-app navigation; uploads live in an app-level store, not the page | The user isn't held on one screen for minutes | Medium |
| Resume without re-selecting | Store `FileSystemFileHandle`s in IndexedDB (Chromium) so a reopened tab resumes automatically. Elsewhere, ask the user to re-pick the file, matching it by fingerprint | A closed tab or crashed browser costs one click, not a full re-upload | Medium |
| Survive network changes | Listen for `online`/`offline` and connection-type changes; pause in-flight parts and restart them immediately on reconnect instead of waiting for timeouts | Switching Wi-Fi to cellular costs seconds, not a 30 s timeout per part | Low |
| Notify when done | Browser notification, plus an email when a batch takes over 5 min or the tab is closed. The notification links back to the batch | Users can walk away from long batches | Low |
| Fix in place | A failed file offers the exact fix ("Convert to PDF", "Split into parts under 5 GB", "Replace file") and retries only that file, keeping its completed stages | No restarting the batch; one click per problem | Medium |
| Warn before losing work | `beforeunload` prompt only while unconfirmed bytes exist, never once everything is in S3 | No accidental loss, no needless nagging | Low |

### Make progress readable

- **Smooth, honest ETAs.** Show ETA only after 3 s of data; smooth with a moving average and round to friendly steps ("about 2 min").
- **Batch summary first.** One header bar for the batch, with an "N need attention" filter that jumps to failed files, so 50 rows never need scanning.
- **Collapse finished rows** into a "42 done" group; keep active and failed files on top.
- **Accessible.** Announce stage changes and failures through a polite `aria-live` region, at most once per file per stage. Every action works from the keyboard.

### UX metrics to track

| Metric | Target |
| --- | --- |
| Time from drop to first progress | p95 < 1 s |
| Time to first completed file in a batch | p50 < 10 s |
| Time to done per file, by size bucket | p95 within 1.5× upload time + p50 processing time |
| Batches needing any user action | < 2% |
| Interrupted uploads that resume without re-upload | > 95% |
| Upload throughput vs measured link speed | > 80% |

These come from client telemetry (RUM) tagged with batch and file ids, and sit on the same dashboard as the server metrics in Testing and observability.

## Concurrency and processing pipeline

Processing is four stages, each its own BullMQ queue and worker pool, so a slow stage limits only itself. Scan must pass before anything else touches the file; extract, thumbnail and process (the external processing API) then run in parallel, and the file completes when all three finish.

| Stage | Vendor (mocked) | Worker concurrency | Timeout | Depends on |
| --- | --- | --- | --- | --- |
| `scan` | Malware scanner | 8 | 30 s | `uploaded` |
| `extract` | Metadata / OCR API | 4 | 60 s | `scan` passed |
| `thumbnail` | Image/preview service | 6 | 45 s | `scan` passed |
| `process` | External processing API (async job + webhook) | 8 | 10 s submit; 15 min job deadline | `scan` passed |
| `finalize` | none (internal) | 16 | 5 s | `extract`, `thumbnail` and `process` |

**Concurrency at three levels**

- **Browser:** 3 files uploading at once, 4 chunks each, via a small promise pool. Remaining files queue on the client so the tab never opens 200 sockets.
- **Worker pool:** per-queue `concurrency` above, tuned so the sum of in-flight vendor calls stays under each vendor's rate limit.
- **Per user fairness:** BullMQ group keys by `user_id` with max 10 active jobs per user, so one user's 50-file batch cannot starve everyone else.

**Fan-in without races.** When `extract`, `thumbnail` or `process` finishes, it runs `UPDATE jobs SET status='done'` and then counts remaining jobs for that file in the same transaction (`SELECT ... FOR UPDATE` on the file row). Only the transaction that sees zero remaining enqueues `finalize`, with job id `finalize:<file_id>` so a duplicate is a no-op. For `process`, "finishes" means the vendor reported done. The webhook handler and the poller both run this same transaction, so whichever arrives first completes the job and the other finds it already done.

**Backpressure.** If a queue's waiting count passes a high-water mark (e.g. 5,000), `POST /batches` returns `503` with `Retry-After`, and the UI shows "Busy — your upload will start shortly" and retries. Uploads already in S3 are never dropped; they simply wait in the queue.

**Large files.** Workers stream objects from S3 with ranged `GET`s; no stage loads a whole file into memory. Memory per worker is capped and checked in load tests.

## Idempotency

Every layer assumes it will be called twice: queues deliver at least once, browsers retry, and workers crash mid-job. We make each effect safe to repeat rather than trying to guarantee exactly-once delivery.

| Layer | Key | Mechanism |
| --- | --- | --- |
| API writes | `Idempotency-Key` header (UUID v4 from client) | Middleware stores (`user_id`, key) → request hash + response for 24 h in Postgres. Same key + same body returns the stored response; same key + different body returns `422`. A key still in flight returns `409` with `Retry-After: 1`. |
| Batch / file rows | (`user_id`, `idempotency_key`), (`batch_id`, `client_file_id`) | Unique constraints; `INSERT ... ON CONFLICT DO NOTHING RETURNING`, then read the existing row. |
| S3 objects | `s3_key = users/<user>/<file_id>/<sha256>` | Deterministic key: re-uploading the same part or object overwrites identical bytes. Content dedupe by `sha256` skips upload entirely. |
| Multipart | stored `s3_upload_id` | Created once per file; parts are keyed by part number so a re-sent part replaces itself. |
| Queue jobs | `jobId = <stage>:<file_id>` | BullMQ ignores an add with an existing job id. Outbox relay can publish twice safely. |
| Stage transitions | `status` + `version` guard | `UPDATE files SET status='processing', version=version+1 WHERE id=$1 AND version=$2`. Zero rows updated = someone else already did it; the job exits as success. |
| Vendor calls | `sha256(file_id + stage + vendor)` | Checked in `vendor_calls` before calling; result saved after. Sent to vendors that accept an idempotency header. |
| Progress events | per-batch `seq` | Client ignores any event with `seq` ≤ last applied. |

**Transactional outbox.** State changes and "enqueue next stage" are written in one Postgres transaction to the `files` and `outbox` tables. A relay process reads unpublished outbox rows, adds the BullMQ job (with its deterministic id) and marks the row published. A crash between the two steps only causes a repeat publish, which the job id makes harmless. This removes the classic "DB updated but job never enqueued" bug.

## Rate limiting

We limit in two directions: inbound, to protect our API from any one user, and outbound, to stay inside each vendor's quota. Both use token buckets in Redis, updated by one Lua script so the check-and-take is atomic across all pods.

| Limit | Scope | Bucket | On limit |
| --- | --- | --- | --- |
| API requests | per user | 20 req/s, burst 60 | `429` + `Retry-After`; client backs off |
| Part signing | per user | 500 parts/min | `429`; client signs in smaller pages |
| Batch creation | per user | 10 batches/min, 50 files each | `429` with plain message in UI |
| Malware scanner | global | 50 req/s, burst 100 | Job delayed until next token |
| Metadata API | global | 10 req/s, 1,000/hour | Job delayed until next token |
| Thumbnail service | global | 30 req/s | Job delayed until next token |
| Processing API — submits | global | 20 req/s, burst 40 | Job delayed until next token |
| Processing API — status polls | global, separate bucket | 30 req/s | Poll skipped and rescheduled; webhooks still complete jobs. Polls can never use up the submit quota |

**Outbound behaviour.** Before calling a vendor, the worker calls `acquire(vendor)`. If no token is free, the job is moved back to the delayed set with the exact wait the script returns, and the worker slot is freed for other work — workers never sleep while holding a slot. A `429` from the vendor itself also drains the local bucket and honours the vendor's `Retry-After`, so our limiter self-corrects if a quota was lowered.

**Inbound behaviour.** Standard headers `RateLimit-Limit`, `RateLimit-Remaining`, `RateLimit-Reset` on every response. The client upload pool reads `Retry-After` and pauses only the affected file, showing "Paused — resuming in 3s".

Limits live in config (`config/limits.yaml`) and can be changed without a deploy.

## Circuit breaking, retries and timeouts

Each vendor (and S3) gets its own circuit breaker whose state is shared through Redis, so when a vendor goes down every worker stops calling it within seconds. Jobs for that vendor are parked as `waiting_vendor` instead of failing, and resume on their own when the breaker closes.

**Breaker states**

| From | To | When |
| --- | --- | --- |
| Closed | Open | ≥ 50% of calls fail over a rolling 30 s window with at least 20 calls, or 5 timeouts in a row |
| Open | Half-open | After a cool-down of 30 s, doubling on each re-open, capped at 5 min |
| Half-open | Closed | 3 trial calls in a row succeed |
| Half-open | Open | Any trial call fails |

Only transient errors count as failures: timeouts, connection errors, `5xx` and `429`. A `4xx` for a bad file (e.g. unsupported format) is the file's problem, not the vendor's, and never trips the breaker. Half-open admits trial calls through a Redis lock so only 3 go out across the fleet.

**Retries with backoff.** Transient errors retry with exponential backoff and full jitter: `delay = random(0, min(cap, base × 2^attempt))`, base 2 s, cap 5 min, 6 attempts per stage. A retry budget caps retries at 20% of a vendor's call volume, so a partial outage cannot turn into a retry storm.

**Timeouts everywhere.** Every outbound call has a timeout (see the pipeline table) and an `AbortController`. The job itself has a timeout 2× the call's, and BullMQ's stalled-job check (lock 60 s) rescues jobs from crashed workers.

**Bulkheads.** Each vendor has its own queue and its own HTTP connection pool (max 50 sockets). If the metadata API hangs, scan and thumbnail jobs keep running at full speed.

**Implementation.** A single `callVendor(vendor, key, fn)` wrapper applies, in order: idempotency cache check → breaker check → rate-limit token → timeout → call → classify error → record breaker outcome → cache result. Built on `opossum` for breaker logic with a Redis-backed state store; all vendor clients go through it, and no code calls a vendor directly.

## Error handling

Every error is classified once, at the point it happens, into one of three kinds, and the kind alone decides what happens next. A batch always ends with partial success allowed: 48 completed files and 2 failed ones is a normal, shown outcome, not a failed batch.

&#91;embedded content: file state machine · 5 happy-path states, 3 error states\]

Transient errors retry on their own, vendor outages park the file without spending attempts, and only permanent errors stop and ask the user.

| Kind | Examples | System action | What the user sees |
| --- | --- | --- | --- |
| Transient | Timeout, connection reset, `5xx`, `429`, S3 `SlowDown` | Retry with backoff + jitter, up to 6 attempts | "Retrying (2 of 6)…" — no action needed |
| Dependency down | Breaker open for a vendor | Park as `waiting_vendor`, resume on close | "Scanner is busy; we'll continue automatically" |
| Permanent | Unsupported type, file over 5 GB, checksum mismatch, malware found, corrupt file | Stop, mark `failed`, keep the object for audit | Specific reason + Retry or Remove button |
| Bug / unknown | Unhandled exception | Treat as transient for 3 attempts, then dead-letter queue and alert | "Something went wrong on our side; we've been notified" |

**Client-side errors.** A failed chunk retries 5 times with backoff before the file is marked errored; other chunks and files keep going. Going offline (`navigator.onLine`, failed fetches) pauses every upload and resumes on reconnect from the last confirmed part. A closed tab loses nothing already confirmed: the next visit lists unfinished files and offers "Resume" (the user re-selects the file; the client checks its SHA-256 matches).

**Dead-letter queue.** Jobs that exhaust retries go to a per-stage DLQ with the full error chain. An admin endpoint `POST /admin/dlq/:stage/replay` re-drives them once the cause is fixed; replays are safe because every stage is idempotent.

**User retry.** `POST /files/:id/retry` resets only the failed stage (completed stages are kept) and re-enqueues it. Retry is offered only when `retryable_by_user` is true — e.g. not for malware.

**Error contract.** Every API error uses one shape: `{ code, message, retryable, retry_after_ms, request_id }`. `code` is a stable enum (e.g. `FILE_TOO_LARGE`, `CHECKSUM_MISMATCH`, `VENDOR_UNAVAILABLE`); `message` is safe to show the user; stack traces and vendor responses never leave the server.

## Mocking S3 and third-party vendors

The whole system runs offline with `docker compose up`: S3 is replaced by MinIO, and each vendor by a small mock service we own. The mocks are not stubs that always succeed — each can be told to be slow, flaky, rate-limited or down, so the resilience code is exercised every day, not only in production incidents.

| Dependency | Mock | Why this one |
| --- | --- | --- |
| AWS S3 | MinIO container (`S3_ENDPOINT=http://minio:9000`, `forcePathStyle: true`) | Real S3 API including multipart, presigned URLs, ETags and checksums; browsers can upload to it directly. LocalStack is an alternative if we later need SQS or SNS too. |
| Malware scanner | `mock-scanner` (Fastify, \~150 lines) | Returns `clean` / `infected` from a rule: a file whose name contains `EICAR` is infected. |
| Metadata / OCR API | `mock-extract` | Returns canned JSON per file type after a configurable delay. |
| Thumbnail service | `mock-thumbs` | Writes a fixed placeholder PNG back to MinIO. |
| Unit tests | `aws-sdk-client-mock` for the S3 client, `nock` / `msw` for HTTP vendors | No containers needed; fastest feedback. |

**One interface, swappable implementations.** Every dependency sits behind a port: `ObjectStore` (S3 / MinIO / in-memory), `Scanner`, `Extractor`, `Thumbnailer`. `config.ts` picks the adapter from env (`VENDOR_MODE=mock|live`). Workers and API code never import an SDK directly, so switching to real vendors is configuration only.

**Fault injection.** Every mock reads a fault profile from env or from `POST /__faults` at runtime:

- `latency_ms` and `latency_jitter_ms` — slow responses, to test timeouts.
- `error_rate` with `error_status` (e.g. 0.3 and `503`) — flakiness, to test retries.
- `rate_limit_rps` — mock returns `429` with `Retry-After` above this rate, to test the limiter.
- `down: true` — refuses connections, to test the breaker and `waiting_vendor`.
- `hang: true` — accepts and never responds, to test aborts.
- `fail_file_ids` — permanent `4xx` for chosen files, to test per-file isolation.

For S3 faults, Toxiproxy sits between workers and MinIO and adds latency, resets or bandwidth limits. A `make chaos` target runs the e2e suite with a random profile per run, logging the seed so failures replay exactly.

**Seed data.** `fixtures/` holds sample files per type, an EICAR test file, a zero-byte file, a 6 GB sparse file for the size limit, and a file with a deliberately wrong checksum.

## Scaling to 100k users

The architecture holds at 100k users without a redesign, because bytes bypass our servers and the API and workers are stateless. The vendor quotas assumed above are the first hard limit: the metadata API at 10 req/s can't keep up even on an average day. After that, the main pressure points are Postgres write churn, open SSE connections and storage cost.

**Load estimate.** The assumptions are mine, so replace them with real product numbers. Here, 100k means daily active users. If it means registered users, divide the load by roughly 4.

| Assumption | Value |
| --- | --- |
| Daily active users | 100,000 |
| Files per active user per day | 10 (one batch) |
| Average file size | 10 MB |
| Peak hour vs average | 5× |

| Derived load | Average | Peak |
| --- | --- | --- |
| Files processed | 12 files/s (1M/day) | 60 files/s |
| Ingest into S3 | 115 MB/s (10 TB/day) | 600 MB/s (\~5 Gbps) |
| API requests (\~10 per file) | 120 req/s | 600 req/s |
| Calls per vendor (1 per file per stage) | 12 req/s | 60 req/s |
| Postgres writes (\~30 per file) | 360/s | 1,800/s |
| Open SSE streams (5% of DAU at peak) | — | 5,000 |
| New storage | 10 TB/day | \~300 TB/month before lifecycle |

**What breaks and what to change**

| Component | Breaks because | Change for 100k | Priority |
| --- | --- | --- | --- |
| Vendor quotas | Peak needs 60 req/s per vendor. Extract is capped at 10 req/s (864k/day, below the 1M/day average), so its backlog grows without limit | Negotiate quotas ≥ 2× peak (scan 120, extract 120, thumbnail 120 req/s), or use batch endpoints, or shard across several vendor accounts with one token bucket each. Skip vendor calls for content-dedupe hits. Set a backlog SLO (e.g. 95% of files done within 10 min) and alert on it | Must |
| Postgres writes | 1,800 writes/s at peak, plus dead rows from part reports and outbox churn, cause vacuum lag and bloat | Keep `progress_pct` in Redis only and write to Postgres only on stage transitions. Record parts in one `complete` call instead of one row per part (on resume, use S3 `ListParts`). Delete outbox rows once published. Partition `files`, `jobs` and `vendor_calls` by month. Add PgBouncer, plus a read replica for status reads | Must |
| Postgres size | \~30M file rows a month | Partition by month, archive after 90 days, expire idempotency keys after 24 h with a nightly job | Must |
| SSE connections | 5,000 long-lived streams on API pods block rolling deploys and skew load balancing | Split out a separate `progress-gateway` service (Node handles \~10k streams per pod), with a 25 s heartbeat and an LB idle timeout over 60 s. Drain on deploy by closing streams; clients reconnect with `Last-Event-ID` | Should |
| Redis | One instance mixes queues, limiters, breakers and pub/sub, so a queue backlog can starve rate-limit checks | Split into three instances: `redis-queues` (BullMQ, AOF on), `redis-coord` (buckets, breakers, idempotency locks) and `redis-events` (sharded pub/sub `SPUBLISH`, streams with `MAXLEN`). Move queues to Redis Cluster with `{stage}` hash tags only if one shard exceeds \~50k ops/s | Should |
| Outbox relay | A single relay process is a throughput ceiling and a single point of failure | Run 3+ relays claiming rows with `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 500` | Must |
| Workers | Fixed pool sizes are idle at night and short at peak | Autoscale each stage on queue depth and age (KEDA), with the per-stage max set by the vendor quota, not by CPU | Must |
| Per-user fairness | BullMQ group keys need BullMQ Pro | Buy Pro, or enforce "10 active jobs per user" with a Redis counter checked on dequeue, re-delaying the job if the user is over | Should |
| API | 600 req/s is well within the capacity of 3–4 pods; per-part reporting is the chattiest call | Batch part reports (or drop them, see Postgres). Autoscale on CPU, minimum 3 pods across availability zones | Should |
| S3 | Per-prefix limits are 3,500 PUT/s and 5,500 GET/s. The `users/<id>/` keys already spread load across prefixes | No change to throughput. Turn on Transfer Acceleration if users are far from the region. Add lifecycle rules: Infrequent Access at 30 days, abort incomplete multipart uploads after 1 day | Should |
| Storage cost | 10 TB/day is the largest line item | Set per-user daily and total quotas, use content dedupe (already designed), lifecycle tiering, and a retention policy for failed and infected files | Must |
| Abuse | Free accounts can upload junk at scale | Per-user storage quota, per-IP limit on batch creation, and malware scanning before any file is shareable | Should |

**What stays the same.** Direct-to-S3 multipart uploads, the state machine, idempotency keys, the `callVendor` wrapper and the error taxonomy need no changes. They scale by running more copies.

**How to prove it.** Before launch, run the k6 load test at 2× the estimated peak (120 files/s, 10,000 SSE streams) against mocks set to the negotiated vendor quotas. Pass criteria: p95 time-to-first-progress under 1 s, no vendor `429`s, Postgres replication lag under 1 s, and the queue backlog drains within 15 min after the peak ends.

## Testing, observability and milestones

The test plan is built around the failure modes, not just the happy path: every row in the error-handling table has at least one automated test that triggers it through the mocks.

| Level | Tool | What it proves |
| --- | --- | --- |
| Unit | Vitest | Error classifier, backoff maths, state-transition guards, reducer on the client |
| Integration | Vitest + Testcontainers (Postgres, Redis, MinIO) | Idempotency middleware, outbox relay, multipart complete, rate-limit Lua script, breaker shared across 2 worker processes |
| End-to-end | Playwright against `docker compose` | 50-file drop with live per-file bars; refresh mid-upload resumes; offline/online; per-file retry |
| Resilience | Playwright + fault profiles | Vendor down → files show "waiting" then complete when restored; 30% `503` → all files still complete; malware file fails alone |
| Idempotency | Custom harness | Every job run twice, every API call sent twice, workers killed with `SIGKILL` mid-job: exactly one object, one result, one completed state per file |
| Load | k6 | 200 concurrent users × 20 files; vendor quotas never exceeded; p95 time-to-first-progress < 1 s |

**Observability.** Structured JSON logs with `request_id`, `batch_id`, `file_id` and `job_id` on every line; OpenTelemetry traces from browser request through API, queue and vendor call. Key metrics and their alerts:

- Queue depth and age per stage — alert if oldest waiting job > 10 min.
- Breaker state per vendor — alert on open > 5 min.
- Vendor `429` rate and token-bucket wait time.
- File outcomes: completed / failed by `error_code` — alert if permanent failures > 2% over 15 min.
- DLQ size — alert on any growth.
- Upload resume rate and chunk retry rate from client telemetry.

**Tech stack.** TypeScript throughout. React + Vite client; Fastify API; BullMQ workers on Redis 7; Postgres 16 with Drizzle migrations; MinIO for S3 locally; Docker Compose for local and CI.

**Milestones**

1. **Skeleton:** Compose stack (Postgres, Redis, MinIO, three mock vendors), ports and adapters, error contract, CI.
2. **Uploads:** batch create with idempotency keys, presigned multipart, resume, client upload pool with per-file progress.
3. **Pipeline:** queues, stages, outbox, fan-in, SSE progress with replay.
4. **Resilience:** rate limiters, `callVendor` wrapper with breakers, retries, DLQ, `waiting_vendor`, user retry.
5. **Hardening:** fault-injection and chaos suite, load test, dashboards and alerts, swap one mock for a real vendor in staging.

**Open questions**

- Which real vendors replace the mocks, and what are their actual quotas and idempotency support?
- How long do we keep files that failed scanning, and who can see them?
- Is 50 files / 5 GB per file the right limit for launch?
