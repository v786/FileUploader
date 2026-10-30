/**
 * Client for the upload API. Every error has one shape ({code, message, retryable,
 * retry_after_ms, request_id}). 429 / 503 and "request still in flight" are retried
 * here, honouring Retry-After, and reported through `onWait` so the UI can say
 * "Paused — resuming in 3s" for just the affected file.
 */
import type { FileResult, ServerBatchView, ServerFileView, StartInfo } from "./types";

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly retryable = false,
    readonly retryAfterMs: number | null = null,
    readonly requestId: string | null = null,
  ) {
    super(message);
  }
}

export interface CallOptions {
  idempotencyKey?: string;
  signal?: AbortSignal;
  /** Called before each wait caused by 429 / 503 / network trouble. */
  onWait?: (ms: number, code: string) => void;
  maxAttempts?: number;
}

export const sleep = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve, reject) => {
    if (signal?.aborted) return reject(new DOMException("aborted", "AbortError"));
    const t = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(t);
      reject(new DOMException("aborted", "AbortError"));
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });

export const isAbort = (e: unknown) => e instanceof DOMException && e.name === "AbortError";

export function backoffMs(attempt: number, base = 500, cap = 8000): number {
  return Math.random() * Math.min(cap, base * 2 ** attempt); // full jitter
}

export function newKey(): string {
  return globalThis.crypto?.randomUUID?.() ?? `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

export interface CreateFile {
  client_file_id: string;
  name: string;
  size_bytes: number;
  fingerprint?: string;
  sha256?: string;
  head_hex?: string;
  part_size?: number;
}

export class UploadApi {
  constructor(private readonly baseUrl: string, private readonly userId: string) {}

  async request<T>(method: string, path: string, body?: unknown, opts: CallOptions = {}): Promise<T> {
    const max = opts.maxAttempts ?? 6;
    for (let attempt = 0; ; attempt++) {
      let res: Response;
      try {
        res = await fetch(this.baseUrl + path, {
          method,
          signal: opts.signal,
          headers: {
            "X-User-Id": this.userId,
            ...(body !== undefined ? { "Content-Type": "application/json" } : {}),
            ...(opts.idempotencyKey ? { "Idempotency-Key": opts.idempotencyKey } : {}),
          },
          body: body !== undefined ? JSON.stringify(body) : undefined,
        });
      } catch (e) {
        if (isAbort(e)) throw e;
        if (attempt + 1 >= max) throw new ApiError(0, "NETWORK_ERROR", "Can't reach the server. Check your connection.", true);
        const wait = backoffMs(attempt);
        opts.onWait?.(wait, "NETWORK_ERROR");
        await sleep(wait, opts.signal);
        continue;
      }
      if (res.ok) return (await res.json()) as T;

      const err = await toApiError(res);
      const transient = res.status === 429 || res.status === 503 || res.status >= 500
        || err.code === "IDEMPOTENCY_IN_FLIGHT";
      if (!transient || attempt + 1 >= max) throw err;
      const wait = err.retryAfterMs ?? backoffMs(attempt);
      opts.onWait?.(wait, err.code);
      await sleep(wait, opts.signal);
    }
  }

  createBatch(files: CreateFile[], key: string, opts: CallOptions = {}) {
    return this.request<ServerBatchView>("POST", "/batches", { files }, { maxAttempts: 30, ...opts, idempotencyKey: key });
  }
  getBatch(batchId: string) {
    return this.request<ServerBatchView>("GET", `/batches/${batchId}`);
  }
  startUpload(fileId: string, opts?: CallOptions) {
    return this.request<StartInfo>("POST", `/files/${fileId}/multipart`, undefined, opts);
  }
  signParts(fileId: string, partNumbers: number[], opts?: CallOptions) {
    return this.request<{ urls: Record<string, string>; expires_at: number }>(
      "POST", `/files/${fileId}/parts:sign`, { part_numbers: partNumbers }, opts);
  }
  reportPart(fileId: string, n: number, etag: string, size: number) {
    return this.request("POST", `/files/${fileId}/parts/${n}`, { etag, size_bytes: size }, { maxAttempts: 2 });
  }
  listParts(fileId: string, opts?: CallOptions) {
    return this.request<{ parts: { part_number: number; etag: string; size_bytes: number }[]; part_size: number }>(
      "GET", `/files/${fileId}/parts`, undefined, opts);
  }
  complete(fileId: string, sha256: string, opts?: CallOptions) {
    return this.request<ServerFileView>("POST", `/files/${fileId}/complete`, { sha256 }, opts);
  }
  retry(fileId: string) {
    return this.request<ServerFileView>("POST", `/files/${fileId}/retry`, undefined, { idempotencyKey: newKey() });
  }
  cancel(fileId: string) {
    return this.request<ServerFileView>("POST", `/files/${fileId}/cancel`);
  }
  unfinished() {
    return this.request<{ files: ServerFileView[] }>("GET", "/files?unfinished=true");
  }
  result(fileId: string) {
    return this.request<FileResult>("GET", `/files/${fileId}/result`);
  }
  eventsUrl(batchId: string, lastEventId?: number): string {
    const q = new URLSearchParams({ user_id: this.userId }); // EventSource can't send headers
    if (lastEventId !== undefined) q.set("last_event_id", String(lastEventId));
    return `${this.baseUrl}/batches/${batchId}/events?${q}`;
  }
}

async function toApiError(res: Response): Promise<ApiError> {
  let body: Record<string, unknown> = {};
  try {
    body = await res.json();
  } catch {
    /* not JSON */
  }
  const header = res.headers.get("Retry-After");
  const retryAfter = typeof body.retry_after_ms === "number" ? body.retry_after_ms
    : header ? Number(header) * 1000 : null;
  return new ApiError(res.status, String(body.code ?? `HTTP_${res.status}`),
    String(body.message ?? `Request failed (${res.status}).`), Boolean(body.retryable), retryAfter,
    (body.request_id as string) ?? null);
}

// ---- direct-to-storage PUTs (presigned URLs) ----

export class PutError extends Error {
  constructor(readonly status: number, readonly code: string) {
    super(`PUT failed: ${status} ${code}`);
  }
}

/** PUT a blob to a presigned URL with byte-level progress (fetch can't report upload progress). */
export function putBlob(url: string, blob: Blob, opts: { checksum?: string; signal?: AbortSignal;
                                                         onProgress?: (loaded: number) => void }): Promise<string> {
  return new Promise((resolve, reject) => {
    if (opts.signal?.aborted) return reject(new DOMException("aborted", "AbortError"));
    const xhr = new XMLHttpRequest();
    xhr.open("PUT", url);
    if (opts.checksum) xhr.setRequestHeader("x-amz-checksum-sha256", opts.checksum);
    xhr.upload.onprogress = (e) => {
      if (!opts.signal?.aborted) opts.onProgress?.(e.loaded); // a paused part must not move the bar
    };
    const onAbort = () => xhr.abort();
    opts.signal?.addEventListener("abort", onAbort, { once: true });
    const done = () => opts.signal?.removeEventListener("abort", onAbort);
    xhr.onload = () => {
      done();
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve((xhr.getResponseHeader("ETag") ?? "").replace(/"/g, ""));
      } else {
        const code = /<Code>([^<]+)<\/Code>/.exec(xhr.responseText ?? "")?.[1] ?? `HTTP_${xhr.status}`;
        reject(new PutError(xhr.status, code));
      }
    };
    xhr.onerror = () => {
      done();
      reject(new PutError(0, "NetworkError"));
    };
    xhr.onabort = () => {
      done();
      reject(new DOMException("aborted", "AbortError"));
    };
    xhr.send(blob);
  });
}
