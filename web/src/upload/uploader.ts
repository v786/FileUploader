/**
 * The upload engine. Framework-free: React (or anything else) reads `store` and calls
 * the actions. One instance per signed-in user.
 *
 * Flow per file:
 *   addFiles   validate in the browser (type by magic bytes, size, count, names)
 *   start      POST /batches: one call creates every upload and signs its first parts
 *   upload     3 files at once (smallest first), 4 parts each, straight to storage with
 *              per-part checksums; the full SHA-256 is computed in a worker meanwhile
 *   complete   send the hash; the server verifies and takes over
 *   process    server-sent events drive the processing half of the bar
 */
import { ApiError, PutError, UploadApi, backoffMs, isAbort, newKey, putBlob, sleep } from "./api";
import type { CreateFile } from "./api";
import { fingerprint, hashFile, partChecksum, readHead } from "./hash";
import type { HashJob } from "./hash";
import { DEFAULT_PROCESSING_MS, EtaSmoother, Throughput, uploadWeights } from "./progress";
import { stageLabel, vendorLabel } from "./labels";
import { Store } from "./store";
import { BatchStream } from "./stream";
import type {
  BatchInfo, FileError, Limits, Phase, ServerBatchView, ServerFileView, StartInfo, UploadItem, UploaderOptions,
  UploaderState,
} from "./types";
import { DEFAULT_LIMITS, clientError, toHexString, validate } from "./validate";

const MB = 1024 * 1024;
const GB = 1024 * MB;
const PART_RETRIES = 5;
const FINAL: Phase[] = ["done", "error", "cancelled", "invalid"];
const UNCONFIRMED: Phase[] = ["queued", "uploading", "paused", "finishing"];

class UploadFailure extends Error {}

interface Runtime {
  file: File;
  headHex?: string;
  abort: AbortController | null;
  hash: HashJob | null;
  paused: boolean;
  offlinePaused: boolean;
  fresh: boolean; // upload created by POST /batches just now: nothing in storage yet
  start?: StartInfo;
  done: Map<number, number>; // part -> bytes stored
  inflight: Map<number, number>; // part -> bytes sent so far
  urls: Map<number, { url: string; expiresAt: number }>;
  signing: Promise<void> | null;
  eta: EtaSmoother | null;
  speed: number;
  lastBytes: number;
  lastAt: number;
}

const newRuntime = (file: File): Runtime => ({
  file, abort: null, hash: null, paused: false, offlinePaused: false, fresh: false, done: new Map(),
  inflight: new Map(), urls: new Map(), signing: null, eta: null, speed: 0, lastBytes: 0, lastAt: 0,
});

const now = () => (typeof performance !== "undefined" ? performance.now() : Date.now());

/** Adaptive part size: bigger parts for big files and fast links, never more than 10,000 parts. */
export function choosePartSize(size: number, bytesPerSecond: number): number | undefined {
  if (size < 8 * MB) return undefined; // a single PUT
  let ps = size < GB ? 8 * MB : size < 4 * GB ? 16 * MB : 32 * MB;
  if (bytesPerSecond > 12.5e6) ps *= 2;
  return Math.min(64 * MB, Math.max(5 * MB, ps, Math.ceil(size / 10_000)));
}

function derive(it: UploadItem): UploadItem {
  if (it.phase === "done") return { ...it, overallPct: 100, tone: "done", etaSeconds: null };
  if (it.phase === "error" || it.phase === "cancelled" || it.phase === "invalid" || !it.weights) return it;
  const raw = it.weights.upload * it.uploadPct + it.weights.process * it.processingPct;
  const pct = Math.max(it.overallPct, Math.min(99, Math.round(raw * 10) / 10)); // never backwards; 100 only when done
  return pct === it.overallPct ? it : { ...it, overallPct: pct };
}

function toFileError(e: unknown): FileError {
  if (e instanceof ApiError) {
    if (["CHECKSUM_MISMATCH", "SIZE_MISMATCH", "TYPE_MISMATCH"].includes(e.code)) {
      const replace = e.code === "TYPE_MISMATCH";
      return { code: e.code, message: e.message, retryable: !replace, source: "server",
               fix: replace ? { action: "replace", label: "Replace file" } : { action: "retry", label: "Retry upload" } };
    }
    return { code: e.code, message: e.message, retryable: true, source: "client",
             fix: { action: "retry", label: "Retry upload" } };
  }
  if (e instanceof UploadFailure) {
    return { code: "UPLOAD_FAILED", message: e.message, retryable: true, source: "client",
             fix: { action: "retry", label: "Retry upload" } };
  }
  return { code: "CLIENT_ERROR", message: "Something went wrong in the browser while uploading.", retryable: true,
           source: "client", fix: { action: "retry", label: "Retry" } };
}

const messageOf = (e: unknown) => (e instanceof Error ? e.message : String(e));

export class Uploader {
  readonly store: Store<UploaderState>;
  readonly api: UploadApi;
  readonly limits: Limits;
  private readonly fileConcurrency: number;
  private readonly partConcurrency: number;
  private readonly notify: boolean;
  private rt = new Map<string, Runtime>();
  private streams = new Map<string, BatchStream>();
  private active = 0;
  private prio = 0;
  private bps = new Throughput(4);
  private notified = new Set<string>();
  private attached = 0;
  private disposeTimer: ReturnType<typeof setTimeout> | null = null;
  private disposed = false;

  constructor(opts: UploaderOptions) {
    this.api = new UploadApi(opts.baseUrl ?? "", opts.userId);
    this.limits = { ...DEFAULT_LIMITS, ...opts.limits };
    this.fileConcurrency = opts.fileConcurrency ?? 3;
    this.partConcurrency = opts.partConcurrency ?? 4;
    this.notify = opts.notify ?? false;
    this.store = new Store<UploaderState>({
      items: [], batches: {}, online: typeof navigator === "undefined" ? true : navigator.onLine, starting: false,
      notice: null, interrupted: [],
    });
  }

  // ------------------------------------------------------------------ lifecycle

  /** Start listening for online/offline. Returns the cleanup. Safe under React StrictMode. */
  attach(disposeOnDetach = false): () => void {
    this.attached += 1;
    if (this.disposeTimer) clearTimeout(this.disposeTimer);
    const on = () => this.setOnline(true);
    const off = () => this.setOnline(false);
    window.addEventListener("online", on);
    window.addEventListener("offline", off);
    return () => {
      window.removeEventListener("online", on);
      window.removeEventListener("offline", off);
      this.attached -= 1;
      if (disposeOnDetach && this.attached === 0) {
        this.disposeTimer = setTimeout(() => this.attached === 0 && this.dispose(), 0);
      }
    };
  }

  /** Stop everything: abort uploads, close streams, free previews. */
  dispose(): void {
    this.disposed = true;
    for (const s of this.streams.values()) s.close();
    this.streams.clear();
    for (const r of this.rt.values()) {
      r.paused = true;
      r.abort?.abort();
      r.hash?.cancel();
    }
    for (const it of this.state.items) if (it.previewUrl) URL.revokeObjectURL(it.previewUrl);
  }

  // ------------------------------------------------------------------ state helpers

  private get state(): UploaderState {
    return this.store.get();
  }

  private set(patch: Partial<UploaderState>, urgent = true): void {
    this.store.set({ ...this.state, ...patch }, urgent);
  }

  item(localId: string): UploadItem | undefined {
    return this.state.items.find((i) => i.localId === localId);
  }

  private byFileId(fileId: unknown): UploadItem | undefined {
    return this.state.items.find((i) => i.fileId === fileId);
  }

  private update(localId: string, patch: Partial<UploadItem> | ((it: UploadItem) => Partial<UploadItem>),
                 urgent = true): void {
    let changed = false;
    const items = this.state.items.map((it) => {
      if (it.localId !== localId) return it;
      changed = true;
      return derive({ ...it, ...(typeof patch === "function" ? patch(it) : patch) });
    });
    if (changed) this.set({ items }, urgent);
  }

  private setBatch(batchId: string, patch: Partial<BatchInfo>): void {
    const prev = this.state.batches[batchId] ?? { id: batchId, seq: 0, createdAt: Date.now() };
    this.set({ batches: { ...this.state.batches, [batchId]: { ...prev, ...patch } } });
  }

  private weightsFor(it: UploadItem) {
    return uploadWeights(it.size, this.bps.estimate(), it.processingEstimateMs);
  }

  /** True while bytes exist only in this tab: leaving now would lose work. */
  hasUnconfirmedBytes(): boolean {
    return this.state.items.some((i) => UNCONFIRMED.includes(i.phase));
  }

  // ------------------------------------------------------------------ selection

  /** Add files to the selection; each is validated in the browser before any request. */
  async addFiles(list: FileList | File[]): Promise<void> {
    const files = Array.from(list);
    if (!files.length) return;
    const fresh: UploadItem[] = files.map((f) => ({
      localId: newKey(), name: f.name, size: f.size, mime: f.type, phase: "checking", tone: "normal", seq: 0,
      uploadPct: 0, processingPct: 0, overallPct: 0,
      previewUrl: f.type.startsWith("image/") && f.size < 20 * MB && typeof URL.createObjectURL === "function"
        ? URL.createObjectURL(f) : undefined,
    }));
    fresh.forEach((it, i) => this.rt.set(it.localId, newRuntime(files[i])));
    this.set({ items: [...this.state.items, ...fresh] });

    const heads = await Promise.all(files.map((f) => readHead(f)));
    const staged = this.state.items.filter((i) => i.phase === "staged");
    const codes = validate(files.map((f, i) => ({ name: f.name, size: f.size, head: heads[i] })),
      new Set(staged.map((s) => s.name.toLowerCase())), staged.length, this.limits);
    fresh.forEach((it, i) => {
      const rt = this.rt.get(it.localId);
      if (rt) rt.headHex = toHexString(heads[i]);
      const code = codes[i];
      if (!code) return this.update(it.localId, { phase: "staged" });
      if (it.previewUrl) URL.revokeObjectURL(it.previewUrl); // e.g. a ".png" that isn't an image: don't show a broken preview
      this.update(it.localId, { phase: "invalid", tone: "error", error: clientError(code), previewUrl: undefined });
    });
  }

  /** Remove a file that isn't uploading (selected, rejected, finished). */
  remove(localId: string): void {
    const it = this.item(localId);
    if (!it) return;
    if (!FINAL.includes(it.phase) && !["staged", "checking"].includes(it.phase)) return;
    const r = this.rt.get(localId);
    r?.hash?.cancel();
    this.rt.delete(localId);
    if (it.previewUrl) URL.revokeObjectURL(it.previewUrl);
    this.set({ items: this.state.items.filter((i) => i.localId !== localId) });
  }

  /** Clear finished and rejected rows (keeps anything still in flight or needing attention). */
  clearFinished(): void {
    for (const it of this.state.items) if (["done", "cancelled", "invalid"].includes(it.phase)) this.remove(it.localId);
  }

  // ------------------------------------------------------------------ start

  /** Create a batch from the selected files and start uploading. */
  async start(): Promise<void> {
    const staged = this.state.items.filter((i) => i.phase === "staged");
    if (!staged.length || this.state.starting) return;
    this.set({ starting: true, notice: null });
    const key = newKey(); // reused by every retry of this create: never a duplicate batch
    staged.forEach((it) => this.update(it.localId, { phase: "queued", note: "Starting…" }));
    try {
      const bps = this.bps.estimate();
      const files: CreateFile[] = await Promise.all(staged.map(async (it) => {
        const rt = this.rt.get(it.localId)!;
        return { client_file_id: it.localId, name: it.name, size_bytes: it.size, fingerprint: await fingerprint(rt.file),
                 head_hex: rt.headHex, part_size: choosePartSize(it.size, bps) };
      }));
      const batch = await this.api.createBatch(files, key, {
        onWait: (ms, code) => this.set({ notice: { tone: "info", text:
          code === "BUSY" ? "Busy — your upload will start shortly"
            : code === "RATE_LIMITED" ? `Too many requests — starting in ${Math.ceil(ms / 1000)}s`
              : "Connection trouble — retrying…" } }),
      });
      this.set({ notice: null });
      this.setBatch(batch.id, { summary: batch.summary, seq: batch.seq });
      for (const f of batch.files) {
        const it = this.item(f.client_file_id);
        const rt = this.rt.get(f.client_file_id);
        if (!it || !rt) continue;
        rt.start = f.upload.start;
        rt.fresh = Boolean(f.upload.start);
        this.update(it.localId, { fileId: f.id, batchId: batch.id, note: undefined, dedupeCandidate: f.upload.dedupe_candidate,
                                  processingEstimateMs: f.processing_estimate_ms ?? undefined });
        this.applyView(it.localId, f);
      }
      this.openStream(batch.id);
      this.pump();
    } catch (e) {
      staged.forEach((it) => this.update(it.localId, { phase: "staged", note: undefined }));
      this.set({ notice: { tone: "error", text: `Couldn't start the upload: ${messageOf(e)}` } });
    } finally {
      this.set({ starting: false });
    }
  }

  // ------------------------------------------------------------------ queue

  private nextQueued(): UploadItem | undefined {
    return this.state.items
      .filter((i) => i.phase === "queued" && i.fileId && this.rt.has(i.localId))
      .sort((a, b) => (b.prioritized ?? 0) - (a.prioritized ?? 0) || a.size - b.size)[0]; // smallest first
  }

  private pump(): void {
    if (this.disposed || !this.state.online) return;
    while (this.active < this.fileConcurrency) {
      const next = this.nextQueued();
      if (!next) return;
      this.active += 1;
      this.update(next.localId, { phase: "uploading", note: undefined });
      this.uploadOne(next.localId).finally(() => {
        this.active -= 1;
        this.pump();
      });
    }
  }

  private async uploadOne(localId: string): Promise<void> {
    const rt = this.rt.get(localId)!;
    const it0 = this.item(localId)!;
    const fileId = it0.fileId!;
    rt.paused = false;
    rt.abort = new AbortController();
    const signal = rt.abort.signal;
    const onWait = (ms: number, code: string) => this.update(localId, {
      tone: "paused",
      note: code === "NETWORK_ERROR" ? "Connection trouble — retrying…" : `Paused — resuming in ${Math.ceil(ms / 1000)}s`,
    });
    try {
      rt.hash ??= hashFile(rt.file); // hash while uploading, not before
      rt.eta = new EtaSmoother(now());
      rt.lastAt = now();
      rt.lastBytes = 0;
      rt.speed = 0;
      this.update(localId, (it) => ({ weights: it.weights ?? this.weightsFor(it), tone: "normal", etaSeconds: null }));

      if (it0.dedupeCandidate) {
        // A fingerprint match: a duplicate only if the full hash matches too.
        this.update(localId, { phase: "finishing", note: "Checking for an existing copy…" });
        const sha = await rt.hash.promise;
        try {
          this.applyView(localId, await this.api.complete(fileId, sha, { signal, onWait }));
          return;
        } catch (e) {
          if (!(e instanceof ApiError && e.code === "UPLOAD_REQUIRED")) throw e;
        }
        this.update(localId, { dedupeCandidate: false, phase: "uploading", note: undefined });
      }

      const info = rt.start ?? (await this.api.startUpload(fileId, { signal, onWait }));
      rt.start = undefined;
      if (info.mode === "single") await this.putSingle(localId, fileId, info, signal);
      else await this.putParts(localId, fileId, info, signal, onWait);
      rt.fresh = false;

      this.update(localId, { phase: "finishing", uploadPct: 100, tone: "normal", note: "Verifying…", etaSeconds: null });
      const sha = await rt.hash.promise;
      const view = await this.api.complete(fileId, sha, { signal, onWait });
      this.applyView(localId, view);
      this.openStream(view.batch_id);
    } catch (e) {
      if (isAbort(e) || rt.paused) return;
      rt.fresh = false;
      this.update(localId, { phase: "error", tone: "error", note: undefined, etaSeconds: null, error: toFileError(e) });
    }
  }

  private async putSingle(localId: string, fileId: string, info: StartInfo, signal: AbortSignal): Promise<void> {
    const rt = this.rt.get(localId)!;
    let url = info.url!;
    const checksum = await partChecksum(rt.file);
    for (let attempt = 0; ; attempt++) {
      const t0 = now();
      try {
        await putBlob(url, rt.file, { checksum, signal, onProgress: (l) => {
          rt.inflight.set(1, l);
          this.progress(localId);
        } });
        rt.inflight.clear();
        rt.done.set(1, rt.file.size);
        if (rt.file.size >= MB) this.bps.add(rt.file.size, now() - t0);
        this.progress(localId);
        return;
      } catch (e) {
        rt.inflight.clear();
        if (isAbort(e)) throw e;
        if (attempt + 1 >= PART_RETRIES) throw new UploadFailure(`The upload failed after ${PART_RETRIES} tries.`);
        if (e instanceof PutError && e.status === 403) url = (await this.api.startUpload(fileId, { signal })).url!;
        await this.waitBeforeRetry(localId, attempt, signal);
      }
    }
  }

  private async putParts(localId: string, fileId: string, info: StartInfo, signal: AbortSignal,
                         onWait: (ms: number, code: string) => void): Promise<void> {
    const rt = this.rt.get(localId)!;
    const ps = info.part_size;
    const n = info.part_count;
    const expected = (p: number) => Math.min(ps, rt.file.size - (p - 1) * ps);
    for (const [k, url] of Object.entries(info.urls ?? {})) {
      rt.urls.set(Number(k), { url, expiresAt: (info.expires_at ?? 0) * 1000 });
    }
    if (!rt.fresh) {
      // Resume: storage (ListParts) knows which parts already landed.
      rt.done.clear();
      const { parts } = await this.api.listParts(fileId, { signal, onWait });
      for (const p of parts) if (p.size_bytes === expected(p.part_number)) rt.done.set(p.part_number, p.size_bytes);
      this.progress(localId);
    }
    const missing = Array.from({ length: n }, (_, i) => i + 1).filter((p) => !rt.done.has(p));
    let next = 0;
    let failure: unknown = null;
    const lane = async () => {
      while (next < missing.length && !signal.aborted) {
        const p = missing[next++];
        try {
          await this.uploadPart(localId, fileId, p, ps, n, signal);
        } catch (e) {
          if (failure === null && !isAbort(e)) {
            failure = e;
            rt.abort?.abort(); // stop the other lanes
          }
          return;
        }
      }
    };
    await Promise.all(Array.from({ length: Math.min(this.partConcurrency, missing.length) }, lane));
    if (failure !== null) throw failure;
    if (signal.aborted) throw new DOMException("aborted", "AbortError");
  }

  private async uploadPart(localId: string, fileId: string, p: number, ps: number, n: number,
                           signal: AbortSignal): Promise<void> {
    const rt = this.rt.get(localId)!;
    const blob = rt.file.slice((p - 1) * ps, Math.min(rt.file.size, p * ps));
    const checksum = await partChecksum(blob);
    for (let attempt = 0; ; attempt++) {
      const url = await this.partUrl(localId, fileId, p, n, signal);
      const t0 = now();
      try {
        const etag = await putBlob(url, blob, { checksum, signal, onProgress: (l) => {
          rt.inflight.set(p, l);
          this.progress(localId);
        } });
        rt.inflight.delete(p);
        rt.done.set(p, blob.size);
        this.bps.add(blob.size, now() - t0);
        this.progress(localId);
        this.api.reportPart(fileId, p, etag, blob.size).catch(() => {}); // optional: ListParts is the truth
        return;
      } catch (e) {
        rt.inflight.delete(p);
        if (isAbort(e)) throw e;
        if (e instanceof PutError && e.status === 403) rt.urls.delete(p); // expired URL: re-sign transparently
        if (attempt + 1 >= PART_RETRIES) {
          throw new UploadFailure(`Part ${p} couldn't be uploaded after ${PART_RETRIES} tries.`);
        }
        await this.waitBeforeRetry(localId, attempt, signal);
      }
    }
  }

  /** A valid presigned URL for part p, signing the next page of missing parts when needed. */
  private async partUrl(localId: string, fileId: string, p: number, n: number, signal: AbortSignal): Promise<string> {
    const rt = this.rt.get(localId)!;
    for (let round = 0; round < 3; round++) {
      const cached = rt.urls.get(p);
      if (cached && cached.expiresAt - 60_000 > Date.now()) return cached.url;
      if (rt.signing) {
        await rt.signing;
        continue;
      }
      const page: number[] = [];
      for (let k = p; k <= n && page.length < 100; k++) if (!rt.done.has(k)) page.push(k);
      rt.signing = this.api.signParts(fileId, page, { signal }).then((r) => {
        for (const [k, url] of Object.entries(r.urls)) rt.urls.set(Number(k), { url, expiresAt: r.expires_at * 1000 });
      }).finally(() => {
        rt.signing = null;
      });
      await rt.signing;
    }
    throw new UploadFailure(`Couldn't get an upload URL for part ${p}.`);
  }

  private async waitBeforeRetry(localId: string, attempt: number, signal: AbortSignal): Promise<void> {
    this.update(localId, { tone: "retrying", note: "Connection trouble — retrying…" });
    await sleep(backoffMs(attempt), signal);
    this.update(localId, { tone: "normal", note: undefined });
  }

  private progress(localId: string): void {
    const it = this.item(localId);
    const rt = this.rt.get(localId);
    if (!it || !rt) return;
    let bytes = 0;
    rt.done.forEach((b) => (bytes += b));
    rt.inflight.forEach((b) => (bytes += b));
    const t = now();
    if (t - rt.lastAt >= 500) {
      const inst = ((bytes - rt.lastBytes) * 1000) / (t - rt.lastAt);
      rt.speed = rt.speed ? 0.3 * inst + 0.7 * rt.speed : inst;
      rt.lastAt = t;
      rt.lastBytes = bytes;
    }
    const remaining = (it.size - bytes) / Math.max(1, rt.speed || this.bps.estimate())
      + (it.processingEstimateMs ?? DEFAULT_PROCESSING_MS) / 1000;
    this.update(localId, {
      uploadPct: it.size ? Math.min(100, (bytes * 100) / it.size) : 0,
      bytesPerSecond: rt.speed,
      etaSeconds: rt.eta?.update(remaining, t) ?? null,
    }, false);
  }

  // ------------------------------------------------------------------ user actions

  pause(localId: string): void {
    const it = this.item(localId);
    if (!it || !["queued", "uploading", "finishing"].includes(it.phase)) return;
    const rt = this.rt.get(localId);
    if (rt) {
      rt.paused = true;
      rt.abort?.abort();
      rt.inflight.clear();
    }
    this.update(localId, { phase: "paused", userPaused: true, tone: "paused", note: "Paused", etaSeconds: null });
  }

  resume(localId: string): void {
    const it = this.item(localId);
    if (!it || it.phase !== "paused") return;
    this.update(localId, { phase: "queued", userPaused: false, tone: "normal", note: undefined, prioritized: ++this.prio });
    this.pump();
  }

  pauseAll(): void {
    for (const it of this.state.items) this.pause(it.localId);
  }

  resumeAll(): void {
    for (const it of this.state.items) if (it.phase === "paused") this.resume(it.localId);
  }

  /** "Upload next": move a waiting file to the front of the queue. */
  prioritize(localId: string): void {
    const it = this.item(localId);
    if (it?.phase !== "queued") return;
    this.update(localId, { prioritized: ++this.prio });
  }

  async cancel(localId: string): Promise<void> {
    const it = this.item(localId);
    if (!it || FINAL.includes(it.phase)) return;
    const rt = this.rt.get(localId);
    if (rt) {
      rt.paused = true;
      rt.abort?.abort();
      rt.hash?.cancel();
      rt.inflight.clear();
    }
    if (it.phase === "staged" || it.phase === "checking") return this.remove(localId);
    this.update(localId, { phase: "cancelled", tone: "normal", note: "Cancelled", etaSeconds: null });
    if (it.fileId) await this.api.cancel(it.fileId).catch(() => {});
    if (it.batchId) this.maybeFinish(it.batchId);
  }

  /** Retry one failed file; completed stages and confirmed parts are kept. */
  async retry(localId: string): Promise<void> {
    const it = this.item(localId);
    if (!it || it.phase !== "error" || !it.error?.retryable) return;
    const rt = this.rt.get(localId);
    if (it.error.source === "client" || !it.fileId) {
      if (!rt) return;
      this.update(localId, { phase: "queued", error: undefined, tone: "normal", note: undefined, prioritized: ++this.prio });
      return this.pump();
    }
    try {
      const v = await this.api.retry(it.fileId);
      if (v.status === "pending") {
        // The upload itself failed (e.g. checksum): send this file again, from scratch.
        if (!rt) {
          this.update(localId, { error: { ...it.error, message: "Pick the file again to retry its upload." } });
          return;
        }
        rt.done.clear();
        rt.urls.clear();
        rt.start = undefined;
        rt.fresh = false;
        this.update(localId, { phase: "queued", error: undefined, tone: "normal", note: undefined, uploadPct: 0,
                               seq: v.seq, prioritized: ++this.prio });
        this.pump();
      } else {
        this.applyView(localId, v);
      }
      if (it.batchId) this.openStream(it.batchId);
    } catch (e) {
      this.update(localId, { error: { ...it.error, message: `Retry failed: ${messageOf(e)}` } });
    }
  }

  // ------------------------------------------------------------------ resume after a reload

  /** Find uploads interrupted by a closed tab or crash; the user re-picks the files to resume. */
  async loadInterrupted(): Promise<void> {
    try {
      const { files } = await this.api.unfinished();
      const known = new Set(this.state.items.map((i) => i.fileId));
      this.set({ interrupted: files.filter((f) => !known.has(f.id)).map((f) => ({
        fileId: f.id, batchId: f.batch_id, name: f.name, size: f.size_bytes, fingerprint: f.fingerprint })) });
    } catch {
      /* not fatal: the user simply isn't offered a resume */
    }
  }

  /** Match re-picked files to interrupted uploads by fingerprint (or name + size) and resume them. */
  async resumeWith(list: FileList | File[]): Promise<{ matched: string[]; unmatched: string[] }> {
    const matched: string[] = [];
    const unmatched: string[] = [];
    let interrupted = [...this.state.interrupted];
    for (const file of Array.from(list)) {
      const fp = await fingerprint(file);
      const hit = interrupted.find((x) => x.size === file.size && (x.fingerprint ? x.fingerprint === fp : x.name === file.name));
      if (!hit) {
        unmatched.push(file.name);
        continue;
      }
      interrupted = interrupted.filter((x) => x !== hit);
      const localId = newKey();
      this.rt.set(localId, newRuntime(file));
      const item: UploadItem = {
        localId, name: hit.name, size: file.size, mime: file.type, phase: "queued", tone: "normal", seq: 0,
        fileId: hit.fileId, batchId: hit.batchId, uploadPct: 0, processingPct: 0, overallPct: 0,
      };
      this.set({ items: [...this.state.items, item] });
      if (!this.state.batches[hit.batchId]) this.setBatch(hit.batchId, {});
      this.openStream(hit.batchId);
      matched.push(file.name);
    }
    this.set({ interrupted });
    this.pump();
    return { matched, unmatched };
  }

  /** Give up on interrupted uploads (cancels them on the server). */
  async discardInterrupted(fileId?: string): Promise<void> {
    const drop = this.state.interrupted.filter((x) => !fileId || x.fileId === fileId);
    this.set({ interrupted: this.state.interrupted.filter((x) => !drop.includes(x)) });
    await Promise.all(drop.map((x) => this.api.cancel(x.fileId).catch(() => {})));
  }

  // ------------------------------------------------------------------ network

  private setOnline(online: boolean): void {
    this.set({ online });
    for (const it of this.state.items) {
      const rt = this.rt.get(it.localId);
      if (!rt) continue;
      if (!online && ["uploading", "finishing"].includes(it.phase)) {
        // Pause in-flight parts now instead of waiting for timeouts; resume from the last confirmed part.
        rt.offlinePaused = true;
        rt.paused = true;
        rt.abort?.abort();
        rt.inflight.clear();
        this.update(it.localId, { phase: "paused", tone: "paused", note: "Offline — will resume when you reconnect" });
      } else if (online && it.phase === "paused" && rt.offlinePaused && !it.userPaused) {
        rt.offlinePaused = false;
        this.update(it.localId, { phase: "queued", tone: "normal", note: undefined, prioritized: ++this.prio });
      }
    }
    if (online) this.pump();
  }

  // ------------------------------------------------------------------ server progress

  private openStream(batchId: string): void {
    if (this.disposed || this.streams.has(batchId)) return;
    this.streams.set(batchId, new BatchStream((last) => this.api.eventsUrl(batchId, last),
      (type, data, seq) => this.onEvent(batchId, type, data, seq)));
  }

  private maybeFinish(batchId: string): void {
    const items = this.state.items.filter((i) => i.batchId === batchId);
    if (!items.length || !items.every((i) => FINAL.includes(i.phase))) return;
    this.streams.get(batchId)?.close();
    this.streams.delete(batchId);
    const summary = this.state.batches[batchId]?.summary;
    if (this.notify && !this.notified.has(batchId) && typeof document !== "undefined" && document.hidden
        && typeof Notification !== "undefined" && Notification.permission === "granted") {
      this.notified.add(batchId);
      new Notification("Upload finished", { body: summary?.text ?? `${items.length} files processed` });
    }
  }

  /** Fold a server file view (create response, snapshot, complete, retry) into an item. */
  private applyView(localId: string, v: ServerFileView): void {
    const it = this.item(localId);
    if (!it || v.seq < it.seq) return;
    const base: Partial<UploadItem> = {
      seq: Math.max(it.seq, v.seq), fileId: v.id, batchId: v.batch_id, dedupOf: v.dedup_of,
      processingEstimateMs: v.processing_estimate_ms ?? it.processingEstimateMs,
    };
    switch (v.status) {
      case "pending":
      case "uploading": // the client owns the file until it completes the upload
        this.update(localId, ["staged", "checking", "queued"].includes(it.phase) ? { ...base, phase: "queued" } : base);
        return;
      case "uploaded":
      case "processing":
      case "failed_retryable":
      case "waiting_vendor":
        this.update(localId, {
          ...base, phase: "processing", stage: v.stage, uploadPct: 100, error: undefined,
          processingPct: Math.max(it.processingPct, v.processing_pct), weights: it.weights ?? this.weightsFor(it),
          tone: v.status === "failed_retryable" ? "retrying" : v.status === "waiting_vendor" ? "waiting" : "normal",
          note: v.status === "failed_retryable" ? "Retrying…"
            : v.status === "waiting_vendor" ? "Waiting for a service — will resume automatically" : stageLabel(v.stage),
        });
        return;
      case "completed":
        this.update(localId, { ...base, phase: "done", uploadPct: 100, processingPct: 100, error: undefined,
                               note: undefined, retryAt: undefined });
        break;
      case "failed":
        this.update(localId, { ...base, phase: "error", tone: "error", note: undefined, error: {
          code: v.error?.code ?? "FAILED", message: v.error?.message ?? "This file couldn't be processed.",
          retryable: Boolean(v.error?.retryable_by_user), fix: v.error?.fix, source: "server" } });
        break;
      case "cancelled":
        this.update(localId, { ...base, phase: "cancelled", tone: "normal", note: "Cancelled" });
        break;
    }
    if (v.batch_id) this.maybeFinish(v.batch_id);
  }

  private onEvent(batchId: string, type: string, data: Record<string, unknown>, seq: number): void {
    if (type === "snapshot") {
      const view = data as unknown as ServerBatchView;
      for (const f of view.files) {
        const it = this.byFileId(f.id);
        if (it) this.applyView(it.localId, f);
      }
      this.setBatch(batchId, { summary: view.summary, seq: view.seq });
      return this.maybeFinish(batchId);
    }
    if (type === "batch.summary") {
      this.setBatch(batchId, { summary: data as unknown as BatchInfo["summary"], seq });
      return this.maybeFinish(batchId);
    }
    const it = this.byFileId(data.file_id);
    if (!it || seq <= it.seq) return; // replayed or out of order: already applied
    const id = it.localId;
    switch (type) {
      case "file.stage": {
        const queuedAtVendor = data.vendor_status === "queued";
        this.update(id, {
          seq, phase: "processing", uploadPct: 100, stage: String(data.stage), error: undefined, retryAt: undefined,
          processingPct: Math.max(it.processingPct, Number(data.processing_pct ?? 0)),
          weights: it.weights ?? this.weightsFor(it), tone: "normal",
          note: queuedAtVendor ? `Queued at ${vendorLabel("processor")}${data.queue_position ? ` (#${data.queue_position})` : ""}`
            : stageLabel(String(data.stage)),
        });
        break;
      }
      case "file.retrying":
        this.update(id, { seq, phase: "processing", tone: "retrying", retryAt: Number(data.next_retry_at),
                          note: `Retrying (${data.attempt} of ${data.max_attempts})` });
        break;
      case "file.waiting":
        this.update(id, { seq, phase: "processing", tone: "waiting", retryAt: undefined,
                          note: `Waiting for ${vendorLabel(String(data.vendor))} — will resume automatically` });
        break;
      case "file.completed":
        this.update(id, { seq, phase: "done", uploadPct: 100, processingPct: 100, error: undefined, note: undefined,
                          retryAt: undefined });
        break;
      case "file.failed":
        this.rt.get(id)?.abort?.abort();
        this.update(id, { seq, phase: "error", tone: "error", note: undefined, retryAt: undefined, etaSeconds: null,
                          error: { code: String(data.error_code), message: String(data.message),
                                   retryable: Boolean(data.retryable_by_user),
                                   fix: (data.fix as FileError["fix"]) ?? null, source: "server" } });
        break;
      case "file.cancelled":
        this.update(id, { seq, phase: "cancelled", tone: "normal", note: "Cancelled" });
        break;
    }
    this.maybeFinish(batchId);
  }
}
