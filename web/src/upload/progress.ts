/**
 * The combined progress bar: one bar per file, upload then processing.
 *
 *   overall = w_upload × upload_pct + w_process × processing_pct
 *   w_upload = est_upload_s / (est_upload_s + est_processing_s)
 *
 * The client measures its own upload throughput; the server sends the processing
 * estimate (p50 for this file type and size). Weights are fixed when the upload
 * starts so the bar never jumps, and the bar never moves backwards.
 */

export const DEFAULT_BYTES_PER_SECOND = 2.5e6; // ~20 Mbps until we've measured
export const DEFAULT_PROCESSING_MS = 20_000;

export function uploadWeights(sizeBytes: number, bytesPerSecond: number,
                              processingEstimateMs?: number | null): { upload: number; process: number } {
  const up = sizeBytes / Math.max(1, bytesPerSecond);
  const proc = (processingEstimateMs ?? DEFAULT_PROCESSING_MS) / 1000;
  const w = up + proc > 0 ? up / (up + proc) : 0;
  return { upload: w, process: 1 - w };
}

export function overallPct(prev: number, w: { upload: number; process: number }, uploadPct: number,
                           processingPct: number, completed: boolean): number {
  if (completed) return 100;
  const raw = w.upload * uploadPct + w.process * processingPct;
  return Math.max(prev, Math.min(99, Math.round(raw * 10) / 10)); // 100% only on file.completed
}

/** Moving average of upload throughput over the first parts (shared across files). */
export class Throughput {
  private samples: number[] = [];
  constructor(private keep = 2) {}

  add(bytes: number, ms: number): void {
    if (ms <= 0 || bytes <= 0) return;
    this.samples.push((bytes * 1000) / ms);
    if (this.samples.length > this.keep) this.samples.shift();
  }

  estimate(): number {
    if (!this.samples.length) return DEFAULT_BYTES_PER_SECOND;
    return this.samples.reduce((a, b) => a + b, 0) / this.samples.length;
  }
}

/** Smooth ETAs: shown only after 3 s of data, smoothed, rounded to friendly steps. */
export class EtaSmoother {
  private value: number | null = null;
  constructor(private readonly startedAt: number, private readonly alpha = 0.3) {}

  update(rawSeconds: number, now: number): number | null {
    if (!Number.isFinite(rawSeconds) || rawSeconds < 0) return this.value;
    this.value = this.value === null ? rawSeconds : this.alpha * rawSeconds + (1 - this.alpha) * this.value;
    return now - this.startedAt >= 3000 ? this.value : null;
  }
}

export function friendlyEta(seconds: number | null | undefined): string | null {
  if (seconds === null || seconds === undefined || !Number.isFinite(seconds)) return null;
  if (seconds < 5) return "a few seconds left";
  if (seconds < 60) return `~${Math.max(5, Math.round(seconds / 5) * 5)} s left`;
  if (seconds < 3600) return `about ${Math.round(seconds / 60)} min left`;
  return `about ${Math.round(seconds / 360) / 10} h left`;
}

export function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v < 10 ? v.toFixed(1) : Math.round(v)} ${units[i]}`;
}
