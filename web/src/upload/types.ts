/** Public types of the upload module. */

/**
 * Client-side phase of one file. The server's own status is folded into these:
 *
 *   checking → staged | invalid                      (selected, validated in the browser)
 *   staged → queued → uploading → finishing → processing → done
 *   any active phase → paused (user, offline) → queued
 *   uploading | finishing | processing → error        (needs attention)
 *   any non-final phase → cancelled
 */
export type Phase =
  | "checking"
  | "staged"
  | "invalid"
  | "queued"
  | "uploading"
  | "paused"
  | "finishing"
  | "processing"
  | "done"
  | "error"
  | "cancelled";

/** How the row should look while it is not plainly progressing. */
export type Tone = "normal" | "retrying" | "waiting" | "paused" | "error" | "done";

export interface Fix {
  action: string; // convert | replace | split | rename | free_space | remove | retry
  label: string;
}

export interface FileError {
  code: string;
  message: string; // safe to show to the user
  /** True when a Retry button makes sense. */
  retryable: boolean;
  fix?: Fix | null;
  /** client: the browser couldn't upload it. server: the service rejected or failed it. */
  source: "client" | "server";
}

export interface UploadItem {
  localId: string; // also sent as client_file_id
  name: string;
  size: number;
  mime: string;
  previewUrl?: string;
  phase: Phase;
  tone: Tone;
  /** Short status line, e.g. "Retrying (2 of 6)" or "Waiting for scanner — will resume automatically". */
  note?: string;
  /** Unix seconds; lets the UI count down "Retrying in 8s". */
  retryAt?: number;
  stage?: string | null;
  error?: FileError;

  // server identity
  fileId?: string;
  batchId?: string;
  seq: number; // newest event applied (events with seq <= this are ignored)

  // progress: the combined bar is upload then processing, weighted by their expected durations
  uploadPct: number;
  processingPct: number;
  overallPct: number; // never moves backwards; 100 only when done
  weights?: { upload: number; process: number };
  processingEstimateMs?: number;
  bytesPerSecond?: number;
  etaSeconds?: number | null;

  dedupeCandidate?: boolean;
  dedupOf?: string | null;
  prioritized?: number; // "Upload next": larger = sooner
  userPaused?: boolean;
}

export interface BatchSummary {
  total: number;
  counts: Record<string, number>;
  done: number;
  need_attention: number;
  text: string;
}

export interface BatchInfo {
  id: string;
  summary?: BatchSummary;
  seq: number;
  createdAt: number;
}

/** A file whose upload was interrupted (tab closed, crash): re-pick it to resume. */
export interface InterruptedFile {
  fileId: string;
  batchId: string;
  name: string;
  size: number;
  fingerprint: string | null;
}

export interface UploaderState {
  items: UploadItem[];
  batches: Record<string, BatchInfo>;
  online: boolean;
  starting: boolean;
  /** Top-level notice, e.g. "Busy — your upload will start shortly". */
  notice?: { tone: "info" | "error"; text: string } | null;
  interrupted: InterruptedFile[];
}

export interface Limits {
  maxFiles: number;
  maxFileSize: number;
  allowedExtensions: string[];
  quotaBytes?: number;
}

export interface UploaderOptions {
  /** Base URL of the upload API ("" = same origin). */
  baseUrl?: string;
  /** Sent as X-User-Id (the backend's mock auth). */
  userId: string;
  /** Files uploading at once (default 3) and parts per file (default 4). */
  fileConcurrency?: number;
  partConcurrency?: number;
  limits?: Partial<Limits>;
  /** Browser notification when a batch finishes while the tab is hidden. */
  notify?: boolean;
}

// ---- server shapes (subset) ----

export interface ServerFileView {
  id: string;
  batch_id: string;
  client_file_id: string;
  name: string;
  size_bytes: number;
  sha256: string | null;
  fingerprint: string | null;
  status: string;
  stage: string | null;
  progress_pct: number;
  upload_pct: number;
  processing_pct: number;
  processing_estimate_ms: number | null;
  seq: number;
  upload: {
    mode: "single" | "multipart";
    part_size: number;
    part_count: number;
    dedupe_candidate: boolean;
    confirmed_bytes?: number;
    start?: StartInfo;
  };
  error: { code: string; message: string; retryable_by_user: boolean; fix: Fix | null } | null;
  dedup_of: string | null;
  result_url?: string;
}

export interface StartInfo {
  mode: "single" | "multipart";
  url?: string;
  upload_id?: string;
  urls?: Record<string, string>;
  expires_at?: number;
  part_size: number;
  part_count: number;
}

export interface ServerBatchView {
  id: string;
  seq: number;
  summary: BatchSummary;
  files: ServerFileView[];
}

export interface FileResult {
  file: ServerFileView;
  result: Record<string, unknown>;
  download_url: string;
  thumbnail_url: string | null;
  process_result_url: string | null;
}
