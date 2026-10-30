/** Plain-language labels for what a file is doing. Usable by any UI, not just ours. */
import { formatBytes, friendlyEta } from "./progress";
import type { UploadItem } from "./types";

const STAGES: Record<string, string> = {
  uploaded: "Queued for processing",
  scan: "Scanning for viruses",
  extract: "Reading metadata",
  thumbnail: "Making a preview",
  process: "Processing",
  finalize: "Finishing up",
};

const VENDORS: Record<string, string> = {
  scanner: "the virus scanner",
  extractor: "the metadata service",
  thumbnailer: "the preview service",
  processor: "the processor",
};

export const stageLabel = (stage: string | null | undefined) => (stage && STAGES[stage]) || "Processing";
export const vendorLabel = (vendor: string) => VENDORS[vendor] ?? vendor;

export type Filter = "all" | "active" | "attention" | "done";

export function matchesFilter(it: UploadItem, f: Filter): boolean {
  switch (f) {
    case "all":
      return true;
    case "attention":
      return it.phase === "error" || it.phase === "invalid";
    case "done":
      return it.phase === "done";
    case "active":
      return !["done", "error", "invalid", "cancelled"].includes(it.phase);
  }
}

/** One line of status (and an optional detail line) for a file, as of `nowSeconds`. */
export function describe(it: UploadItem, nowSeconds = Date.now() / 1000): { label: string; detail?: string } {
  switch (it.phase) {
    case "checking":
      return { label: "Checking…" };
    case "staged":
      return { label: "Ready to upload" };
    case "invalid":
      return { label: "Can't be uploaded", detail: it.error?.message };
    case "queued":
      return { label: it.note ?? "Waiting to upload" };
    case "uploading": {
      const speed = it.bytesPerSecond ? `${formatBytes(it.bytesPerSecond)}/s` : undefined;
      return {
        label: it.note ?? `Uploading · ${Math.floor(it.uploadPct)}%`,
        detail: [friendlyEta(it.etaSeconds), speed].filter(Boolean).join(" · ") || undefined,
      };
    }
    case "paused":
      return { label: it.note ?? "Paused" };
    case "finishing":
      return { label: it.note ?? "Verifying…" };
    case "processing": {
      if (it.tone === "retrying" && it.retryAt) {
        const s = Math.ceil(it.retryAt - nowSeconds);
        return { label: s > 0 ? `${it.note} · next try in ${s}s` : it.note ?? "Retrying…" };
      }
      const est = it.processingEstimateMs;
      const eta = it.tone === "normal" && est ? friendlyEta((est / 1000) * (1 - it.processingPct / 100)) : null;
      return { label: it.note ?? stageLabel(it.stage), detail: eta ?? undefined };
    }
    case "done":
      return { label: it.dedupOf ? "Done · reused an earlier upload" : "Done" };
    case "error":
      return { label: "Needs attention", detail: it.error?.message };
    case "cancelled":
      return { label: "Cancelled" };
  }
}
