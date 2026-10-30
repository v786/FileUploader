import { describe, expect, it } from "vitest";
import { describe as describeItem, matchesFilter } from "../labels";
import type { UploadItem } from "../types";

const item = (over: Partial<UploadItem>): UploadItem => ({
  localId: "l1", name: "a.txt", size: 10, mime: "text/plain", phase: "processing", tone: "normal",
  seq: 0, uploadPct: 100, processingPct: 0, overallPct: 50, ...over,
});

describe("describe", () => {
  it("names the processing stage in plain language", () => {
    expect(describeItem(item({ stage: "scan" })).label).toBe("Scanning for viruses");
    expect(describeItem(item({ stage: "unknown" })).label).toBe("Processing");
  });

  it("counts down to the next retry", () => {
    const it = item({ tone: "retrying", note: "Retrying (2 of 6)", retryAt: 108 });
    expect(describeItem(it, 100).label).toBe("Retrying (2 of 6) · next try in 8s");
    expect(describeItem(it, 200).label).toBe("Retrying (2 of 6)");
  });

  it("shows upload percent and the error message", () => {
    expect(describeItem(item({ phase: "uploading", uploadPct: 42.7 })).label).toBe("Uploading · 42%");
    expect(describeItem(item({ phase: "error", error: { code: "X", message: "Bad", retryable: true, source: "server" } })))
      .toEqual({ label: "Needs attention", detail: "Bad" });
    expect(describeItem(item({ phase: "done", dedupOf: "f0" })).label).toMatch(/reused/);
  });
});

describe("matchesFilter", () => {
  it("groups phases", () => {
    expect(matchesFilter(item({ phase: "uploading" }), "active")).toBe(true);
    expect(matchesFilter(item({ phase: "invalid" }), "attention")).toBe(true);
    expect(matchesFilter(item({ phase: "cancelled" }), "active")).toBe(false);
    expect(matchesFilter(item({ phase: "done" }), "done")).toBe(true);
  });
});
