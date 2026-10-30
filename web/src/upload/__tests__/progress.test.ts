import { describe, expect, it } from "vitest";
import { choosePartSize } from "../uploader";
import { EtaSmoother, Throughput, formatBytes, friendlyEta, overallPct, uploadWeights } from "../progress";

const MB = 1024 * 1024;

describe("combined progress", () => {
  it("weights upload and processing by expected duration", () => {
    const w = uploadWeights(100 * MB, 10 * MB, 10_000); // 10 s up, 10 s processing
    expect(w.upload).toBeCloseTo(0.5);
    expect(w.process).toBeCloseTo(0.5);
    expect(overallPct(0, w, 100, 0, false)).toBe(50);
    expect(overallPct(0, w, 100, 50, false)).toBe(75);
  });

  it("falls back to a default processing estimate", () => {
    expect(uploadWeights(1, 1e9, undefined).process).toBeGreaterThan(0.99);
  });

  it("never goes backwards and stays below 100 until completed", () => {
    const w = { upload: 0.5, process: 0.5 };
    expect(overallPct(60, w, 100, 0, false)).toBe(60);
    expect(overallPct(0, w, 100, 100, false)).toBe(99);
    expect(overallPct(10, w, 0, 0, true)).toBe(100);
  });
});

describe("throughput and ETA", () => {
  it("averages the last samples", () => {
    const t = new Throughput(2);
    t.add(1000, 1000);
    t.add(2000, 1000);
    t.add(4000, 1000);
    expect(t.estimate()).toBe(3000);
    t.add(0, 1000); // ignored
    expect(t.estimate()).toBe(3000);
  });

  it("hides the ETA for the first 3 s, then smooths it", () => {
    const e = new EtaSmoother(0, 0.5);
    expect(e.update(100, 1000)).toBeNull();
    expect(e.update(50, 3000)).toBe(75);
    expect(e.update(Number.NaN, 4000)).toBe(75);
  });

  it("formats in friendly steps", () => {
    expect(friendlyEta(null)).toBeNull();
    expect(friendlyEta(3)).toBe("a few seconds left");
    expect(friendlyEta(22)).toBe("~20 s left");
    expect(friendlyEta(150)).toBe("about 3 min left");
    expect(friendlyEta(5400)).toBe("about 1.5 h left");
    expect(formatBytes(512)).toBe("512 B");
    expect(formatBytes(1536)).toBe("1.5 KB");
    expect(formatBytes(200 * MB)).toBe("200 MB");
  });
});

describe("choosePartSize", () => {
  it("uses a single PUT below 8 MB and grows parts with size and speed", () => {
    expect(choosePartSize(7 * MB, 1e6)).toBeUndefined();
    expect(choosePartSize(100 * MB, 1e6)).toBe(8 * MB);
    expect(choosePartSize(100 * MB, 20e6)).toBe(16 * MB);
    expect(choosePartSize(2 * 1024 * MB, 1e6)).toBe(16 * MB);
    expect(choosePartSize(5 * 1024 * MB, 20e6)).toBe(64 * MB);
  });

  it("never needs more than 10,000 parts", () => {
    const size = 5 * 1024 * MB;
    expect(Math.ceil(size / choosePartSize(size, 0)!)).toBeLessThanOrEqual(10_000);
  });
});
