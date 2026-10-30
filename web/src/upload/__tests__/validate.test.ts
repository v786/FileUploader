import { describe, expect, it } from "vitest";
import { DEFAULT_LIMITS, clientError, contentMatches, ext, validate } from "../validate";

const enc = (s: string) => new TextEncoder().encode(s);
const PNG = Uint8Array.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0, 0]);
const ZIP = Uint8Array.from([0x50, 0x4b, 0x03, 0x04]);

describe("contentMatches", () => {
  it("checks magic bytes for binary types", () => {
    expect(contentMatches("a.png", PNG)).toBe(true);
    expect(contentMatches("a.png", enc("hello"))).toBe(false);
    expect(contentMatches("a.pdf", enc("%PDF-1.7"))).toBe(true);
    expect(contentMatches("a.docx", ZIP)).toBe(true);
    expect(contentMatches("a.webp", enc("RIFF\0\0\0\0WEBPVP8 "))).toBe(true);
    expect(contentMatches("a.mp4", enc("\0\0\0\x18ftypmp42"))).toBe(true);
  });

  it("accepts text but rejects a binary disguised as text", () => {
    expect(contentMatches("notes.txt", enc("hello"))).toBe(true);
    expect(contentMatches("notes.txt", ZIP)).toBe(false);
    expect(contentMatches("data.json", PNG)).toBe(false);
  });

  it("is case-insensitive on the extension", () => {
    expect(ext("Photo.PNG")).toBe(".png");
    expect(ext(".bashrc")).toBe("");
    expect(contentMatches("Photo.PNG", PNG)).toBe(true);
  });
});

describe("validate", () => {
  const c = (name: string, head: Uint8Array = enc("text"), size = 10) => ({ name, size, head });

  it("returns one code per file, null for OK", () => {
    expect(validate([c("a.txt"), c("b.exe"), c("big.txt", enc("x"), 6 * 1024 ** 3), c("fake.png"), c(" ")], new Set(), 0))
      .toEqual([null, "UNSUPPORTED_TYPE", "FILE_TOO_LARGE", "TYPE_MISMATCH", "EMPTY_NAME"]);
  });

  it("catches duplicate names within the new files and against the existing selection", () => {
    expect(validate([c("a.txt"), c("A.TXT"), c("b.txt")], new Set(["b.txt"]), 1))
      .toEqual([null, "DUPLICATE_NAME", "DUPLICATE_NAME"]);
  });

  it("only counts accepted files toward the limit", () => {
    const limits = { ...DEFAULT_LIMITS, maxFiles: 2 };
    expect(validate([c("x.exe"), c("a.txt"), c("b.txt"), c("c.txt")], new Set(), 0, limits))
      .toEqual(["UNSUPPORTED_TYPE", null, null, "TOO_MANY_FILES"]);
  });

  it("gives each error a message and a fix", () => {
    const e = clientError("TYPE_MISMATCH");
    expect(e).toMatchObject({ source: "client", retryable: false, fix: { action: "replace" } });
    expect(e.message).toMatch(/don't match/);
  });
});
