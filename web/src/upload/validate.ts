/**
 * Fail fast, before any bytes: type by magic bytes (not just extension), size, count and
 * duplicate names. The server checks all of these again; this just saves the user a wait.
 * The rules mirror fileuploader/filetypes.py.
 */
import type { FileError, Limits } from "./types";

const GB = 1024 ** 3;

export const DEFAULT_LIMITS: Limits = {
  maxFiles: 50,
  maxFileSize: 5 * GB,
  allowedExtensions: [
    ".pdf", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".txt", ".csv",
    ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".zip", ".mp4", ".mov", ".json",
  ],
};

const bytes = (...xs: number[]) => Uint8Array.from(xs);
const ascii = (s: string) => Uint8Array.from(s, (c) => c.charCodeAt(0));
const ZIP = [bytes(0x50, 0x4b, 0x03, 0x04), bytes(0x50, 0x4b, 0x05, 0x06)];
const OLE = [bytes(0xd0, 0xcf, 0x11, 0xe0, 0xa1, 0xb1, 0x1a, 0xe1)];

export const MAGIC: Record<string, Uint8Array[]> = {
  ".pdf": [ascii("%PDF")],
  ".png": [bytes(0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a)],
  ".jpg": [bytes(0xff, 0xd8, 0xff)],
  ".jpeg": [bytes(0xff, 0xd8, 0xff)],
  ".gif": [ascii("GIF87a"), ascii("GIF89a")],
  ".zip": ZIP, ".docx": ZIP, ".xlsx": ZIP, ".pptx": ZIP,
  ".doc": OLE, ".xls": OLE, ".ppt": OLE,
};
const TEXT = new Set([".txt", ".csv", ".json"]);
const ISO_BOXES = ["ftyp", "moov", "mdat", "wide", "free"];

export function ext(name: string): string {
  const i = name.lastIndexOf(".");
  return i > 0 ? name.slice(i).toLowerCase() : "";
}

const startsWith = (head: Uint8Array, m: Uint8Array) => m.every((b, i) => head[i] === b);
const str = (head: Uint8Array, a: number, b: number) => String.fromCharCode(...head.slice(a, b));
const isWebp = (h: Uint8Array) => str(h, 0, 4) === "RIFF" && str(h, 8, 12) === "WEBP";
const isIso = (h: Uint8Array) => ISO_BOXES.includes(str(h, 4, 8));

function knownBinary(head: Uint8Array): boolean {
  return Object.values(MAGIC).some((ms) => ms.some((m) => startsWith(head, m))) || isWebp(head) || isIso(head);
}

/** True if the first bytes are consistent with the extension. */
export function contentMatches(name: string, head: Uint8Array): boolean {
  const e = ext(name);
  if (MAGIC[e]) return MAGIC[e].some((m) => startsWith(head, m));
  if (e === ".webp") return isWebp(head);
  if (e === ".mp4" || e === ".mov") return isIso(head);
  if (TEXT.has(e)) return !knownBinary(head); // text has no magic; reject a binary in disguise
  return true;
}

const MESSAGES: Record<string, string> = {
  FILE_TOO_LARGE: "This file is larger than the 5 GB limit.",
  UNSUPPORTED_TYPE: "This file type isn't supported.",
  TYPE_MISMATCH: "This file's contents don't match its type.",
  DUPLICATE_NAME: "Another file in this upload has the same name.",
  TOO_MANY_FILES: "You can upload up to 50 files at once.",
  EMPTY_NAME: "This file has no name.",
};
const FIXES: Record<string, { action: string; label: string }> = {
  FILE_TOO_LARGE: { action: "split", label: "Split into parts under 5 GB" },
  UNSUPPORTED_TYPE: { action: "convert", label: "Convert to PDF" },
  TYPE_MISMATCH: { action: "replace", label: "Replace file" },
  DUPLICATE_NAME: { action: "rename", label: "Rename file" },
  TOO_MANY_FILES: { action: "remove", label: "Upload in a separate batch" },
};

export function clientError(code: string): FileError {
  return { code, message: MESSAGES[code] ?? "This file can't be uploaded.", retryable: false, fix: FIXES[code], source: "client" };
}

export interface Candidate {
  name: string;
  size: number;
  head: Uint8Array;
}

/**
 * Validate new files against the ones already selected. Returns an error code per
 * candidate (null = OK). `takenNames` are the names already in this selection, lower-cased.
 */
export function validate(candidates: Candidate[], takenNames: Set<string>, alreadySelected: number,
                         limits: Limits = DEFAULT_LIMITS): (string | null)[] {
  const names = new Set(takenNames);
  let count = alreadySelected;
  return candidates.map((c) => {
    let code: string | null = null;
    if (!c.name.trim()) code = "EMPTY_NAME";
    else if (!limits.allowedExtensions.includes(ext(c.name))) code = "UNSUPPORTED_TYPE";
    else if (c.size > limits.maxFileSize) code = "FILE_TOO_LARGE";
    else if (!contentMatches(c.name, c.head)) code = "TYPE_MISMATCH";
    else if (names.has(c.name.toLowerCase())) code = "DUPLICATE_NAME";
    else if (count >= limits.maxFiles) code = "TOO_MANY_FILES";
    if (code === null) {
      names.add(c.name.toLowerCase());
      count += 1;
    }
    return code;
  });
}

export function toHexString(head: Uint8Array): string {
  return Array.from(head, (b) => b.toString(16).padStart(2, "0")).join("");
}
