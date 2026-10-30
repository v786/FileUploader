import { Sha256, toBase64, toHex } from "./sha256";

const MB = 1024 * 1024;

export interface HashJob {
  promise: Promise<string>;
  cancel(): void;
}

/** Full SHA-256 of a file in a Web Worker (inline where workers don't exist, e.g. tests). */
export function hashFile(file: Blob, onProgress?: (doneBytes: number) => void): HashJob {
  if (typeof Worker === "undefined") {
    let cancelled = false;
    const promise = (async () => {
      const h = new Sha256();
      for (let off = 0; off < file.size; off += 4 * MB) {
        if (cancelled) throw new DOMException("cancelled", "AbortError");
        h.update(new Uint8Array(await file.slice(off, off + 4 * MB).arrayBuffer()));
        onProgress?.(Math.min(file.size, off + 4 * MB));
      }
      return toHex(h.digest());
    })();
    return { promise, cancel: () => (cancelled = true) };
  }
  const worker = new Worker(new URL("./hash.worker.ts", import.meta.url), { type: "module" });
  let reject: (e: unknown) => void = () => {};
  const promise = new Promise<string>((resolve, rej) => {
    reject = rej;
    worker.onmessage = (e) => {
      const m = e.data;
      if (m.type === "progress") onProgress?.(m.done);
      else {
        worker.terminate();
        if (m.type === "done") resolve(m.sha256);
        else rej(new Error(m.message));
      }
    };
    worker.onerror = (e) => {
      worker.terminate();
      rej(new Error(e.message));
    };
    worker.postMessage({ file });
  });
  return {
    promise,
    cancel: () => {
      worker.terminate();
      reject(new DOMException("cancelled", "AbortError"));
    },
  };
}

async function sha256Bytes(data: Uint8Array): Promise<Uint8Array> {
  if (globalThis.crypto?.subtle) return new Uint8Array(await crypto.subtle.digest("SHA-256", data as BufferSource));
  return new Sha256().update(data).digest();
}

/** Per-part checksum, sent as x-amz-checksum-sha256 so storage rejects a corrupted part. */
export async function partChecksum(blob: Blob): Promise<string> {
  return toBase64(await sha256Bytes(new Uint8Array(await blob.arrayBuffer())));
}

/**
 * Quick fingerprint for pre-upload dedupe and for matching a re-picked file after a
 * reload: size + hash of the first and last 1 MB. Takes milliseconds even for 5 GB.
 */
export async function fingerprint(file: Blob): Promise<string> {
  const head = new Uint8Array(await file.slice(0, MB).arrayBuffer());
  const tail = new Uint8Array(await file.slice(Math.max(MB, file.size - MB)).arrayBuffer());
  const both = new Uint8Array(head.length + tail.length);
  both.set(head);
  both.set(tail, head.length);
  return `${file.size}-${toHex(await sha256Bytes(both)).slice(0, 48)}`;
}

export async function readHead(file: Blob, n = 32): Promise<Uint8Array> {
  return new Uint8Array(await file.slice(0, n).arrayBuffer());
}
