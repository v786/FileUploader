/// <reference lib="webworker" />
// Full-file SHA-256 off the UI thread, read in order while the parts upload in parallel.
import { Sha256, toHex } from "./sha256";

const CHUNK = 4 * 1024 * 1024;

self.onmessage = async (e: MessageEvent<{ file: Blob }>) => {
  const { file } = e.data;
  const h = new Sha256();
  try {
    for (let off = 0; off < file.size; off += CHUNK) {
      h.update(new Uint8Array(await file.slice(off, off + CHUNK).arrayBuffer()));
      self.postMessage({ type: "progress", done: Math.min(file.size, off + CHUNK) });
    }
    self.postMessage({ type: "done", sha256: toHex(h.digest()) });
  } catch (err) {
    self.postMessage({ type: "error", message: String(err) });
  }
};
