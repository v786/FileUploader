import { createHash, randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";
import { fingerprint, partChecksum } from "../hash";
import { Sha256, toBase64, toHex } from "../sha256";

const nodeHex = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");

describe("Sha256", () => {
  it("matches known vectors", () => {
    expect(toHex(new Sha256().digest())).toBe("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    expect(toHex(new Sha256().update(new TextEncoder().encode("abc")).digest()))
      .toBe("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  });

  it.each([0, 1, 55, 56, 63, 64, 65, 1000, 100_003])("matches node crypto for %i bytes fed in odd chunks", (n) => {
    const data = new Uint8Array(randomBytes(n));
    const h = new Sha256();
    for (let i = 0; i < n; i += 37) h.update(data.subarray(i, i + 37));
    expect(toHex(h.digest())).toBe(nodeHex(data));
  });

  it("base64-encodes like S3's checksum header expects", async () => {
    const data = new Uint8Array(randomBytes(5000));
    expect(await partChecksum(new Blob([data]))).toBe(createHash("sha256").update(data).digest("base64"));
    expect(toBase64(Uint8Array.from([0xff, 0x00, 0x10]))).toBe("/wAQ");
  });
});

describe("fingerprint", () => {
  it("depends on size, head and tail but not the middle", async () => {
    const MB = 1024 * 1024;
    const a = new Uint8Array(3 * MB);
    const b = a.slice();
    b[Math.floor(1.5 * MB)] = 1; // middle byte
    const c = a.slice();
    c[c.length - 1] = 1; // tail byte
    const fa = await fingerprint(new Blob([a]));
    expect(fa).toMatch(/^3145728-[0-9a-f]{48}$/);
    expect(await fingerprint(new Blob([b]))).toBe(fa);
    expect(await fingerprint(new Blob([c]))).not.toBe(fa);
  });
});
