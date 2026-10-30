/**
 * Sample files generated in the browser, so the demo can show every path without
 * the user hunting for test files: normal files, a large multipart upload, and
 * files that fail in each way (wrong type, disguised type, malware).
 */
const MB = 1024 * 1024;

// The standard EICAR antivirus test string: harmless, but every scanner flags it.
const EICAR = "X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*";

function randomBytes(n: number): Uint8Array {
  const out = new Uint8Array(n);
  for (let i = 0; i < n; i += 65536) crypto.getRandomValues(out.subarray(i, Math.min(n, i + 65536)));
  return out;
}

async function png(label: string, hue: number): Promise<File> {
  const c = document.createElement("canvas");
  c.width = 480;
  c.height = 320;
  const g = c.getContext("2d")!;
  const grad = g.createLinearGradient(0, 0, 480, 320);
  grad.addColorStop(0, `hsl(${hue} 70% 55%)`);
  grad.addColorStop(1, `hsl(${(hue + 60) % 360} 70% 40%)`);
  g.fillStyle = grad;
  g.fillRect(0, 0, 480, 320);
  g.fillStyle = "rgba(255,255,255,.9)";
  g.font = "600 36px system-ui, sans-serif";
  g.fillText(label, 32, 176);
  const blob = await new Promise<Blob>((resolve) => c.toBlob((b) => resolve(b!), "image/png"));
  return new File([blob], `${label.toLowerCase().replace(/\s+/g, "-")}.png`, { type: "image/png" });
}

export async function goodSamples(): Promise<File[]> {
  const pdfBody = "%PDF-1.4\n% demo document\n" + "Lorem ipsum dolor sit amet. ".repeat(8000) + "\n%%EOF\n";
  const csv = "id,name,amount\n" + Array.from({ length: 4000 }, (_, i) => `${i},row ${i},${(i * 7) % 100}`).join("\n");
  const zip = new Uint8Array(24 * MB);
  zip.set([0x50, 0x4b, 0x03, 0x04]);
  zip.set(randomBytes(24 * MB - 4), 4);
  return [
    new File(["Meeting notes\n".repeat(200)], "meeting-notes.txt", { type: "text/plain" }),
    new File([pdfBody], "quarterly-report.pdf", { type: "application/pdf" }),
    new File([csv], "sales.csv", { type: "text/csv" }),
    await png("Team photo", 210),
    await png("Whiteboard", 30),
    new File([zip], "project-archive.zip", { type: "application/zip" }),
  ];
}

export function problemSamples(): File[] {
  return [
    new File(["this is really just text"], "holiday.png", { type: "image/png" }), // contents don't match the type
    new File([new Uint8Array([0x4d, 0x5a, 0x90, 0x00])], "installer.exe"),       // type not allowed
    new File([EICAR], "eicar-test.txt", { type: "text/plain" }),                 // flagged by the virus scanner
  ];
}

export function largeSample(mb = 200): File {
  const data = new Uint8Array(mb * MB);
  data.set([0x50, 0x4b, 0x03, 0x04]);
  data.set(randomBytes(4 * MB), 4); // the rest stays zero: fast to make, still a real multipart upload
  return new File([data], `large-${mb}mb.zip`, { type: "application/zip" });
}
