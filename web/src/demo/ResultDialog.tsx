import { useEffect, useRef, useState } from "react";
import { formatBytes, useUploader } from "../upload";
import type { FileResult, UploadItem } from "../upload";

interface Props {
  item: UploadItem | null;
  onClose(): void;
}

function Value({ v }: { v: unknown }) {
  if (v === null || v === undefined) return <span className="muted">—</span>;
  if (typeof v === "object") return <code>{JSON.stringify(v)}</code>;
  return <>{String(v)}</>;
}

/** Details of one processed file: scan verdict, metadata, preview, processing result. */
export function ResultDialog({ item, onClose }: Props) {
  const u = useUploader();
  const ref = useRef<HTMLDialogElement>(null);
  const [data, setData] = useState<FileResult | null>(null);
  const [processed, setProcessed] = useState<Record<string, unknown> | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const d = ref.current;
    if (!d) return;
    if (item && !d.open) d.showModal();
    if (!item && d.open) d.close();
  }, [item]);

  useEffect(() => {
    setData(null);
    setProcessed(null);
    setError(null);
    if (!item?.fileId) return;
    let live = true;
    u.api.result(item.fileId).then(async (r) => {
      if (!live) return;
      setData(r);
      if (r.process_result_url) {
        const res = await fetch(r.process_result_url);
        if (live && res.ok) setProcessed(await res.json());
      }
    }).catch((e: Error) => live && setError(e.message));
    return () => {
      live = false;
    };
  }, [item, u]);

  const meta = (data?.result.metadata ?? {}) as Record<string, unknown>;
  const scan = data?.result.scan as { verdict?: string } | undefined;

  return (
    <dialog ref={ref} className="dialog" onClose={onClose} aria-labelledby="result-title">
      <div className="dialog__head">
        <h2 id="result-title">{item?.name}</h2>
        <button type="button" className="fu-btn fu-btn--sm" onClick={onClose} aria-label="Close">Close</button>
      </div>
      {error && <p className="fu-text-error">Couldn't load the result: {error}</p>}
      {!data && !error && <p className="muted">Loading…</p>}
      {data && (
        <div className="dialog__body">
          {(data.thumbnail_url || item?.previewUrl) && (
            // The mock thumbnailer stores a 1×1 placeholder; fall back to the local preview when it's that small.
            <img className="dialog__thumb" src={data.thumbnail_url ?? item?.previewUrl} alt={`Preview of ${item?.name}`}
                 onLoad={(e) => {
                   const img = e.currentTarget;
                   if (img.naturalWidth < 8 && item?.previewUrl && img.src !== item.previewUrl) img.src = item.previewUrl;
                 }} />
          )}
          <dl className="kv">
            <dt>Size</dt><dd>{formatBytes(data.file.size_bytes)}</dd>
            <dt>Virus scan</dt><dd>{scan?.verdict === "clean" ? "Clean" : <Value v={scan?.verdict} />}</dd>
            {Object.entries(meta).filter(([k]) => !["name", "size_bytes"].includes(k)).map(([k, v]) => (
              <div key={k} className="kv__row"><dt>{k.replace(/_/g, " ")}</dt><dd><Value v={v} /></dd></div>
            ))}
            {processed && Object.entries(processed).map(([k, v]) => (
              <div key={`p-${k}`} className="kv__row"><dt>processed {k}</dt><dd><Value v={v} /></dd></div>
            ))}
            <dt>SHA-256</dt><dd><code className="hash">{data.file.sha256}</code></dd>
          </dl>
          <div className="dialog__actions">
            <a className="fu-btn fu-btn--primary" href={data.download_url} download={item?.name}>Download</a>
            {data.process_result_url && (
              <a className="fu-btn" href={data.process_result_url} target="_blank" rel="noreferrer">Processing result (JSON)</a>
            )}
          </div>
        </div>
      )}
    </dialog>
  );
}
