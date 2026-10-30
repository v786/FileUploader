import { useState } from "react";
import { DropZone, FileList, Notices, ResumeInterrupted, formatBytes, useUploadState, useUploader } from "../../upload";
import { goodSamples, largeSample, problemSamples } from "../samples";

interface Props {
  onStarted(): void;
  onViewUploads(): void;
}

/** Step 1: pick files. Each is checked in the browser right away; nothing is sent yet. */
export function SelectScreen({ onStarted, onViewUploads }: Props) {
  const u = useUploader();
  const { items, starting } = useUploadState();
  const [busy, setBusy] = useState(false);
  const selection = items.filter((i) => ["checking", "staged", "invalid"].includes(i.phase));
  const ready = selection.filter((i) => i.phase === "staged");
  const rejected = selection.filter((i) => i.phase === "invalid");
  const inFlight = items.filter((i) => i.fileId && !["done", "cancelled", "error"].includes(i.phase)).length;
  const totalBytes = ready.reduce((s, i) => s + i.size, 0);

  const add = async (make: () => Promise<File[]> | File[]) => {
    setBusy(true);
    try {
      await u.addFiles(await make());
    } finally {
      setBusy(false);
    }
  };

  const start = async () => {
    await u.start();
    if (u.store.get().items.some((i) => i.fileId)) onStarted();
  };

  return (
    <div className="screen">
      <header className="screen__head">
        <div>
          <h1>Choose files</h1>
          <p className="muted">Up to 50 files at a time, 5 GB each. We check every file before anything is sent.</p>
        </div>
        {inFlight > 0 && (
          <button type="button" className="fu-btn" onClick={onViewUploads}>View {inFlight} upload{inFlight > 1 ? "s" : ""} in progress</button>
        )}
      </header>

      <Notices />
      <ResumeInterrupted />

      <DropZone onFiles={(f) => void add(() => f)} compact={selection.length > 0}
                hint="PDF, images, Office documents, text, CSV, JSON, ZIP and video" />

      <div className="samples">
        <span className="muted">No files handy?</span>
        <button type="button" className="fu-btn fu-btn--sm" disabled={busy} onClick={() => void add(goodSamples)}>Add sample files</button>
        <button type="button" className="fu-btn fu-btn--sm" disabled={busy} onClick={() => void add(problemSamples)}>Add files that fail</button>
        <button type="button" className="fu-btn fu-btn--sm" disabled={busy} onClick={() => void add(() => [largeSample()])}>Add a 200 MB file</button>
      </div>

      {selection.length > 0 && (
        <section className="card" aria-label="Selected files">
          <div className="card__head">
            <h2>Selected <span className="muted">({selection.length})</span></h2>
            <button type="button" className="fu-btn fu-btn--sm" onClick={() => selection.forEach((i) => u.remove(i.localId))}>Clear all</button>
          </div>
          <FileList items={selection} collapseDone={false} label="Selected files" />
        </section>
      )}

      {selection.length > 0 && (
        <footer className="action-bar">
          <p className="action-bar__text">
            <strong>{ready.length} ready</strong> · {formatBytes(totalBytes)}
            {rejected.length > 0 && <span className="fu-text-error"> · {rejected.length} can't be uploaded</span>}
          </p>
          <button type="button" className="fu-btn fu-btn--primary fu-btn--lg" disabled={!ready.length || starting}
                  onClick={() => void start()}>
            {starting ? "Starting…" : `Upload ${ready.length} file${ready.length === 1 ? "" : "s"}`}
          </button>
        </footer>
      )}
    </div>
  );
}
