import { useState } from "react";
import type { Filter } from "../../labels";
import type { UploadItem } from "../../types";
import { useUploadState, useUploader, useWarnBeforeUnload } from "../context";
import { BatchSummary } from "./BatchSummary";
import { DropZone } from "./DropZone";
import { FileList } from "./FileList";
import { IconUpload } from "./icons";
import { Notices, ResumeInterrupted } from "./Notices";

export interface UploadPanelProps {
  onOpenResult?(item: UploadItem): void;
  /** Start as soon as files are dropped instead of waiting for the Upload button. */
  autoStart?: boolean;
}

/** Everything on one panel, for apps that don't need their own flow. */
export function UploadPanel({ onOpenResult, autoStart }: UploadPanelProps) {
  const u = useUploader();
  const { items, starting } = useUploadState();
  const [filter, setFilter] = useState<Filter>("all");
  useWarnBeforeUnload();
  const staged = items.filter((i) => i.phase === "staged" || i.phase === "checking" || i.phase === "invalid");
  const sent = items.filter((i) => i.fileId);
  const ready = items.filter((i) => i.phase === "staged").length;

  const add = async (files: File[]) => {
    await u.addFiles(files);
    if (autoStart) await u.start();
  };

  return (
    <div className="fu-panel">
      <Notices />
      <ResumeInterrupted />
      <DropZone onFiles={(f) => void add(f)} compact={items.length > 0}
                hint="Up to 50 files, 5 GB each. PDF, images, Office, text, ZIP and video." />
      {staged.length > 0 && (
        <>
          <FileList items={staged} collapseDone={false} label="Selected files" />
          {!autoStart && (
            <div className="fu-panel__start">
              <button type="button" className="fu-btn fu-btn--primary fu-btn--lg" disabled={!ready || starting}
                      onClick={() => void u.start()}>
                <IconUpload /> {starting ? "Starting…" : `Upload ${ready} file${ready === 1 ? "" : "s"}`}
              </button>
            </div>
          )}
        </>
      )}
      {sent.length > 0 && (
        <>
          <BatchSummary items={sent} filter={filter} onFilter={setFilter} />
          <FileList items={sent} filter={filter} onOpenResult={onOpenResult} />
        </>
      )}
    </div>
  );
}
