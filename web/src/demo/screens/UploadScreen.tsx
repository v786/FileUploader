import { useState } from "react";
import { BatchSummary, FileList, Notices, matchesFilter, useUploadState } from "../../upload";
import type { Filter, UploadItem } from "../../upload";

interface Props {
  onAddMore(): void;
  onResults(): void;
  onOpenResult(item: UploadItem): void;
}

/** Step 2: live per-file progress — upload, then each processing stage, then the outcome. */
export function UploadScreen({ onAddMore, onResults, onOpenResult }: Props) {
  const { items } = useUploadState();
  const [filter, setFilter] = useState<Filter>("all");
  const sent = items.filter((i) => i.fileId);
  const active = sent.filter((i) => matchesFilter(i, "active")).length;
  const attention = sent.filter((i) => i.phase === "error").length;
  const finished = sent.length > 0 && active === 0;

  return (
    <div className="screen">
      <header className="screen__head">
        <div>
          <h1>{finished ? "Uploads finished" : "Uploading"}</h1>
          <p className="muted">
            {finished
              ? "Everything that could be processed is done."
              : "You can keep working — uploads continue while you use the rest of the app."}
          </p>
        </div>
        <div className="screen__actions">
          <button type="button" className="fu-btn" onClick={onAddMore}>Add more files</button>
          <button type="button" className={`fu-btn${finished ? " fu-btn--primary" : ""}`} onClick={onResults}>
            See results
          </button>
        </div>
      </header>

      <Notices />

      {finished && attention > 0 && (
        <div className="callout callout--error" role="status">
          <strong>{attention} file{attention > 1 ? "s" : ""} need{attention > 1 ? "" : "s"} attention.</strong>{" "}
          The rest of the batch is fine. Fix or retry {attention > 1 ? "them" : "it"} one at a time.
          <button type="button" className="fu-btn fu-btn--sm" onClick={() => setFilter("attention")}>Show them</button>
        </div>
      )}

      {sent.length === 0 ? (
        <div className="empty">
          <p>Nothing is uploading.</p>
          <button type="button" className="fu-btn fu-btn--primary" onClick={onAddMore}>Choose files</button>
        </div>
      ) : (
        <>
          <BatchSummary items={sent} filter={filter} onFilter={setFilter} title="This session" />
          <FileList items={sent} filter={filter} onOpenResult={onOpenResult}
                    emptyText={filter === "attention" ? "Nothing needs attention." : "No files in this view."} />
        </>
      )}
    </div>
  );
}
