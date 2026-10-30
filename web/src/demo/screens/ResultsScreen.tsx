import { FileList, formatBytes, useUploadState, useUploader } from "../../upload";
import type { UploadItem } from "../../upload";

interface Props {
  onNew(): void;
  onOpenResult(item: UploadItem): void;
}

/** Step 3: what finished, what didn't and why. Partial success is a normal outcome. */
export function ResultsScreen({ onNew, onOpenResult }: Props) {
  const u = useUploader();
  const { items } = useUploadState();
  const sent = items.filter((i) => i.fileId);
  const done = sent.filter((i) => i.phase === "done");
  const failed = sent.filter((i) => i.phase === "error");
  const cancelled = sent.filter((i) => i.phase === "cancelled");
  const pending = sent.length - done.length - failed.length - cancelled.length;

  const startOver = () => {
    u.clearFinished();
    onNew();
  };

  return (
    <div className="screen">
      <header className="screen__head">
        <div>
          <h1>Results</h1>
          <p className="muted">
            {done.length} completed · {failed.length} need attention
            {cancelled.length > 0 && ` · ${cancelled.length} cancelled`}
            {pending > 0 && ` · ${pending} still in progress`}
          </p>
        </div>
        <div className="screen__actions">
          <button type="button" className="fu-btn fu-btn--primary" onClick={startOver}>Upload more files</button>
        </div>
      </header>

      {failed.length > 0 && (
        <section className="card" aria-label="Files that need attention">
          <div className="card__head"><h2>Need attention <span className="muted">({failed.length})</span></h2></div>
          <p className="muted small">Each file shows what went wrong and how to fix it. Retrying one file doesn't touch the others.</p>
          <FileList items={failed} collapseDone={false} label="Files that need attention" />
        </section>
      )}

      <section className="card" aria-label="Completed files">
        <div className="card__head"><h2>Completed <span className="muted">({done.length})</span></h2></div>
        {done.length === 0 ? (
          <p className="muted">No files have finished yet.</p>
        ) : (
          <ul className="results">
            {done.map((it) => (
              <li key={it.localId}>
                <button type="button" className="result" onClick={() => onOpenResult(it)}>
                  {it.previewUrl ? <img src={it.previewUrl} alt="" className="result__thumb" />
                    : <span className="result__thumb result__thumb--badge" aria-hidden="true">{it.name.split(".").pop()?.toUpperCase()}</span>}
                  <span className="result__name">{it.name}</span>
                  <span className="muted small">{formatBytes(it.size)}{it.dedupOf ? " · reused an earlier upload" : ""}</span>
                  <span className="result__link">View details</span>
                </button>
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}
