import { matchesFilter } from "../../labels";
import type { Filter } from "../../labels";
import type { UploadItem } from "../../types";
import { useUploader } from "../context";
import { IconPause, IconPlay } from "./icons";
import { ProgressBar } from "./ProgressBar";

export interface BatchSummaryProps {
  items: UploadItem[];
  filter: Filter;
  onFilter(f: Filter): void;
  title?: string;
}

const FILTERS: { id: Filter; label: string }[] = [
  { id: "all", label: "All" },
  { id: "active", label: "In progress" },
  { id: "attention", label: "Need attention" },
  { id: "done", label: "Done" },
];

/** "42 of 50 done, 2 need attention", one bar for the batch, filters and bulk actions. */
export function BatchSummary({ items, filter, onFilter, title = "Uploads" }: BatchSummaryProps) {
  const u = useUploader();
  const counted = items.filter((i) => i.phase !== "cancelled" && i.phase !== "invalid");
  const done = items.filter((i) => i.phase === "done").length;
  const attention = items.filter((i) => matchesFilter(i, "attention")).length;
  const active = items.filter((i) => matchesFilter(i, "active")).length;
  const paused = items.filter((i) => i.phase === "paused").length;
  const pausable = items.filter((i) => ["queued", "uploading", "finishing"].includes(i.phase)).length;
  const pct = counted.length ? counted.reduce((s, i) => s + (i.phase === "error" ? 100 : i.overallPct), 0) / counted.length : 0;
  const tone = active === 0 && attention === 0 && done > 0 ? "done" : attention > 0 && active === 0 ? "error" : "normal";

  return (
    <section className="fu-summary" aria-label="Upload summary">
      <div className="fu-summary__head">
        <div>
          <h2 className="fu-summary__title">{title}</h2>
          <p className="fu-summary__text">
            {done} of {counted.length} done
            {attention > 0 && <>, <strong className="fu-text-error">{attention} need attention</strong></>}
            {active > 0 && <> · {active} in progress</>}
          </p>
        </div>
        <div className="fu-summary__actions">
          {pausable > 0 && (
            <button type="button" className="fu-btn fu-btn--sm" onClick={() => u.pauseAll()}>
              <IconPause /><span>Pause all</span>
            </button>
          )}
          {paused > 0 && (
            <button type="button" className="fu-btn fu-btn--sm fu-btn--primary" onClick={() => u.resumeAll()}>
              <IconPlay /><span>Resume all</span>
            </button>
          )}
        </div>
      </div>
      <ProgressBar value={pct} tone={tone} label="Overall progress" size="sm" />
      <div className="fu-chips" role="group" aria-label="Filter files">
        {FILTERS.map((f) => {
          const n = f.id === "all" ? items.length : items.filter((i) => matchesFilter(i, f.id)).length;
          return (
            <button key={f.id} type="button" aria-pressed={filter === f.id}
                    className={`fu-chip${filter === f.id ? " fu-chip--on" : ""}${f.id === "attention" && n > 0 ? " fu-chip--alert" : ""}`}
                    onClick={() => onFilter(f.id)}>
              {f.label} <span className="fu-chip__n">{n}</span>
            </button>
          );
        })}
      </div>
    </section>
  );
}
