import { useEffect, useRef, useState } from "react";
import { matchesFilter, stageLabel } from "../../labels";
import type { Filter } from "../../labels";
import type { UploadItem } from "../../types";
import { useNow } from "../context";
import { FileRow } from "./FileRow";

export interface FileListProps {
  items: UploadItem[];
  filter?: Filter;
  /** Fold finished rows into a "42 done" group; active and failed files stay on top. */
  collapseDone?: boolean;
  onOpenResult?(item: UploadItem): void;
  emptyText?: string;
  label?: string;
}

/** Announces stage changes and failures politely, at most once per file per stage. */
function useAnnouncements(items: UploadItem[]): string {
  const seen = useRef(new Map<string, string>());
  const [text, setText] = useState("");
  useEffect(() => {
    const out: string[] = [];
    for (const it of items) {
      const key = `${it.phase}:${it.stage ?? ""}`;
      if (seen.current.get(it.localId) === key) continue;
      const first = !seen.current.has(it.localId);
      seen.current.set(it.localId, key);
      if (first && ["staged", "checking"].includes(it.phase)) continue;
      if (it.phase === "processing") out.push(`${it.name}: ${stageLabel(it.stage)}`);
      else if (it.phase === "done") out.push(`${it.name} is done`);
      else if (it.phase === "error" || it.phase === "invalid") out.push(`${it.name} needs attention: ${it.error?.message ?? ""}`);
    }
    if (out.length) setText(out.slice(-3).join(". "));
  }, [items]);
  return text;
}

export function FileList({ items, filter = "all", collapseDone = true, onOpenResult,
                           emptyText = "No files here.", label = "Files" }: FileListProps) {
  const now = useNow(1000);
  const [showDone, setShowDone] = useState(false);
  const announce = useAnnouncements(items);
  const visible = items.filter((it) => matchesFilter(it, filter));
  const folding = collapseDone && filter === "all";
  const top = folding ? visible.filter((i) => i.phase !== "done") : visible;
  const done = folding ? visible.filter((i) => i.phase === "done") : [];
  // Failed files first, then the rest in the order they were added.
  const rank = (i: UploadItem) => (i.phase === "error" || i.phase === "invalid" ? 0 : 1);
  const ordered = [...top].sort((a, b) => rank(a) - rank(b));

  return (
    <div className="fu-list">
      <div className="fu-sr-only" aria-live="polite">{announce}</div>
      {visible.length === 0 && <p className="fu-list__empty">{emptyText}</p>}
      {ordered.length > 0 && (
        <ul aria-label={label} className="fu-list__rows">
          {ordered.map((it) => <FileRow key={it.localId} item={it} now={now} onOpenResult={onOpenResult} />)}
        </ul>
      )}
      {done.length > 0 && (
        <div className="fu-list__group">
          <button type="button" className="fu-list__toggle" aria-expanded={showDone}
                  onClick={() => setShowDone((v) => !v)}>
            <span className="fu-list__chev" aria-hidden="true">{showDone ? "▾" : "▸"}</span>
            {done.length} done
          </button>
          {showDone && (
            <ul aria-label="Finished files" className="fu-list__rows">
              {done.map((it) => <FileRow key={it.localId} item={it} now={now} onOpenResult={onOpenResult} />)}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}
