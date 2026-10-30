import { matchesFilter } from "../../labels";
import { useUploadState } from "../context";
import { ProgressBar } from "./ProgressBar";

export interface UploadTrayProps {
  onOpen(): void;
  /** Hide it (e.g. while the full upload screen is showing). */
  hidden?: boolean;
}

/** A small tray that follows the user around the app while uploads run in the background. */
export function UploadTray({ onOpen, hidden }: UploadTrayProps) {
  const { items } = useUploadState();
  const sent = items.filter((i) => i.fileId && i.phase !== "cancelled");
  const active = sent.filter((i) => matchesFilter(i, "active")).length;
  const attention = sent.filter((i) => i.phase === "error").length;
  if (hidden || (!active && !attention)) return null;
  // A failed file is finished too: count it as complete so a finished batch reads 100%, not 85%.
  const pct = sent.reduce((s, i) => s + (i.phase === "error" ? 100 : i.overallPct), 0) / Math.max(1, sent.length);
  return (
    <button type="button" className="fu-tray" onClick={onOpen} aria-label="Open uploads">
      <span className="fu-tray__text">
        {active ? `Uploading ${active} file${active > 1 ? "s" : ""}` : "Uploads finished"}
        {attention > 0 && <span className="fu-text-error"> · {attention} need attention</span>}
        <span className="fu-tray__pct">{Math.floor(pct)}%</span>
      </span>
      <ProgressBar value={pct} tone={attention && !active ? "error" : "normal"} label="Upload progress" size="sm" />
    </button>
  );
}
