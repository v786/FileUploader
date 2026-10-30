import { memo } from "react";
import { describe } from "../../labels";
import { formatBytes } from "../../progress";
import type { UploadItem } from "../../types";
import { useUploader } from "../context";
import { IconArrowUp, IconEye, IconPause, IconPlay, IconRetry, IconTrash, IconX } from "./icons";
import { ProgressBar } from "./ProgressBar";

export interface FileRowProps {
  item: UploadItem;
  /** Current time in seconds (for countdowns); pass from useNow() in the list. */
  now?: number;
  onOpenResult?(item: UploadItem): void;
}

function Thumb({ item }: { item: UploadItem }) {
  if (item.previewUrl) return <img className="fu-thumb" src={item.previewUrl} alt="" />;
  const ext = item.name.includes(".") ? item.name.split(".").pop()!.slice(0, 4).toUpperCase() : "FILE";
  return <span className="fu-thumb fu-thumb--badge" aria-hidden="true">{ext}</span>;
}

function Action({ label, onClick, icon, variant }: { label: string; onClick(): void; icon: JSX.Element;
                                                     variant?: "primary" | "danger" }) {
  return (
    <button type="button" className={`fu-btn fu-btn--sm${variant ? ` fu-btn--${variant}` : ""}`} onClick={onClick}>
      {icon}
      <span>{label}</span>
    </button>
  );
}

export const FileRow = memo(function FileRow({ item, now, onOpenResult }: FileRowProps) {
  const u = useUploader();
  const id = item.localId;
  const { label, detail } = describe(item, now);
  const showBar = !["staged", "checking", "invalid"].includes(item.phase);
  const problem = (item.phase === "error" || item.phase === "invalid") && item.error;

  const actions: JSX.Element[] = [];
  switch (item.phase) {
    case "queued":
      actions.push(<Action key="next" label="Upload next" icon={<IconArrowUp />} onClick={() => u.prioritize(id)} />);
      actions.push(<Action key="pause" label="Pause" icon={<IconPause />} onClick={() => u.pause(id)} />);
      actions.push(<Action key="cancel" label="Cancel" icon={<IconX />} onClick={() => void u.cancel(id)} />);
      break;
    case "uploading":
    case "finishing":
      actions.push(<Action key="pause" label="Pause" icon={<IconPause />} onClick={() => u.pause(id)} />);
      actions.push(<Action key="cancel" label="Cancel" icon={<IconX />} onClick={() => void u.cancel(id)} />);
      break;
    case "paused":
      actions.push(<Action key="resume" label="Resume" icon={<IconPlay />} onClick={() => u.resume(id)} variant="primary" />);
      actions.push(<Action key="cancel" label="Cancel" icon={<IconX />} onClick={() => void u.cancel(id)} />);
      break;
    case "processing":
      actions.push(<Action key="cancel" label="Cancel" icon={<IconX />} onClick={() => void u.cancel(id)} />);
      break;
    case "error":
      if (item.error?.retryable) {
        actions.push(<Action key="retry" label="Retry" icon={<IconRetry />} onClick={() => void u.retry(id)} variant="primary" />);
      }
      actions.push(<Action key="remove" label="Remove" icon={<IconTrash />} onClick={() => u.remove(id)} />);
      break;
    case "done":
      if (onOpenResult) actions.push(<Action key="view" label="View" icon={<IconEye />} onClick={() => onOpenResult(item)} />);
      actions.push(<Action key="remove" label="Remove" icon={<IconTrash />} onClick={() => u.remove(id)} />);
      break;
    case "staged":
    case "invalid":
    case "cancelled":
      actions.push(<Action key="remove" label="Remove" icon={<IconTrash />} onClick={() => u.remove(id)} />);
      break;
  }

  return (
    <li className={`fu-row fu-row--${item.tone} fu-row--${item.phase}`}>
      <Thumb item={item} />
      <div className="fu-row__main">
        <div className="fu-row__top">
          <span className="fu-row__name" title={item.name}>{item.name}</span>
          <span className="fu-row__size">{formatBytes(item.size)}</span>
        </div>
        {showBar && (
          <ProgressBar value={item.overallPct} tone={item.tone} split={item.weights?.upload}
                       label={`${item.name}: ${label}`} />
        )}
        <div className="fu-row__status">
          <span className="fu-row__label">{label}</span>
          {showBar && item.phase !== "error" && item.phase !== "cancelled" && (
            <span className="fu-row__pct">{Math.floor(item.overallPct)}%</span>
          )}
          {detail && !problem && <span className="fu-row__detail">{detail}</span>}
        </div>
        {problem && (
          <div className="fu-row__error">
            <span>{item.error!.message}</span>
            {item.error!.fix && item.error!.fix.action !== "retry" && (
              <span className="fu-row__fix">How to fix: {item.error!.fix.label}</span>
            )}
          </div>
        )}
      </div>
      <div className="fu-row__actions">{actions}</div>
    </li>
  );
});
