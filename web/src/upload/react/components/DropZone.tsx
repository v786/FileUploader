import { useRef, useState } from "react";
import type { DragEvent, KeyboardEvent, ReactNode } from "react";
import { IconUpload } from "./icons";

export interface DropZoneProps {
  onFiles(files: File[]): void;
  disabled?: boolean;
  title?: ReactNode;
  hint?: ReactNode;
  compact?: boolean;
  accept?: string;
}

/** Drop files here, or click / press Enter to browse. */
export function DropZone({ onFiles, disabled, title = "Drop files here or click to browse",
                           hint, compact, accept }: DropZoneProps) {
  const input = useRef<HTMLInputElement>(null);
  const [over, setOver] = useState(false);
  const depth = useRef(0);

  const take = (list: FileList | null) => {
    if (!disabled && list?.length) onFiles(Array.from(list));
  };
  const onDrop = (e: DragEvent) => {
    e.preventDefault();
    depth.current = 0;
    setOver(false);
    take(e.dataTransfer.files);
  };
  const onKey = (e: KeyboardEvent) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      input.current?.click();
    }
  };

  return (
    <div
      className={`fu-drop${over ? " fu-drop--over" : ""}${compact ? " fu-drop--compact" : ""}${disabled ? " fu-drop--disabled" : ""}`}
      role="button" tabIndex={disabled ? -1 : 0} aria-disabled={disabled}
      onClick={() => !disabled && input.current?.click()} onKeyDown={onKey}
      onDragEnter={(e) => { e.preventDefault(); depth.current += 1; setOver(true); }}
      onDragLeave={() => { depth.current -= 1; if (depth.current <= 0) setOver(false); }}
      onDragOver={(e) => e.preventDefault()} onDrop={onDrop}
    >
      <span className="fu-drop__icon"><IconUpload width={compact ? 18 : 28} height={compact ? 18 : 28} /></span>
      <span className="fu-drop__title">{over ? "Release to add files" : title}</span>
      {hint && !compact && <span className="fu-drop__hint">{hint}</span>}
      <input ref={input} type="file" multiple hidden accept={accept}
             onChange={(e) => { take(e.target.files); e.target.value = ""; }} />
    </div>
  );
}
