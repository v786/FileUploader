import type { Tone } from "../../types";

export interface ProgressBarProps {
  /** 0–100 */
  value: number;
  tone?: Tone;
  /** Where the upload segment ends and processing begins (0–1); draws a divider. */
  split?: number;
  label: string;
  size?: "sm" | "md";
}

/**
 * One bar, two segments: upload then processing. A retry changes the colour, not the fill;
 * the fill never moves backwards (the engine guarantees it).
 */
export function ProgressBar({ value, tone = "normal", split, label, size = "md" }: ProgressBarProps) {
  const v = Math.max(0, Math.min(100, value));
  return (
    <div className={`fu-bar fu-bar--${tone} fu-bar--${size}`} role="progressbar" aria-label={label}
         aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(v)}>
      <div className="fu-bar__fill" style={{ width: `${v}%` }} />
      {split !== undefined && split > 0.02 && split < 0.98 && (
        <div className="fu-bar__split" style={{ left: `${split * 100}%` }} aria-hidden="true" />
      )}
    </div>
  );
}
