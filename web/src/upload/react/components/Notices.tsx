import { useRef, useState } from "react";
import { formatBytes } from "../../progress";
import { useUploadState, useUploader } from "../context";
import { IconAlert, IconOffline } from "./icons";

/** Offline and service notices ("Busy — your upload will start shortly"). */
export function Notices() {
  const { online, notice } = useUploadState();
  return (
    <div className="fu-notices" aria-live="polite">
      {!online && (
        <div className="fu-notice fu-notice--warn">
          <IconOffline /> You're offline. Uploads are paused and will pick up where they left off when you reconnect.
        </div>
      )}
      {notice && (
        <div className={`fu-notice fu-notice--${notice.tone === "error" ? "error" : "info"}`}>
          {notice.tone === "error" && <IconAlert />} {notice.text}
        </div>
      )}
    </div>
  );
}

/**
 * Uploads interrupted by a closed tab or crash. The browser can't reopen a file on its
 * own, so the user re-picks it; we match it by fingerprint and resume from the last part.
 */
export function ResumeInterrupted() {
  const u = useUploader();
  const { interrupted } = useUploadState();
  const input = useRef<HTMLInputElement>(null);
  const [message, setMessage] = useState<string | null>(null);
  if (!interrupted.length && !message) return null;

  const pick = async (files: FileList | null) => {
    if (!files?.length) return;
    const { matched, unmatched } = await u.resumeWith(files);
    setMessage([
      matched.length ? `Resuming ${matched.length} file${matched.length > 1 ? "s" : ""}.` : "",
      unmatched.length ? `${unmatched.join(", ")} didn't match an interrupted upload.` : "",
    ].filter(Boolean).join(" "));
  };

  return (
    <section className="fu-resume" aria-label="Interrupted uploads">
      {interrupted.length > 0 && (
        <>
          <div className="fu-resume__head">
            <strong>{interrupted.length} upload{interrupted.length > 1 ? "s were" : " was"} interrupted</strong>
            <span>Pick the same files again to continue — parts already uploaded won't be sent twice.</span>
          </div>
          <ul className="fu-resume__list">
            {interrupted.slice(0, 6).map((f) => (
              <li key={f.fileId}>
                <span className="fu-row__name">{f.name}</span> <span className="fu-muted">{formatBytes(f.size)}</span>
              </li>
            ))}
            {interrupted.length > 6 && <li className="fu-muted">and {interrupted.length - 6} more</li>}
          </ul>
          <div className="fu-resume__actions">
            <button type="button" className="fu-btn fu-btn--primary" onClick={() => input.current?.click()}>
              Pick files to resume
            </button>
            <button type="button" className="fu-btn" onClick={() => void u.discardInterrupted()}>Discard</button>
            <input ref={input} type="file" multiple hidden
                   onChange={(e) => { void pick(e.target.files); e.target.value = ""; }} />
          </div>
        </>
      )}
      {message && <p className="fu-resume__msg" role="status">{message}</p>}
    </section>
  );
}
