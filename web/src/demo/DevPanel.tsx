import { useCallback, useEffect, useState } from "react";

/**
 * Developer panel: flips the backend's mock-vendor fault profiles (POST /__faults/:vendor)
 * so you can watch retries, "waiting for a service", the polling fallback and failures
 * without touching the backend. Not part of the upload module.
 */
type Profiles = Record<string, Record<string, unknown>>;

const PRESETS: { id: string; label: string; detail: string; apply: Profiles }[] = [
  { id: "scanner-down", label: "Virus scanner down", detail: "Files wait, then resume when you turn it off",
    apply: { scanner: { down: true } } },
  { id: "flaky", label: "Flaky services (30% errors)", detail: "Retries with backoff; files still complete",
    apply: { extractor: { error_rate: 0.3, error_status: 503 }, thumbnailer: { error_rate: 0.3, error_status: 503 },
             processor: { error_rate: 0.3, error_status: 503 } } },
  { id: "slow-processor", label: "Slow processor", detail: "Queued 5 s, then 20 s of processing with live %",
    apply: { processor: { queue_ms: 5000, duration_ms: 20000 } } },
  { id: "no-webhooks", label: "Processor webhooks lost", detail: "Completion found by polling (10 s → 60 s)",
    apply: { processor: { duration_ms: 8000, webhook_drop_rate: 1 } } },
];
const VENDORS = ["scanner", "extractor", "thumbnailer", "processor"];

const matches = (current: Profiles, want: Profiles) =>
  Object.entries(want).every(([v, p]) => JSON.stringify(current[v] ?? {}) === JSON.stringify(p));

export function DevPanel({ onClose }: { onClose(): void }) {
  const [current, setCurrent] = useState<Profiles>({});
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const r = await fetch("/__faults");
      setCurrent(await r.json());
      setError(null);
    } catch {
      setError("Backend not reachable on /__faults.");
    }
  }, []);
  useEffect(() => void load(), [load]);

  const send = async (profiles: Profiles) => {
    await Promise.all(Object.entries(profiles).map(([v, p]) =>
      fetch(`/__faults/${v}`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(p) })));
    await load();
  };
  const reset = async () => {
    await Promise.all(VENDORS.map((v) => fetch(`/__faults/${v}`, { method: "DELETE" })));
    await load();
  };

  return (
    <aside className="devpanel" aria-label="Developer tools">
      <div className="devpanel__head">
        <h2>Simulate problems</h2>
        <button type="button" className="fu-btn fu-btn--sm" onClick={onClose}>Close</button>
      </div>
      <p className="muted small">Changes the mock services on the backend for everyone using it.</p>
      {error && <p className="fu-text-error small">{error}</p>}
      <ul className="devpanel__list">
        {PRESETS.map((p) => {
          const on = matches(current, p.apply);
          return (
            <li key={p.id}>
              <label className="toggle">
                <input type="checkbox" checked={on}
                       onChange={() => void (on ? send(Object.fromEntries(Object.keys(p.apply).map((v) => [v, {}]))) : send(p.apply))} />
                <span>
                  <strong>{p.label}</strong>
                  <span className="muted small">{p.detail}</span>
                </span>
              </label>
            </li>
          );
        })}
      </ul>
      <button type="button" className="fu-btn fu-btn--sm" onClick={() => void reset()}>Reset all</button>
      <details className="devpanel__raw">
        <summary>Current fault profiles</summary>
        <pre>{JSON.stringify(current, null, 2)}</pre>
      </details>
      <p className="muted small">Tip: use your browser's network throttling (e.g. "Slow 4G") to watch byte-level upload progress,
        and its offline switch to see uploads pause and resume.</p>
    </aside>
  );
}
