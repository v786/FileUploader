import { useState } from "react";
import { UploadTray, UploaderProvider, useUploadState, useWarnBeforeUnload } from "../upload";
import type { UploadItem } from "../upload";
import { DevPanel } from "./DevPanel";
import { ResultDialog } from "./ResultDialog";
import { ResultsScreen } from "./screens/ResultsScreen";
import { SelectScreen } from "./screens/SelectScreen";
import { UploadScreen } from "./screens/UploadScreen";

type Screen = "select" | "upload" | "results";

const STEPS: { id: Screen; label: string }[] = [
  { id: "select", label: "Choose files" },
  { id: "upload", label: "Upload" },
  { id: "results", label: "Results" },
];

function readUser(): string {
  try {
    return localStorage.getItem("demo-user") || "demo-user";
  } catch {
    return "demo-user";
  }
}

function Shell({ userId, onUser }: { userId: string; onUser(u: string): void }) {
  const [screen, setScreen] = useState<Screen>("select");
  const [devOpen, setDevOpen] = useState(false);
  const [open, setOpen] = useState<UploadItem | null>(null);
  const [notify, setNotify] = useState(() => typeof Notification !== "undefined" && Notification.permission === "granted");
  const { items } = useUploadState();
  useWarnBeforeUnload();
  const sent = items.some((i) => i.fileId);

  const askNotify = async () => {
    if (typeof Notification === "undefined") return;
    setNotify((await Notification.requestPermission()) === "granted");
  };

  return (
    <div className="app">
      <header className="topbar">
        <div className="topbar__brand">Uploader</div>
        <nav className="steps" aria-label="Steps">
          {STEPS.map((s, i) => (
            <button key={s.id} type="button" className={`step${screen === s.id ? " step--on" : ""}`}
                    aria-current={screen === s.id ? "step" : undefined}
                    disabled={s.id !== "select" && !sent} onClick={() => setScreen(s.id)}>
              <span className="step__n">{i + 1}</span> {s.label}
            </button>
          ))}
        </nav>
        <div className="topbar__tools">
          {!notify && typeof Notification !== "undefined" && (
            <button type="button" className="fu-btn fu-btn--sm" onClick={() => void askNotify()}>Notify me when done</button>
          )}
          <label className="user">
            <span className="muted small">User</span>
            <select value={userId} onChange={(e) => onUser(e.target.value)}>
              {["demo-user", "alice", "bob"].map((u) => <option key={u}>{u}</option>)}
            </select>
          </label>
          <button type="button" className="fu-btn fu-btn--sm" aria-expanded={devOpen} onClick={() => setDevOpen((v) => !v)}>
            Dev tools
          </button>
        </div>
      </header>

      <main className="main">
        {screen === "select" && <SelectScreen onStarted={() => setScreen("upload")} onViewUploads={() => setScreen("upload")} />}
        {screen === "upload" && (
          <UploadScreen onAddMore={() => setScreen("select")} onResults={() => setScreen("results")} onOpenResult={setOpen} />
        )}
        {screen === "results" && <ResultsScreen onNew={() => setScreen("select")} onOpenResult={setOpen} />}
      </main>

      {devOpen && <DevPanel onClose={() => setDevOpen(false)} />}
      <UploadTray hidden={screen === "upload"} onOpen={() => setScreen("upload")} />
      <ResultDialog item={open} onClose={() => setOpen(null)} />
    </div>
  );
}

export function App() {
  const [userId, setUserId] = useState(readUser);
  const switchUser = (u: string) => {
    try {
      localStorage.setItem("demo-user", u);
    } catch {
      /* private mode: just don't remember it */
    }
    setUserId(u);
  };
  // A new provider per user: each user gets their own uploader, uploads and resume list.
  return (
    <UploaderProvider key={userId} options={{ userId, notify: true }}>
      <Shell userId={userId} onUser={switchUser} />
    </UploaderProvider>
  );
}
