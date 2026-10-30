import { createContext, useContext, useEffect, useState, useSyncExternalStore } from "react";
import type { ReactNode } from "react";
import { Uploader } from "../uploader";
import type { UploaderOptions, UploaderState } from "../types";

const UploaderContext = createContext<Uploader | null>(null);

interface ProviderProps {
  /** Pass your own instance, or options to have the provider create (and dispose) one. */
  uploader?: Uploader;
  options?: UploaderOptions;
  /** Look for uploads interrupted by a closed tab and offer to resume them (default true). */
  findInterrupted?: boolean;
  children: ReactNode;
}

export function UploaderProvider({ uploader, options, findInterrupted = true, children }: ProviderProps) {
  const [u] = useState(() => {
    if (uploader) return uploader;
    if (!options) throw new Error("UploaderProvider needs `uploader` or `options`");
    return new Uploader(options);
  });
  useEffect(() => u.attach(!uploader), [u, uploader]);
  useEffect(() => {
    if (findInterrupted) void u.loadInterrupted();
  }, [u, findInterrupted]);
  return <UploaderContext.Provider value={u}>{children}</UploaderContext.Provider>;
}

export function useUploader(): Uploader {
  const u = useContext(UploaderContext);
  if (!u) throw new Error("useUploader must be used inside <UploaderProvider>");
  return u;
}

/** The whole upload state; re-renders at most every 100 ms while bytes are moving. */
export function useUploadState(): UploaderState {
  const u = useUploader();
  return useSyncExternalStore(u.store.subscribe, u.store.get);
}

/** Warn before leaving only while some bytes aren't safely in storage yet. */
export function useWarnBeforeUnload(): void {
  const u = useUploader();
  useEffect(() => {
    const onBeforeUnload = (e: BeforeUnloadEvent) => {
      if (!u.hasUnconfirmedBytes()) return;
      e.preventDefault();
      e.returnValue = "";
    };
    window.addEventListener("beforeunload", onBeforeUnload);
    return () => window.removeEventListener("beforeunload", onBeforeUnload);
  }, [u]);
}

/** Current time in seconds, ticking every `ms` (for countdowns like "next try in 8s"). */
export function useNow(ms = 1000): number {
  const [t, setT] = useState(() => Date.now() / 1000);
  useEffect(() => {
    const id = setInterval(() => setT(Date.now() / 1000), ms);
    return () => clearInterval(id);
  }, [ms]);
  return t;
}
