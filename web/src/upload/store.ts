/**
 * A tiny external store for useSyncExternalStore. Progress updates are batched to
 * at most one render per 100 ms, so 50 rows animating stay smooth; state changes
 * that matter (phase, errors) notify right away.
 */
export class Store<S> {
  private listeners = new Set<() => void>();
  private timer: ReturnType<typeof setTimeout> | null = null;

  constructor(private state: S) {}

  get = (): S => this.state;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  set(next: S, urgent = true): void {
    this.state = next;
    if (urgent) this.flush();
    else if (this.timer === null) this.timer = setTimeout(() => this.flush(), 100);
  }

  private flush(): void {
    if (this.timer !== null) clearTimeout(this.timer);
    this.timer = null;
    for (const l of this.listeners) l();
  }
}
