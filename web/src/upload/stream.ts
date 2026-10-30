/**
 * Server-sent progress for one batch. EventSource reconnects on its own and sends
 * Last-Event-ID; if the browser gives up, we reopen with ?last_event_id= so the
 * server replays what we missed (or sends a snapshot). Events carry a per-batch seq
 * and the uploader applies only newer ones, so replays are harmless.
 */
const EVENT_TYPES = [
  "snapshot", "file.stage", "file.retrying", "file.waiting", "file.completed", "file.failed",
  "file.cancelled", "batch.summary",
];

export type StreamHandler = (type: string, data: Record<string, unknown>, seq: number) => void;

export class BatchStream {
  private es: EventSource | null = null;
  private lastSeq: number | undefined;
  private closed = false;
  private reopenTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(private readonly url: (lastSeq?: number) => string, private readonly onEvent: StreamHandler) {
    this.open();
  }

  private open(): void {
    if (this.closed || typeof EventSource === "undefined") return;
    const es = new EventSource(this.url(this.lastSeq));
    this.es = es;
    for (const type of EVENT_TYPES) {
      es.addEventListener(type, (e) => {
        const me = e as MessageEvent<string>;
        const seq = Number(me.lastEventId);
        if (Number.isFinite(seq)) this.lastSeq = Math.max(this.lastSeq ?? 0, seq);
        this.onEvent(type, JSON.parse(me.data), seq);
      });
    }
    es.onerror = () => {
      if (es.readyState === EventSource.CLOSED && !this.closed) {
        this.reopenTimer = setTimeout(() => this.open(), 2000);
      }
    };
  }

  close(): void {
    this.closed = true;
    if (this.reopenTimer) clearTimeout(this.reopenTimer);
    this.es?.close();
  }
}
