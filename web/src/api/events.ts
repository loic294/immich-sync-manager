import { type ReadonlySignal, signal } from '@preact/signals-core';

/**
 * Live change notifications from `GET /api/admin/events` (server-sent events named `change`, each
 * a JSON `{ "type": ... }`). There is no replay: after a reconnect every listener is called with a
 * `resync` event so pages refetch what they show.
 */

export type ChangeType =
  | 'operation_updated'
  | 'operation_committed'
  | 'maintenance_changed'
  | 'sync_changed'
  | 'conflicts_changed'
  | 'watch_changed'
  | 'library_scanned'
  | 'resync';

export interface ChangeEvent {
  type: ChangeType;
  [key: string]: unknown;
}

export type ConnectionState = 'connecting' | 'open' | 'reconnecting' | 'closed';

export interface EventSourceLike {
  onopen: ((event: Event) => void) | null;
  onerror: ((event: Event) => void) | null;
  addEventListener(type: string, listener: (event: MessageEvent<string>) => void): void;
  close(): void;
}

export interface EventsClient {
  readonly connection: ReadonlySignal<ConnectionState>;
  /** Called for every change and for `resync`; returns the unsubscribe function. */
  on(handler: (event: ChangeEvent) => void): () => void;
  start(): void;
  stop(): void;
  emit(event: ChangeEvent): void;
}

const MIN_RETRY_MS = 3000;
const MAX_RETRY_MS = 30_000;

export function createEventsClient(
  url = '/api/admin/events',
  factory: (url: string) => EventSourceLike = (u) => new EventSource(u),
): EventsClient {
  const connection = signal<ConnectionState>('closed');
  const handlers = new Set<(event: ChangeEvent) => void>();
  let source: EventSourceLike | undefined;
  let timer: number | undefined;
  let failures = 0;
  let missed = false;

  function emit(event: ChangeEvent) {
    for (const handler of [...handlers]) {
      try {
        handler(event);
      } catch (error) {
        console.error('[events] handler failed', error);
      }
    }
  }

  function open() {
    source = factory(url);
    source.onopen = () => {
      connection.value = 'open';
      failures = 0;
      if (missed) emit({ type: 'resync' });
      missed = false;
    };
    source.onerror = () => {
      source?.close();
      source = undefined;
      missed = true;
      connection.value = 'reconnecting';
      failures += 1;
      const delay = Math.min(MAX_RETRY_MS, MIN_RETRY_MS * 2 ** Math.min(failures - 1, 4));
      timer = window.setTimeout(open, delay);
    };
    source.addEventListener('change', (event) => {
      try {
        const parsed: unknown = JSON.parse(event.data);
        if (typeof parsed === 'object' && parsed !== null && 'type' in parsed) {
          emit(parsed as ChangeEvent);
        }
      } catch {
        // Ignore malformed events.
      }
    });
    source.addEventListener('resync', () => emit({ type: 'resync' }));
  }

  return {
    connection,
    on(handler) {
      handlers.add(handler);
      return () => handlers.delete(handler);
    },
    start() {
      if (source || timer !== undefined) return;
      connection.value = 'connecting';
      open();
    },
    stop() {
      window.clearTimeout(timer);
      timer = undefined;
      source?.close();
      source = undefined;
      missed = true;
      connection.value = 'closed';
    },
    emit,
  };
}

export const events: EventsClient = createEventsClient();
