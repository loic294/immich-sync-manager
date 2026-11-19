import { type ReadonlySignal, signal } from '@preact/signals-core';

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

export interface SocketLike {
  onopen: ((event: Event) => void) | null;
  onclose: ((event: CloseEvent) => void) | null;
  onerror: ((event: Event) => void) | null;
  onmessage: ((event: MessageEvent<string>) => void) | null;
  close(): void;
}

export interface EventsClient {
  readonly connection: ReadonlySignal<ConnectionState>;
  on(handler: (event: ChangeEvent) => void): () => void;
  start(): void;
  stop(): void;
  emit(event: ChangeEvent): void;
}

const TYPES = new Set<ChangeType>([
  'operation_updated',
  'operation_committed',
  'maintenance_changed',
  'sync_changed',
  'conflicts_changed',
  'watch_changed',
  'library_scanned',
  'resync',
]);

/** Reconnecting WebSocket notifications; refetch on every connection to close snapshot races. */
export function createEventsClient(
  url = '/api/admin/live',
  factory: (url: string) => SocketLike = (u) => new WebSocket(u),
): EventsClient {
  const connection = signal<ConnectionState>('closed');
  const handlers = new Set<(event: ChangeEvent) => void>();
  let source: SocketLike | undefined;
  let timer: number | undefined;
  let failures = 0;
  let active = false;

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
    timer = undefined;
    if (!active) return;
    const target = new URL(url, window.location.href);
    target.protocol = target.protocol === 'https:' || target.protocol === 'wss:' ? 'wss:' : 'ws:';
    const socket = factory(target.href);
    source = socket;
    socket.onopen = () => {
      if (source !== socket || !active) return;
      connection.value = 'open';
      failures = 0;
      emit({ type: 'resync' });
    };
    const reconnect = () => {
      if (source !== socket || !active) return;
      source = undefined;
      socket.onclose = null;
      socket.onerror = null;
      socket.close();
      connection.value = 'reconnecting';
      const delay = Math.min(30_000, 3000 * 2 ** Math.min(failures++, 4));
      timer = window.setTimeout(open, delay);
    };
    socket.onerror = reconnect;
    socket.onclose = reconnect;
    socket.onmessage = (event) => {
      if (source !== socket || !active) return;
      try {
        const parsed: unknown = JSON.parse(event.data);
        if (
          typeof parsed === 'object' &&
          parsed !== null &&
          'type' in parsed &&
          typeof parsed.type === 'string' &&
          TYPES.has(parsed.type as ChangeType)
        ) {
          emit(parsed as ChangeEvent);
        }
      } catch (error) {
        console.error('[events] invalid notification', error);
      }
    };
  }

  return {
    connection,
    on(handler) {
      handlers.add(handler);
      return () => handlers.delete(handler);
    },
    start() {
      if (active) return;
      active = true;
      connection.value = 'connecting';
      open();
    },
    stop() {
      active = false;
      window.clearTimeout(timer);
      timer = undefined;
      const socket = source;
      source = undefined;
      socket?.close();
      connection.value = 'closed';
    },
    emit,
  };
}

export const events = createEventsClient();
