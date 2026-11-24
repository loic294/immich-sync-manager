import { type ReadonlySignal, signal } from '@preact/signals-core';
import { ApiError } from '@/api/errors';

export type ChangeType =
  | 'activity_changed'
  | 'operation_updated'
  | 'operation_committed'
  | 'maintenance_changed'
  | 'sync_changed'
  | 'conflicts_changed'
  | 'watch_changed'
  | 'library_scanned'
  | 'settings_changed'
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
  send(data: string): void;
}

export interface Snapshot {
  status: number;
  data: unknown;
}

export interface EventsClient {
  readonly connection: ReadonlySignal<ConnectionState>;
  readonly running: boolean;
  on(handler: (event: ChangeEvent) => void): () => void;
  subscribe(url: string, handler: (snapshot: Snapshot | Error) => void, body?: object): () => void;
  start(): void;
  stop(): void;
  emit(event: ChangeEvent): void;
}

const TYPES = new Set<ChangeType>([
  'activity_changed',
  'operation_updated',
  'operation_committed',
  'maintenance_changed',
  'sync_changed',
  'conflicts_changed',
  'watch_changed',
  'library_scanned',
  'settings_changed',
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
  let ready = false;
  let handshakeTimer: number | undefined;
  let nextId = 1;
  const subscriptions = new Map<
    string,
    {
      id: number;
      url: string;
      body?: object;
      handlers: Set<(snapshot: Snapshot | Error) => void>;
      latest?: Snapshot;
    }
  >();

  function unavailable() {
    return new ApiError({
      status: 0,
      code: 'live_unavailable',
      message:
        'Live updates are unavailable. Check the WebSocket connection; no polling fallback is used.',
    });
  }

  function notify(handler: (snapshot: Snapshot | Error) => void, value: Snapshot | Error) {
    try {
      handler(value);
    } catch (error) {
      console.error('[events] snapshot handler failed', error);
    }
  }

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
    ready = false;
    handshakeTimer = window.setTimeout(() => socket.onerror?.(new Event('error')), 10_000);
    const reconnect = () => {
      if (source !== socket || !active) return;
      source = undefined;
      ready = false;
      window.clearTimeout(handshakeTimer);
      for (const subscription of subscriptions.values()) {
        delete subscription.latest;
        for (const handler of subscription.handlers) notify(handler, unavailable());
      }
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
        if (typeof parsed === 'object' && parsed !== null && 'type' in parsed) {
          if (parsed.type === 'ready' && 'protocol' in parsed && parsed.protocol === 1) {
            if (ready) return;
            ready = true;
            connection.value = 'open';
            failures = 0;
            window.clearTimeout(handshakeTimer);
            emit({ type: 'resync' });
            for (const subscription of subscriptions.values())
              socket.send(
                JSON.stringify({
                  type: 'subscribe',
                  id: subscription.id,
                  url: subscription.url,
                  ...(subscription.body ? { body: subscription.body } : {}),
                }),
              );
            return;
          }
          if (
            parsed.type === 'snapshot' &&
            'id' in parsed &&
            typeof parsed.id === 'number' &&
            'status' in parsed &&
            typeof parsed.status === 'number' &&
            'data' in parsed
          ) {
            const subscription = [...subscriptions.values()].find((s) => s.id === parsed.id);
            if (subscription) {
              const snapshot: Snapshot = { status: parsed.status, data: parsed.data };
              subscription.latest = snapshot;
              for (const handler of subscription.handlers) notify(handler, snapshot);
            }
            return;
          }
          if (parsed.type === 'protocol_error') {
            console.error('[events] live protocol error', parsed);
            reconnect();
            return;
          }
        }
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
    get running() {
      return active;
    },
    subscribe(url, handler, body) {
      const key = body ? `${url}\n${JSON.stringify(body)}` : url;
      let subscription = subscriptions.get(key);
      if (!subscription) {
        subscription = { id: nextId++, url, ...(body ? { body } : {}), handlers: new Set() };
        subscriptions.set(key, subscription);
        if (ready)
          source?.send(
            JSON.stringify({
              type: 'subscribe',
              id: subscription.id,
              url,
              ...(body ? { body } : {}),
            }),
          );
      }
      subscription.handlers.add(handler);
      if (subscription.latest) notify(handler, subscription.latest);
      return () => {
        subscription.handlers.delete(handler);
        if (subscription.handlers.size > 0) return;
        subscriptions.delete(key);
        if (ready) source?.send(JSON.stringify({ type: 'unsubscribe', id: subscription.id }));
      };
    },
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
      ready = false;
      window.clearTimeout(handshakeTimer);
      for (const subscription of subscriptions.values()) {
        delete subscription.latest;
        for (const handler of subscription.handlers) notify(handler, unavailable());
      }
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
