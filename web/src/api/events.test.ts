import { afterEach, describe, expect, it, vi } from 'vitest';
import { type ChangeEvent, createEventsClient, type SocketLike } from '@/api/events';

class FakeSocket implements SocketLike {
  onopen: ((event: Event) => void) | null = null;
  onclose: ((event: CloseEvent) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  onmessage: ((event: MessageEvent<string>) => void) | null = null;
  closed = false;
  sent: string[] = [];
  close() {
    this.closed = true;
  }
  send(data: string) {
    this.sent.push(data);
  }
  receive(data: string) {
    this.onmessage?.(new MessageEvent('message', { data }));
  }
}

describe('WebSocket events client', () => {
  afterEach(() => vi.useRealTimers());
  it('resyncs on connection, delivers changes and reconnects after a clean close', async () => {
    vi.useFakeTimers();
    const sources: FakeSocket[] = [];
    const factory = vi.fn((_url: string) => {
      const source = new FakeSocket();
      sources.push(source);
      return source;
    });
    const client = createEventsClient('/api/admin/live', factory);
    const seen: ChangeEvent[] = [];
    client.on((event) => seen.push(event));
    client.start();
    client.start();
    expect(factory).toHaveBeenCalledOnce();
    expect(factory.mock.calls[0]?.[0]).toMatch(/^ws:\/\/.*\/api\/admin\/live$/);
    sources[0]?.onopen?.(new Event('open'));
    sources[0]?.receive('{"type":"ready","protocol":1}');
    expect(seen).toEqual([{ type: 'resync' }]);
    sources[0]?.receive('{"type":"sync_changed"}');
    expect(seen.at(-1)).toEqual({ type: 'sync_changed' });
    sources[0]?.receive('{"type":"settings_changed"}');
    expect(seen.at(-1)).toEqual({ type: 'settings_changed' });
    sources[0]?.onclose?.(new CloseEvent('close'));
    expect(client.connection.value).toBe('reconnecting');
    await vi.advanceTimersByTimeAsync(3000);
    sources[1]?.onopen?.(new Event('open'));
    sources[1]?.receive('{"type":"ready","protocol":1}');
    expect(seen.at(-1)).toEqual({ type: 'resync' });
    sources[0]?.receive('{"type":"operation_updated"}');
    expect(seen.at(-1)).toEqual({ type: 'resync' });
    client.stop();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(factory).toHaveBeenCalledTimes(2);
    expect(client.connection.value).toBe('closed');
  });

  it('backs off after errors, ignores unknown events and cancels pending reconnects', async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const client = createEventsClient('/api/admin/live', () => {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket;
    });
    const handler = vi.fn();
    client.on(handler);
    client.start();
    sockets[0]?.receive('{"type":"unknown"}');
    expect(handler).not.toHaveBeenCalled();
    sockets[0]?.onerror?.(new Event('error'));
    await vi.advanceTimersByTimeAsync(3000);
    sockets[1]?.onerror?.(new Event('error'));
    await vi.advanceTimersByTimeAsync(5999);
    expect(sockets).toHaveLength(2);
    client.stop();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(sockets).toHaveLength(2);
  });

  it('shares snapshot subscriptions, restores them after reconnect, and unsubscribes the last view', async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const client = createEventsClient('/api/admin/live', () => {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket;
    });
    const first = vi.fn();
    const second = vi.fn();
    const stopFirst = client.subscribe('/overview', first);
    const stopSecond = client.subscribe('/overview', second);
    client.start();
    sockets[0]?.onopen?.(new Event('open'));
    sockets[0]?.receive('{"type":"ready","protocol":1}');
    expect(sockets[0]?.sent).toEqual(['{"type":"subscribe","id":1,"url":"/overview"}']);
    sockets[0]?.receive('{"type":"snapshot","id":1,"status":200,"data":{"operations_running":2}}');
    expect(first).toHaveBeenLastCalledWith({ status: 200, data: { operations_running: 2 } });
    expect(second).toHaveBeenLastCalledWith({ status: 200, data: { operations_running: 2 } });
    stopFirst();
    expect(sockets[0]?.sent).toHaveLength(1);
    sockets[0]?.onclose?.(new CloseEvent('close'));
    expect(second).toHaveBeenLastCalledWith(expect.objectContaining({ code: 'live_unavailable' }));
    await vi.advanceTimersByTimeAsync(3000);
    sockets[1]?.onopen?.(new Event('open'));
    sockets[1]?.receive('{"type":"ready","protocol":1}');
    expect(sockets[1]?.sent).toEqual(['{"type":"subscribe","id":1,"url":"/overview"}']);
    stopSecond();
    expect(sockets[1]?.sent.at(-1)).toBe('{"type":"unsubscribe","id":1}');
    client.stop();
  });

  it('reports an incompatible or stalled WebSocket rather than falling back to HTTP', async () => {
    vi.useFakeTimers();
    const socket = new FakeSocket();
    const client = createEventsClient('/api/admin/live', () => socket);
    const handler = vi.fn();
    const stop = client.subscribe('/overview', handler);
    client.start();
    socket.onopen?.(new Event('open'));
    await vi.advanceTimersByTimeAsync(10_000);
    expect(client.connection.value).toBe('reconnecting');
    expect(handler).toHaveBeenCalledWith(expect.objectContaining({ code: 'live_unavailable' }));
    stop();
    client.stop();
  });
});
