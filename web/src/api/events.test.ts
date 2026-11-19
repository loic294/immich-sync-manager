import { afterEach, describe, expect, it, vi } from 'vitest';
import { type ChangeEvent, createEventsClient, type SocketLike } from '@/api/events';

class FakeSocket implements SocketLike {
  onopen: ((event: Event) => void) | null = null;
  onclose: ((event: CloseEvent) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  onmessage: ((event: MessageEvent<string>) => void) | null = null;
  closed = false;
  close() {
    this.closed = true;
  }
  send(data: string) {
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
    expect(seen).toEqual([{ type: 'resync' }]);
    sources[0]?.send('{"type":"sync_changed"}');
    expect(seen.at(-1)).toEqual({ type: 'sync_changed' });
    sources[0]?.onclose?.(new CloseEvent('close'));
    expect(client.connection.value).toBe('reconnecting');
    await vi.advanceTimersByTimeAsync(3000);
    sources[1]?.onopen?.(new Event('open'));
    expect(seen.at(-1)).toEqual({ type: 'resync' });
    sources[0]?.send('{"type":"operation_updated"}');
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
    sockets[0]?.send('{"type":"unknown"}');
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
});
