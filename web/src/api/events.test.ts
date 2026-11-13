import { describe, expect, it, vi } from 'vitest';
import { type ChangeEvent, createEventsClient, type EventSourceLike } from '@/api/events';

class FakeSource implements EventSourceLike {
  onopen: ((event: Event) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  listeners = new Map<string, (event: MessageEvent<string>) => void>();
  closed = false;
  addEventListener(type: string, listener: (event: MessageEvent<string>) => void) {
    this.listeners.set(type, listener);
  }
  close() {
    this.closed = true;
  }
  send(type: string, data: string) {
    this.listeners.get(type)?.(new MessageEvent(type, { data }));
  }
}

describe('events client', () => {
  it('delivers changes and asks for a resync after reconnecting', async () => {
    vi.useFakeTimers();
    const sources: FakeSource[] = [];
    const client = createEventsClient('/events', () => {
      const source = new FakeSource();
      sources.push(source);
      return source;
    });
    const seen: ChangeEvent[] = [];
    client.on((event) => seen.push(event));
    client.start();
    sources[0]?.onopen?.(new Event('open'));
    expect(client.connection.value).toBe('open');

    sources[0]?.send('change', '{"type":"sync_changed"}');
    sources[0]?.send('change', 'not json');
    expect(seen).toEqual([{ type: 'sync_changed' }]);

    sources[0]?.onerror?.(new Event('error'));
    expect(client.connection.value).toBe('reconnecting');
    expect(sources[0]?.closed).toBe(true);
    await vi.advanceTimersByTimeAsync(3000);
    sources[1]?.onopen?.(new Event('open'));
    expect(seen.at(-1)).toEqual({ type: 'resync' });

    client.stop();
    expect(client.connection.value).toBe('closed');
    vi.useRealTimers();
  });
});
