import { vi } from 'vitest';
import { events, type Snapshot } from '@/api/events';

/** Deliver server snapshots independently of the initial HTTP fixture. */
export function mockLiveSnapshots() {
  const handlers = new Map<string, Set<(snapshot: Snapshot | Error) => void>>();
  const bodies = new Map<string, object | undefined>();
  vi.spyOn(events, 'subscribe').mockImplementation((url, handler, body) => {
    const key = body ? `${url}\n${JSON.stringify(body)}` : url;
    bodies.set(key, body);
    let listeners = handlers.get(key);
    if (!listeners) {
      listeners = new Set();
      handlers.set(key, listeners);
    }
    listeners.add(handler);
    return () => {
      listeners.delete(handler);
      if (!listeners.size) {
        handlers.delete(key);
        bodies.delete(key);
      }
    };
  });
  return {
    handlers,
    push(path: string, data: unknown, status = 200) {
      for (const [key, listeners] of handlers) {
        const url = key.split('\n')[0];
        if ((path.includes('?') ? url : url.split('?')[0]) !== path) continue;
        const value = typeof data === 'function' ? data(bodies.get(key)) : data;
        for (const handler of [...listeners]) handler({ status, data: value });
      }
    },
    fail(path: string, message: string) {
      for (const [key, listeners] of handlers) {
        const url = key.split('\n')[0];
        if (url.split('?')[0] !== path) continue;
        for (const handler of [...listeners]) handler(new Error(message));
      }
    },
  };
}
