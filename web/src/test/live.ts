import { vi } from 'vitest';
import { events, type Snapshot } from '@/api/events';

/** Deliver server snapshots independently of the initial HTTP fixture. */
export function mockLiveSnapshots() {
  const handlers = new Map<string, Set<(snapshot: Snapshot | Error) => void>>();
  vi.spyOn(events, 'subscribe').mockImplementation((url, handler) => {
    let listeners = handlers.get(url);
    if (!listeners) {
      listeners = new Set();
      handlers.set(url, listeners);
    }
    listeners.add(handler);
    return () => {
      listeners.delete(handler);
      if (!listeners.size) handlers.delete(url);
    };
  });
  return {
    handlers,
    push(path: string, data: unknown, status = 200) {
      for (const [url, listeners] of handlers) {
        if ((path.includes('?') ? url : url.split('?')[0]) !== path) continue;
        for (const handler of [...listeners]) handler({ status, data });
      }
    },
    fail(path: string, message: string) {
      for (const [url, listeners] of handlers) {
        if (url.split('?')[0] !== path) continue;
        for (const handler of [...listeners]) handler(new Error(message));
      }
    },
  };
}
