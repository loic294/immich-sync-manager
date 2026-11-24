import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import { events, type Snapshot } from '@/api/events';
import { LiveScope } from '@/api/live';

afterEach(() => vi.restoreAllMocks());

describe('live snapshot reads', () => {
  it('uses pushed data for initial and subsequent reads without HTTP, and releases subscriptions', async () => {
    vi.spyOn(events, 'running', 'get').mockReturnValue(true);
    const get = vi.spyOn(api, 'get');
    const stop = vi.fn();
    let push: ((snapshot: Snapshot | Error) => void) | undefined;
    const subscribe = vi.spyOn(events, 'subscribe').mockImplementation((_url, handler) => {
      push = handler;
      return stop;
    });
    const changed = vi.fn();
    const scope = new LiveScope(changed);
    const first = scope.get('/api/admin/sync/page', { query: { status: 'active', offset: 250 } });
    expect(subscribe).toHaveBeenCalledWith(
      '/sync/page?status=active&offset=250',
      expect.any(Function),
    );
    push?.({ status: 200, data: { rows: [{ bytes_done: 1 }], total: 501 } });
    await expect(first).resolves.toEqual({ rows: [{ bytes_done: 1 }], total: 501 });
    push?.({ status: 200, data: { rows: [{ bytes_done: 2 }], total: 501 } });
    await expect(
      scope.get('/api/admin/sync/page', { query: { status: 'active', offset: 250 } }),
    ).resolves.toEqual({ rows: [{ bytes_done: 2 }], total: 501 });
    expect(changed).toHaveBeenCalledOnce();
    expect(get).not.toHaveBeenCalled();
    scope.dispose();
    expect(stop).toHaveBeenCalledOnce();
  });

  it('fails visibly without an HTTP fallback, then recovers from a new snapshot', async () => {
    vi.spyOn(events, 'running', 'get').mockReturnValue(true);
    const get = vi.spyOn(api, 'get');
    let push: ((snapshot: Snapshot | Error) => void) | undefined;
    vi.spyOn(events, 'subscribe').mockImplementation((_url, handler) => {
      push = handler;
      return () => {};
    });
    const changed = vi.fn();
    const scope = new LiveScope(changed);
    const initial = scope.get('/api/admin/overview');
    const rejected = expect(initial).rejects.toThrow('Live connection lost');
    push?.(new Error('Live connection lost'));
    await rejected;
    push?.({ status: 200, data: { operations_running: 2 } });
    await expect(scope.get('/api/admin/overview')).resolves.toEqual({ operations_running: 2 });
    expect(changed).toHaveBeenCalledOnce();
    expect(get).not.toHaveBeenCalled();
    scope.dispose();
  });

  it('preserves server error codes and cancels reads when a view closes', async () => {
    vi.spyOn(events, 'running', 'get').mockReturnValue(true);
    let push: ((snapshot: Snapshot | Error) => void) | undefined;
    vi.spyOn(events, 'subscribe').mockImplementation((_url, handler) => {
      push = handler;
      return () => {};
    });
    const scope = new LiveScope(vi.fn());
    const initial = scope.get('/api/admin/sync');
    const rejected = expect(initial).rejects.toMatchObject({
      code: 'invalid_schedule',
      status: 422,
    });
    push?.({
      status: 422,
      data: { error: { code: 'invalid_schedule', message: 'Invalid schedule' } },
    });
    await rejected;
    const next = scope.get('/api/admin/sync/page');
    const cancelled = expect(next).rejects.toMatchObject({ name: 'AbortError' });
    scope.dispose();
    await cancelled;
  });
});
