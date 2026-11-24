import { afterEach, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import { events } from '@/api/events';
import { overview, startOverview } from '@/stores/overview';

afterEach(() => vi.restoreAllMocks());

it('does not turn a stream of activity notifications into overview HTTP requests', async () => {
  const get = vi.spyOn(api, 'get').mockResolvedValue(undefined);
  const stop = startOverview();
  try {
    await vi.waitFor(() => expect(get).toHaveBeenCalledOnce());
    for (let i = 0; i < 20; i++) {
      events.emit({ type: 'activity_changed' });
      await new Promise((resolve) => window.setTimeout(resolve, 0));
    }
    expect(get).toHaveBeenCalledOnce();
  } finally {
    stop();
    overview.value = undefined;
  }
});
