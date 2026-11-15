import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import { ApiError } from '@/api/errors';
import { mount } from '@/test/helpers';
import '@/pages/ism-sync-page';

describe('ism-sync-page', () => {
  afterEach(() => vi.restoreAllMocks());

  it('offers an explicit, initially closed replacement for a corrupt schedule', async () => {
    const invalid = new ApiError({
      status: 409,
      code: 'invalid_schedule',
      message: 'The saved schedule is invalid.',
    });
    vi.spyOn(api, 'get').mockImplementation(async (path) => {
      if (path === '/api/admin/sync' || path === '/api/admin/sync/schedule') throw invalid;
      return [] as never;
    });
    const put = vi.spyOn(api, 'put').mockResolvedValue({
      timezone: 'UTC',
      windows: [{ days: [], start: '00:00', end: '00:00' }],
    } as never);
    const page = await mount('ism-sync-page');
    await vi.waitFor(() => expect(page.textContent).toContain('saved schedule is invalid'));
    expect(put).not.toHaveBeenCalled();
    const save = [...page.querySelectorAll('button')].find((button) =>
      button.textContent?.includes('Save hours'),
    );
    expect(save).toBeDefined();
    save?.click();
    await vi.waitFor(() =>
      expect(put).toHaveBeenCalledWith('/api/admin/sync/schedule', {
        body: {
          timezone: 'UTC',
          windows: [{ days: [], start: '00:00', end: '00:00' }],
        },
      }),
    );
  });
});
