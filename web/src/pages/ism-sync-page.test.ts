import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import { ApiError } from '@/api/errors';
import { events } from '@/api/events';
import type { SyncActivity } from '@/api/generated/SyncActivity';
import { mount } from '@/test/helpers';
import '@/pages/ism-sync-page';

describe('ism-sync-page', () => {
  afterEach(() => vi.restoreAllMocks());

  it('uses one live paginated table with direction, previews and completed tabs', async () => {
    const transfer: SyncActivity = {
      id: 'tr1',
      peer_id: 'p1',
      share_name: 'Photos',
      relative_path: 'Trip/photo.jpg',
      location: { root_id: 'photos', path: 'Trip/photo.jpg' },
      source: 'ISM: Remote',
      destination: 'ISM: Archive',
      destination_path: '/example/photos/Trip/photo.jpg',
      status: 'running',
      size: 100,
      bytes_done: 25,
      error: null,
      queued_at: '2026-01-01T00:00:00Z',
      finished_at: null,
      operation_id: null,
    };
    let done = 25;
    const get = vi.spyOn(api, 'get').mockImplementation(async (path) => {
      if (path === '/api/admin/sync')
        return {
          allowed_now: true,
          override_active: null,
          queued_total: 1,
          failed_total: 0,
        } as never;
      if (path === '/api/admin/sync/page')
        return { rows: [{ ...transfer, bytes_done: done }], total: 501 } as never;
      if (path === '/api/admin/sync/schedule') return { timezone: 'UTC', windows: [] } as never;
      if (path === '/api/admin/sync/files')
        return {
          folder: '/example/remote/Trip',
          files: [
            { path: 'Trip/photo.jpg', role: 'original' },
            { path: 'Trip/photo.jpg.xmp', role: 'sidecar' },
          ],
        } as never;
      return [] as never;
    });
    const page = await mount('ism-sync-page');
    await vi.waitFor(() => expect(page.textContent).toContain('ISM: Remote'));
    expect(page.querySelectorAll('table')).toHaveLength(1);
    expect([...page.querySelectorAll('th')].map((cell) => cell.textContent)).not.toContain(
      'Queued',
    );
    expect(page.querySelectorAll('th')).toHaveLength(5);
    expect(page.querySelector('ism-sync-thumbnail img')?.getAttribute('src')).toContain(
      'peer_id=p1',
    );
    expect(get).toHaveBeenCalledWith('/api/admin/sync/page', {
      query: { status: 'active', limit: 250, offset: 0 },
    });
    done = 70;
    events.emit({ type: 'sync_changed' });
    await vi.waitFor(() => expect(page.textContent).toContain('70%'));
    page.querySelector<HTMLButtonElement>('#sync-tab-completed')?.click();
    await vi.waitFor(() =>
      expect(get).toHaveBeenCalledWith('/api/admin/sync/page', {
        query: { status: 'completed', limit: 250, offset: 0 },
      }),
    );
    await page.updateComplete;
    expect([...page.querySelectorAll('th')].map((cell) => cell.textContent)).toContain('Finished');
    page.querySelector<HTMLButtonElement>('tbody button')?.click();
    await vi.waitFor(() => expect(page.textContent).toContain('/example/remote/Trip'));
    expect(page.textContent).toContain('photo.jpg.xmp');
    expect(page.textContent).toContain('/example/photos/Trip/photo.jpg');
    expect(page.querySelector('dialog[open]')).not.toBeNull();
    expect([...page.querySelectorAll('dialog[open] dt')].map((term) => term.textContent)).toContain(
      'Queued',
    );
  });

  it('offers an explicit, initially closed replacement for a corrupt schedule', async () => {
    const invalid = new ApiError({
      status: 409,
      code: 'invalid_schedule',
      message: 'The saved schedule is invalid.',
    });
    vi.spyOn(api, 'get').mockImplementation(async (path) => {
      if (path === '/api/admin/sync' || path === '/api/admin/sync/schedule') throw invalid;
      if (path === '/api/admin/sync/page') return { rows: [], total: 0 } as never;
      return [] as never;
    });
    const put = vi.spyOn(api, 'put').mockResolvedValue({
      timezone: 'UTC',
      windows: [{ days: [], start: '00:00', end: '00:00' }],
    } as never);
    const page = await mount('ism-sync-page');
    await vi.waitFor(() => expect(page.textContent).toContain('saved schedule is invalid'));
    expect(put).not.toHaveBeenCalled();
    expect(page.querySelector('dialog[open]')).toBeNull();
    [...page.querySelectorAll('button')]
      .find((button) => button.textContent?.includes('Hours & overrides'))
      ?.click();
    expect(page.querySelector('dialog[open]')).not.toBeNull();
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
