import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import { ApiError } from '@/api/errors';
import { events } from '@/api/events';
import type { SyncActivity } from '@/api/generated/SyncActivity';
import type { SyncProgress } from '@/api/generated/SyncProgress';
import { mount } from '@/test/helpers';
import '@/pages/ism-sync-page';

describe('ism-sync-page', () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it('refreshes transfer statistics every five seconds and stops when disconnected', async () => {
    vi.useFakeTimers();
    const get = vi.spyOn(api, 'get').mockImplementation(async (path) => {
      if (path === '/api/admin/sync') return { allowed_now: true, override_active: null } as never;
      if (path === '/api/admin/sync/page') return { rows: [], total: 0 } as never;
      if (path === '/api/admin/sync/schedule') return { timezone: 'UTC', windows: [] } as never;
      return [] as never;
    });
    const page = await mount('ism-sync-page');
    await page.reload();
    get.mockClear();
    await vi.advanceTimersByTimeAsync(4999);
    expect(get).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(get).toHaveBeenCalledWith('/api/admin/sync/page', {
      query: { status: 'active', limit: 250, offset: 0 },
    });
    get.mockClear();
    page.remove();
    await vi.advanceTimersByTimeAsync(5000);
    expect(get).not.toHaveBeenCalled();
  });

  it('uses one live paginated table with direction, previews and completed tabs', async () => {
    const transfer: SyncActivity = {
      id: 'tr1',
      outgoing: false,
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
      readd_available: false,
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
    page.querySelector<HTMLDialogElement>('ism-sync-detail dialog')?.close();
    transfer.outgoing = true;
    transfer.operation_id = 'remote-publication';
    await page.reload();
    await page.updateComplete;
    page.querySelector<HTMLButtonElement>('tbody button')?.click();
    await vi.waitFor(() =>
      expect(get).toHaveBeenCalledWith('/api/admin/sync/files', {
        query: { share: 'Photos', path: 'Trip/photo.jpg', peer_id: undefined },
      }),
    );
    expect(page.querySelector('ism-sync-thumbnail img')?.getAttribute('src')).not.toContain(
      'peer_id',
    );
    const buttons = [...page.querySelectorAll('ism-sync-detail button')].map(
      (button) => button.textContent,
    );
    expect(buttons).not.toContain('View publishing operation');
    expect(buttons).not.toContain('Cancel transfer');
    expect(page.textContent).toContain('The receiving instance manages this transfer');
  });

  it('shows current-run file and byte totals, a byte-based progress bar, speed and ETA', async () => {
    const progress: SyncProgress = {
      files_total: 8,
      files_completed: 2,
      bytes_total: 10 * 1024 * 1024,
      bytes_done: 4 * 1024 * 1024,
      bytes_per_second: 1024 * 1024,
      eta_seconds: 6,
      queued: 3,
      running: 3,
      publishing: 0,
      failed: 0,
    };
    let warnings: string[] = [];
    vi.spyOn(api, 'get').mockImplementation(async (path) => {
      if (path === '/api/admin/sync') return { allowed_now: true, override_active: null } as never;
      if (path === '/api/admin/sync/page')
        return { rows: [], total: 0, progress: { ...progress }, warnings } as never;
      if (path === '/api/admin/sync/schedule') return { timezone: 'UTC', windows: [] } as never;
      return [] as never;
    });
    const page = await mount('ism-sync-page');
    await vi.waitFor(() => expect(page.textContent).toContain('2 of 8 files transferred'));
    expect(page.textContent).toContain('4.0 MB of 10 MB');
    expect(page.textContent).toContain('8.4 Mbps');
    expect(page.textContent).toContain('last minute');
    expect(page.textContent).toContain('6 sec remaining');
    expect(
      page.querySelector<HTMLProgressElement>('[aria-label="Total sync progress"]')?.value,
    ).toBe(40);

    progress.bytes_per_second = 0;
    progress.eta_seconds = null;
    warnings = ['Outgoing sync with Remote is not up to date.'];
    await page.reload();
    await page.updateComplete;
    expect(page.textContent).toContain('Waiting for transfer speed');
    expect(page.textContent).toContain('Outgoing sync with Remote is not up to date.');
    expect(page.textContent).not.toContain('6 sec remaining');

    progress.bytes_done = progress.bytes_total;
    progress.files_completed = 7;
    progress.publishing = 1;
    await page.reload();
    await page.updateComplete;
    expect(page.textContent).toContain('Finishing publication');
    expect(page.textContent).toContain('7 of 8 files transferred');
    progress.publishing = 0;
    progress.files_completed = 8;
    await page.reload();
    await page.updateComplete;
    expect(page.textContent).toContain('Complete');
  });

  it('keeps failed peer operations visible until they are explicitly re-added', async () => {
    const failed: SyncActivity = {
      id: 'operation:op-peer:0',
      outgoing: false,
      peer_id: 'p1',
      share_name: 'Photos',
      relative_path: 'Trip/new.jpg',
      location: { root_id: 'photos', path: 'Trip/new.jpg' },
      source: 'ISM: Remote',
      destination: 'ISM: Archive',
      destination_path: '/example/photos/Trip/new.jpg',
      status: 'failed',
      size: 0,
      bytes_done: 0,
      error: 'target already exists',
      queued_at: '2026-01-01T00:00:00Z',
      finished_at: '2026-01-01T00:01:00Z',
      operation_id: 'op-peer',
      readd_available: true,
    };
    vi.spyOn(api, 'get').mockImplementation(async (path) => {
      if (path === '/api/admin/sync') return { allowed_now: true, override_active: null } as never;
      if (path === '/api/admin/sync/page') return { rows: [failed], total: 1 } as never;
      if (path === '/api/admin/sync/schedule') return { timezone: 'UTC', windows: [] } as never;
      if (path === '/api/admin/sync/files')
        return { folder: '/example/photos/Trip', files: [] } as never;
      return [] as never;
    });
    const post = vi.spyOn(api, 'post').mockResolvedValue(undefined as never);
    const page = await mount('ism-sync-page');
    await vi.waitFor(() => expect(page.textContent).toContain('target already exists'));
    page.querySelector<HTMLButtonElement>('tbody button')?.click();
    await vi.waitFor(() => expect(page.textContent).toContain('Re-add to sync queue'));
    [...page.querySelectorAll<HTMLButtonElement>('ism-sync-detail button')]
      .find((button) => button.textContent?.includes('Re-add to sync queue'))
      ?.click();
    await vi.waitFor(() =>
      expect(post).toHaveBeenCalledWith('/api/admin/sync/operations/op-peer/0/readd'),
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
