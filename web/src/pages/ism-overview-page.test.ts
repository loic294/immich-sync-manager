import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import type { ImmichServerView } from '@/api/generated/ImmichServerView';
import type { Overview } from '@/api/generated/Overview';
import { mount } from '@/test/helpers';
import '@/pages/ism-overview-page';

const SERVER: ImmichServerView = {
  id: 's1',
  name: 'Family server',
  url: 'https://immich.example',
  version: '3.4.1',
  compatible: true,
  compatibility_detail: null,
  database_configured: true,
  direct_writes_enabled: false,
  external_library_move_mode: 'api_with_db_fallback',
  external_library_api_capable: true,
  admin_account_id: 'a1',
  maintenance: {
    phase: 'idle',
    epoch: 1,
    active_ops: 0,
    queue_paused: false,
    drift: "Immich's file watcher is on.",
    message: null,
  },
};

const OVERVIEW: Overview = {
  instance_name: 'Home',
  servers: [SERVER],
  roots_total: 1,
  roots_unavailable: 0,
  operations_running: 0,
  operations_attention: 0,
  transfers_queued: 0,
  conflicts_open: 0,
  peers_online: 0,
  peers_total: 0,
  uncovered_libraries: 0,
  watchers: [
    {
      root_id: 'photos',
      name: 'Photos',
      watched: true,
      mode: 'auto',
      poll_interval_secs: 300,
      effective_mode: 'poll',
      fs_type: 'nfs',
      last_pass_at: '2024-01-01T00:00:00Z',
      last_change_at: null,
      paused: true,
      warnings: ['The folder is suddenly empty; detection paused.'],
      files: 12,
    },
  ],
  scans: [
    {
      library_id: 'lib1',
      library_name: 'Family library',
      server_id: 's1',
      due_at: new Date(Date.now() + 90_000).toISOString(),
      due_in_secs: 90,
      reasons: ['photos changed'],
      running: false,
      waiting_for_barrier: false,
      last_started_at: null,
      last_finished_at: null,
      last_error: null,
    },
  ],
};

describe('ism-overview-page', () => {
  afterEach(() => vi.restoreAllMocks());

  it('shows watchers, scan countdowns and re-applies drifted Immich settings', async () => {
    vi.spyOn(api, 'get').mockImplementation(
      async (path) =>
        (path === '/api/admin/operations/page' ? { rows: [], total: 0 } : OVERVIEW) as never,
    );
    const post = vi.spyOn(api, 'post').mockResolvedValue(undefined as never);
    const page = await mount('ism-overview-page');
    await vi.waitFor(() => expect(page.textContent).toContain('Family library'));
    expect(page.textContent).toContain('suddenly empty');
    expect(page.textContent).toMatch(/1:(2|3)\d/);
    expect(page.textContent).toContain('writes are blocked');
    const reapply = [...page.querySelectorAll('button')].find((b) =>
      b.textContent?.includes('Turn them off again'),
    );
    reapply?.click();
    await vi.waitFor(() =>
      expect(post).toHaveBeenCalledWith('/api/admin/servers/s1/maintenance/reapply'),
    );
  });
});
