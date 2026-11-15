import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import type { MappingView } from '@/api/generated/MappingView';
import { mount } from '@/test/helpers';
import '@/pages/ism-mappings-page';

const MAPPING: MappingView = {
  root_id: 'photos',
  name: 'Photos',
  container_path: '/mnt/photos',
  server_id: 's1',
  library_id: 'lib1',
  immich_import_path: '/library/photos',
  opm_prefix: null,
  available: true,
  availability_detail: null,
  watch: {
    root_id: 'photos',
    name: 'Photos',
    watched: true,
    mode: 'auto',
    poll_interval_secs: 300,
    effective_mode: 'poll',
    fs_type: 'smbfs',
    last_pass_at: '2024-01-01T00:00:00Z',
    last_change_at: null,
    paused: false,
    warnings: [],
    files: 3,
  },
};

describe('ism-mappings-page', () => {
  afterEach(() => vi.restoreAllMocks());

  it('saves the watch mode and polling interval of a mapping', async () => {
    vi.spyOn(api, 'get').mockImplementation(
      async (path: string) => (path === '/api/admin/mappings' ? [MAPPING] : []) as never,
    );
    const put = vi.spyOn(api, 'put').mockResolvedValue(MAPPING as never);
    const page = await mount('ism-mappings-page');
    await vi.waitFor(() => expect(page.textContent).toContain('every 5 min'));
    page.querySelector<HTMLButtonElement>('button[aria-label="Change detection"]')?.click();
    await page.updateComplete;
    const dialog = [...page.querySelectorAll('dialog')].find((d) =>
      d.textContent?.includes('Change detection for Photos'),
    );
    const form = dialog?.querySelector('form');
    const select = form?.querySelector<HTMLSelectElement>('select[name="mode"]');
    const minutes = form?.querySelector<HTMLInputElement>('input[name="poll_minutes"]');
    if (!form || !select || !minutes) throw new Error('watch form missing');
    select.value = 'poll';
    minutes.value = '15';
    form.dispatchEvent(new SubmitEvent('submit', { cancelable: true }));
    await vi.waitFor(() =>
      expect(put).toHaveBeenCalledWith('/api/admin/mappings/photos/watch', {
        body: { mode: 'poll', poll_interval_secs: 900 },
      }),
    );
  });
});
