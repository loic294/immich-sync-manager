import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import type { MappingView } from '@/api/generated/MappingView';
import type { MountView } from '@/api/generated/MountView';
import { toast } from '@/components/toast';
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
  afterEach(() => {
    toast.clear();
    vi.restoreAllMocks();
  });

  it('browses deeply nested folders, lists immediate children, and maps the chosen folder', async () => {
    const folders = [
      '/mnt/homes',
      '/mnt/homes/example-user',
      '/mnt/homes/example-user/Photos',
      '/mnt/homes/example-user/Photos/2024',
    ];
    const get = vi.spyOn(api, 'get').mockImplementation(async (path, options) => {
      if (path !== '/api/admin/mounts') return [] as never;
      const parent = options?.query?.path;
      const index = typeof parent === 'string' ? folders.indexOf(parent) + 1 : 0;
      const child = folders[index];
      return (
        child ? [{ path: child, writable: true, mapped_root: null } satisfies MountView] : []
      ) as never;
    });
    const page = await mount('ism-mappings-page');
    const mountedFolders = () =>
      [...page.querySelectorAll('section')].find((s) => s.textContent?.includes('Mounted folders'));
    for (const folder of folders) {
      await vi.waitFor(() => {
        const row = mountedFolders()?.querySelector('tbody tr');
        expect(row?.querySelector('td')?.textContent?.trim()).toBe(folder);
        const browse = row?.querySelector<HTMLButtonElement>('button[aria-label^="Browse "]');
        expect(browse).not.toBeNull();
      });
      mountedFolders()?.querySelector<HTMLButtonElement>('button[aria-label^="Browse "]')?.click();
      await vi.waitFor(() =>
        expect(get).toHaveBeenCalledWith('/api/admin/mounts', { query: { path: folder } }),
      );
    }
    await vi.waitFor(() => expect(mountedFolders()?.textContent).toContain('No subfolders'));
    const map = mountedFolders()?.querySelector<HTMLButtonElement>(
      'button[aria-label="Map current folder"]',
    );
    expect(map).not.toBeNull();
    map?.click();
    await page.updateComplete;
    expect(page.querySelector<HTMLInputElement>('input[name="container_path"]')?.value).toBe(
      folders.at(-1),
    );
    page.querySelector<HTMLDialogElement>('dialog')?.close();
    mountedFolders()
      ?.querySelector<HTMLButtonElement>('button[aria-label="Go to parent folder"]')
      ?.click();
    await vi.waitFor(() =>
      expect(mountedFolders()?.querySelector('tbody td')?.textContent?.trim()).toBe(folders.at(-1)),
    );
    mountedFolders()?.querySelectorAll<HTMLButtonElement>('nav button')[1]?.click();
    await vi.waitFor(() =>
      expect(mountedFolders()?.querySelector('tbody td')?.textContent?.trim()).toBe(folders[1]),
    );
    mountedFolders()?.querySelector<HTMLButtonElement>('nav button')?.click();
    await vi.waitFor(() => {
      expect(mountedFolders()?.querySelector('tbody td')?.textContent?.trim()).toBe(folders[0]);
      expect(
        mountedFolders()?.querySelector<HTMLButtonElement>(
          'button[aria-label="Go to parent folder"]',
        )?.disabled,
      ).toBe(true);
    });
  });

  it('keeps the previous folder and allows retry after a browsing error', async () => {
    let unavailable = true;
    vi.spyOn(api, 'get').mockImplementation(async (path, options) => {
      if (path !== '/api/admin/mounts') return [] as never;
      if (options?.query?.path) {
        if (unavailable) throw new Error('The mounted folder is unavailable.');
        return [] as never;
      }
      return [{ path: '/mnt/photos', writable: true, mapped_root: null }] as never;
    });
    const error = vi.spyOn(toast, 'error');
    const page = await mount('ism-mappings-page');
    await vi.waitFor(() => expect(page.loading).toBe(false));
    page.querySelector<HTMLButtonElement>('button[aria-label="Browse /mnt/photos"]')?.click();
    await vi.waitFor(() => {
      expect(error).toHaveBeenCalledWith('The mounted folder is unavailable.');
      expect(page.busy).toBeUndefined();
    });
    expect(page.mountTrail).toEqual([]);
    expect(page.querySelector('button[aria-label="Browse /mnt/photos"]')).not.toBeNull();
    unavailable = false;
    page.querySelector<HTMLButtonElement>('button[aria-label="Browse /mnt/photos"]')?.click();
    await vi.waitFor(() => {
      expect(page.mountTrail).toEqual(['/mnt/photos']);
      expect(page.textContent).toContain('No subfolders');
    });
  });

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
