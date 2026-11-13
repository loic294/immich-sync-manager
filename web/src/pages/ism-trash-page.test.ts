import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import type { TrashEntry } from '@/api/generated/TrashEntry';
import { mount } from '@/test/helpers';
import '@/pages/ism-trash-page';

const ENTRY: TrashEntry = {
  trash_id: 't1',
  location: { root_id: 'photos', path: '2024/a.jpg' },
  is_folder: false,
  size: 2048,
  trashed_at: '2024-01-01T00:00:00Z',
  operation_id: null,
  asset_ids: ['asset-1'],
};

describe('ism-trash-page', () => {
  afterEach(() => vi.restoreAllMocks());

  it('restores an entry through a managed operation', async () => {
    vi.spyOn(api, 'get').mockImplementation(
      async (path: string) => (path === '/api/admin/trash' ? [ENTRY] : []) as never,
    );
    const post = vi
      .spyOn(api, 'post')
      .mockResolvedValue({ operation_id: 'op', status: 'queued' } as never);
    const page = await mount('ism-trash-page');
    await vi.waitFor(() => expect(page.textContent).toContain('photos:2024/a.jpg'));
    const restore = [...page.querySelectorAll('button')].find((b) =>
      b.textContent?.includes('Restore'),
    );
    restore?.click();
    await vi.waitFor(() => expect(post).toHaveBeenCalled());
    const [path, options] = post.mock.calls[0] ?? [];
    expect(path).toBe('/api/v1/operations');
    expect((options as { body: { actions: unknown } }).body.actions).toEqual([
      { kind: 'restore', trash_id: 't1' },
    ]);
  });
});
