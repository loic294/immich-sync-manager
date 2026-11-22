import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import type { ConflictView } from '@/api/generated/ConflictView';
import { mount } from '@/test/helpers';
import '@/pages/ism-conflicts-page';

vi.mock('@/components/confirm', () => ({
  confirmAction: vi.fn(async () => true),
}));

const side = {
  instance: 'Example',
  sha256: null,
  size: 10,
  modified_at: null,
  deleted: false,
};
const conflict: ConflictView = {
  id: 'conflict-1',
  share_name: 'Photos',
  location: { root_id: 'photos', path: 'a.jpg' },
  kind: 'changed_on_both',
  detail: '',
  local: { ...side, instance: 'Alpha' },
  remote: { ...side, instance: 'Beta' },
  peer_id: 'peer-1',
  status: 'open',
  created_at: '2024-01-01T00:00:00Z',
  moved_from: null,
};

const versions = {
  share: 'Photos',
  revision: 'a'.repeat(64),
  receiving: [{ instance: 'Alpha', path: 'old', state: 'folder', files: [], directories: ['old'] }],
  source: { instance: 'Beta', path: 'new', state: 'folder', files: [], directories: ['new'] },
};

describe('ism-conflicts-page', () => {
  afterEach(() => vi.restoreAllMocks());

  it('offers named recoveries including keeping both folders, without dismissal', async () => {
    vi.spyOn(api, 'get').mockResolvedValue([
      conflict,
      {
        ...conflict,
        id: 'conflict-2',
        kind: 'folder_move',
        location: { ...conflict.location, path: 'b' },
        moved_from: { root_id: 'photos', path: 'old' },
      },
    ] as never);
    const page = await mount('ism-conflicts-page');
    await vi.waitFor(() => expect(page.querySelectorAll('article')).toHaveLength(2));
    const [fileCard, folderCard] = page.querySelectorAll('article');
    const labels = (card: Element) =>
      [...card.querySelectorAll('button')].map((button) => [
        button.textContent?.trim(),
        button.disabled,
      ]);
    expect(labels(fileCard)).toEqual([
      ['Compare versions', false],
      ['Keep Alpha', true],
      ['Keep Beta', true],
      ['Keep both', true],
    ]);
    expect(labels(folderCard)).toEqual(labels(fileCard));
    expect(folderCard.textContent).toContain('Moved by the peer from');
  });

  it('queues a folder version decision through shared recovery and collapses diagnostics', async () => {
    vi.spyOn(api, 'get').mockImplementation(async (path) =>
      path.endsWith('/versions')
        ? (versions as never)
        : ([{ ...conflict, kind: 'folder_move', detail: 'Destination content differs.' }] as never),
    );
    const post = vi.spyOn(api, 'post').mockResolvedValue({ items: [] } as never);
    const page = await mount('ism-conflicts-page');
    await page.reload();
    await page.updateComplete;
    expect(page.querySelector('details')?.hasAttribute('open')).toBe(false);
    [...page.querySelectorAll<HTMLButtonElement>('button')]
      .find((button) => button.textContent?.trim() === 'Compare versions')
      ?.click();
    await vi.waitFor(() => expect(page.querySelector('ism-recovery-versions')).not.toBeNull());
    await vi.waitFor(() => expect(page.busy).toBeUndefined());
    [...page.querySelectorAll<HTMLButtonElement>('button')]
      .find((button) => button.textContent?.trim() === 'Keep Beta')
      ?.click();
    await vi.waitFor(() =>
      expect(post).toHaveBeenCalledWith('/api/admin/recovery/decisions', {
        body: {
          decisions: [
            {
              target: { kind: 'conflict', id: conflict.id, peer_id: null },
              revision: versions.revision,
            },
          ],
          resolution: 'keep_remote',
        },
      }),
    );
  });

  it('selects conflicts and applies one reviewed choice to all of them, skipping changed ones', async () => {
    const second = {
      ...conflict,
      id: 'conflict-2',
      location: { root_id: 'photos', path: 'b.jpg' },
    };
    let changed = false;
    vi.spyOn(api, 'get').mockImplementation(async (path, options) => {
      if (!path.endsWith('/versions')) return [conflict, second] as never;
      return {
        ...versions,
        revision:
          changed && options?.query?.id === 'conflict-2' ? 'b'.repeat(64) : versions.revision,
      } as never;
    });
    const post = vi.spyOn(api, 'post').mockResolvedValue({ items: [] } as never);
    const page = await mount('ism-conflicts-page');
    await vi.waitFor(() => expect(page.querySelectorAll('article')).toHaveLength(2));
    page.querySelector<HTMLInputElement>('[aria-label="Select all open conflicts"]')?.click();
    await page.updateComplete;
    expect(page.textContent).toContain('2 of 2 selected');
    const batch = () => page.querySelector('[aria-label="Batch conflict decisions"]');
    [...(batch()?.querySelectorAll('button') ?? [])]
      .find((button) => button.textContent?.trim() === 'Compare selected versions')
      ?.click();
    await vi.waitFor(() => expect(page.querySelectorAll('ism-recovery-versions')).toHaveLength(2));
    expect(page.busy).toBeUndefined();
    changed = true;
    await page.updateComplete;
    [...(batch()?.querySelectorAll('button') ?? [])]
      .find((button) => button.textContent?.trim() === 'Keep Beta')
      ?.click();
    await vi.waitFor(() =>
      expect(post).toHaveBeenCalledWith('/api/admin/recovery/decisions', {
        body: {
          decisions: [
            {
              target: { kind: 'conflict', id: conflict.id, peer_id: null },
              revision: versions.revision,
            },
          ],
          resolution: 'keep_remote',
        },
      }),
    );
    await vi.waitFor(() => expect(page.textContent).toContain('1 of 2 selected'));
  });

  it('shows a failed comparison on its card without blocking the other cards', async () => {
    const second = {
      ...conflict,
      id: 'conflict-2',
      location: { root_id: 'photos', path: 'b.jpg' },
    };
    vi.spyOn(api, 'get').mockImplementation(async (path) => {
      if (!path.endsWith('/versions')) return [conflict, second] as never;
      throw new Error('The peer could not provide its version');
    });
    const page = await mount('ism-conflicts-page');
    await vi.waitFor(() => expect(page.querySelectorAll('article')).toHaveLength(2));
    const [first, other] = page.querySelectorAll('article');
    first.querySelector('button')?.click();
    await vi.waitFor(() => expect(first.querySelector('[role="alert"]')).not.toBeNull());
    expect(first.textContent).toContain('The peer could not provide its version');
    expect(first.textContent).toContain('Try again');
    expect(other.querySelector('button')?.disabled).toBe(false);
  });
});
