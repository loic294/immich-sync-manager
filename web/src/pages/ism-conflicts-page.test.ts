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

  it('shows when each side was last modified and marks the newer one before comparing', async () => {
    vi.spyOn(api, 'get').mockResolvedValue([
      {
        ...conflict,
        local: { ...conflict.local, modified_at: '2024-01-01T00:00:00Z' },
        remote: { ...conflict.remote, modified_at: '2024-02-01T00:00:00Z' },
      },
      { ...conflict, id: 'conflict-2', remote: { ...conflict.remote, deleted: true } },
    ] as never);
    const page = await mount('ism-conflicts-page');
    await vi.waitFor(() => expect(page.querySelectorAll('article')).toHaveLength(2));
    const [changed, deleted] = [...page.querySelectorAll('article')].map((card) =>
      [...card.querySelectorAll('dl > div')].map((entry) =>
        entry.textContent?.replace(/\s+/g, ' ').trim(),
      ),
    );
    expect(changed).toEqual([
      `Alpha · last modified ${new Date('2024-01-01T00:00:00Z').toLocaleString()}`,
      `Beta · last modified ${new Date('2024-02-01T00:00:00Z').toLocaleString()} Newer`,
    ]);
    expect(deleted).toEqual(['Alpha · last modified Unknown', 'Beta · last modified Deleted']);
  });

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
    const post = vi.spyOn(api, 'post').mockResolvedValue({
      items: [{ target: { kind: 'conflict', id: conflict.id, peer_id: null }, state: 'queued' }],
    } as never);
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

  it('applies the versions reviewed in this tab without comparing them again', async () => {
    const second = {
      ...conflict,
      id: 'conflict-2',
      location: { root_id: 'photos', path: 'b.jpg' },
    };
    let changed = false;
    const get = vi.spyOn(api, 'get').mockImplementation(async (path, options) => {
      if (!path.endsWith('/versions')) return [conflict, second] as never;
      return {
        ...versions,
        revision:
          changed && options?.query?.id === 'conflict-2' ? 'b'.repeat(64) : versions.revision,
      } as never;
    });
    const post = vi.spyOn(api, 'post').mockResolvedValue({
      items: [conflict, second].map((c) => ({
        target: { kind: 'conflict', id: c.id, peer_id: null },
        state: 'queued',
      })),
    } as never);
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
    const comparisons = () => get.mock.calls.filter(([path]) => path.endsWith('/versions'));
    expect(comparisons()).toHaveLength(2);
    await page.reload();
    await page.updateComplete;
    expect(page.querySelectorAll('ism-recovery-versions')).toHaveLength(2);
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
            {
              target: { kind: 'conflict', id: second.id, peer_id: null },
              revision: versions.revision,
            },
          ],
          resolution: 'keep_remote',
        },
      }),
    );
    expect(comparisons()).toHaveLength(2);
    await vi.waitFor(() => expect(page.textContent).toContain('0 of 2 selected'));
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

  it('keeps deferred decisions selected without discarding the other reviewed versions', async () => {
    const second = { ...conflict, id: 'conflict-2' };
    const get = vi
      .spyOn(api, 'get')
      .mockImplementation(async (path) =>
        path.endsWith('/versions') ? (versions as never) : ([conflict, second] as never),
      );
    vi.spyOn(api, 'post').mockResolvedValue({
      items: [
        { target: { kind: 'conflict', id: conflict.id, peer_id: null }, state: 'queued' },
        {
          target: { kind: 'conflict', id: second.id, peer_id: null },
          state: 'waiting',
          code: 'recovery_decision_in_progress',
          detail: 'The current decision is already executing.',
        },
      ],
    } as never);
    const page = await mount('ism-conflicts-page');
    await vi.waitFor(() => expect(page.querySelectorAll('article')).toHaveLength(2));
    page.querySelector<HTMLInputElement>('[aria-label="Select all open conflicts"]')?.click();
    await page.updateComplete;
    const batch = () => page.querySelector('[aria-label="Batch conflict decisions"]');
    [...(batch()?.querySelectorAll('button') ?? [])]
      .find((button) => button.textContent?.trim() === 'Compare selected versions')
      ?.click();
    await vi.waitFor(() => expect(page.querySelectorAll('ism-recovery-versions')).toHaveLength(2));
    await page.updateComplete;
    [...(batch()?.querySelectorAll('button') ?? [])]
      .find((button) => button.textContent?.trim() === 'Keep Beta')
      ?.click();
    await vi.waitFor(() => expect(page.textContent).toContain('1 of 2 selected'));
    expect(get.mock.calls.filter(([path]) => path.endsWith('/versions'))).toHaveLength(2);
    expect(page.querySelectorAll('ism-recovery-versions')).toHaveLength(2);
  });

  it('does not choose old evidence after an explicit refresh fails', async () => {
    let fail = false;
    vi.spyOn(api, 'get').mockImplementation(async (path) => {
      if (!path.endsWith('/versions')) return [conflict] as never;
      if (fail) throw new Error('Peer unavailable');
      return versions as never;
    });
    const page = await mount('ism-conflicts-page');
    await vi.waitFor(() => expect(page.querySelector('article')).not.toBeNull());
    const compare = () => page.querySelector<HTMLButtonElement>('article button');
    compare()?.click();
    await vi.waitFor(() => expect(page.querySelector('ism-recovery-versions')).not.toBeNull());
    fail = true;
    compare()?.click();
    await vi.waitFor(() => expect(page.querySelector('[role="alert"]')).not.toBeNull());
    expect(
      [...page.querySelectorAll<HTMLButtonElement>('article button')]
        .filter((button) => button.textContent?.trim().startsWith('Keep'))
        .every((button) => button.disabled),
    ).toBe(true);
  });
});
