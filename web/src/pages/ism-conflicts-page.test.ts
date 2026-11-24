import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import { events } from '@/api/events';
import type { ConflictView } from '@/api/generated/ConflictView';
import { confirmAction } from '@/components/confirm';
import { mount } from '@/test/helpers';
import '@/pages/ism-conflicts-page';

vi.mock('@/components/confirm', () => ({
  confirmAction: vi.fn(async () => true),
}));

const conflict: ConflictView = {
  id: 'conflict-1',
  share_name: 'Photos',
  location: { root_id: 'photos', path: 'a.jpg' },
  kind: 'changed_on_both',
  detail: 'The file changed on both instances.',
  local: {
    instance: 'Alpha',
    sha256: null,
    size: 10,
    modified_at: null,
    deleted: false,
  },
  remote: {
    instance: 'Beta',
    sha256: null,
    size: 20,
    modified_at: null,
    deleted: false,
  },
  peer_id: 'peer-1',
  status: 'open',
  created_at: '2024-01-01T00:00:00Z',
  moved_from: null,
};

const versions = {
  share: 'Photos',
  revision: 'a'.repeat(64),
  receiving: [
    {
      instance: 'Alpha',
      path: 'a.jpg',
      state: 'file',
      files: [
        {
          path: 'a.jpg',
          role: 'file',
          sha256: 'b'.repeat(64),
          size: 10,
          modified_at: '2024-01-01T00:00:00Z',
        },
      ],
      directories: ['.'],
    },
  ],
  source: {
    instance: 'Beta',
    path: 'a.jpg',
    state: 'file',
    files: [
      {
        path: 'a.jpg',
        role: 'file',
        sha256: 'c'.repeat(64),
        size: 20,
        modified_at: '2024-01-02T00:00:00Z',
      },
    ],
    directories: ['.'],
  },
};

async function enableSelection(page: HTMLElementTagNameMap['ism-conflicts-page']) {
  const toggle = [...page.querySelectorAll<HTMLButtonElement>('button')].find(
    (button) => button.textContent?.trim() === 'Select multiple',
  );
  expect(toggle).toBeDefined();
  toggle?.click();
  await page.updateComplete;
}

describe('ism-conflicts-page', () => {
  afterEach(() => vi.restoreAllMocks());

  it('loads cached previews automatically and keeps the queue paginated', async () => {
    const get = vi
      .spyOn(api, 'get')
      .mockImplementation(async (path) =>
        path.endsWith('/versions')
          ? (versions as never)
          : ({ rows: [{ owner: 'local', conflict }], total: 501 } as never),
      );
    const page = await mount('ism-conflicts-page');

    await vi.waitFor(() => expect(page.querySelectorAll('ism-recovery-versions')).toHaveLength(1));
    expect(page.textContent).toContain('Review queue');
    expect(page.textContent).toContain('1–50 of 501');
    expect(page.querySelector('[data-preview]')?.getAttribute('data-preview')).toContain(
      '/recovery/preview',
    );
    const review = page.querySelector('ism-recovery-versions');
    expect(review?.querySelectorAll('button')[0]?.textContent?.trim()).toBe('Keep Alpha');
    expect(review?.querySelectorAll('button')[1]?.textContent?.trim()).toBe('Keep Beta');
    expect(review?.textContent).not.toContain('Keep both');
    expect(page.textContent).not.toContain('Compare versions');
    expect(page.textContent).not.toContain('Technical details');
    expect(get.mock.calls.some(([path]) => path.endsWith('/versions'))).toBe(true);
  });

  it('queues the chosen version using the automatically reviewed revision', async () => {
    vi.spyOn(api, 'get').mockImplementation(async (path) =>
      path.endsWith('/versions')
        ? (versions as never)
        : ({ rows: [{ owner: 'local', conflict }], total: 1 } as never),
    );
    const post = vi.spyOn(api, 'post').mockResolvedValue({
      items: [
        {
          target: { kind: 'conflict', id: conflict.id, peer_id: null },
          state: 'queued',
        },
      ],
    } as never);
    const page = await mount('ism-conflicts-page');
    await vi.waitFor(() => expect(page.querySelector('ism-recovery-versions')).not.toBeNull());

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

  it('hides bulk controls by default and preserves selections when leaving and returning', async () => {
    vi.spyOn(api, 'get').mockImplementation(async (path) =>
      path.endsWith('/versions')
        ? (versions as never)
        : ({ rows: [{ owner: 'local', conflict }], total: 1 } as never),
    );
    const page = await mount('ism-conflicts-page');
    await vi.waitFor(() => expect(page.textContent).toContain('Select multiple'));
    expect(page.querySelector('[aria-label="Select photos:a.jpg"]')).toBeNull();
    expect(page.querySelector('[aria-label="Batch conflict decisions"]')).toBeNull();
    await enableSelection(page);
    page.querySelector<HTMLInputElement>('[aria-label="Select photos:a.jpg"]')?.click();
    await page.updateComplete;
    [...page.querySelectorAll<HTMLButtonElement>('button')]
      .find((button) => button.textContent?.trim() === 'Done selecting')
      ?.click();
    await page.updateComplete;
    expect(page.querySelector('[aria-label="Select photos:a.jpg"]')).toBeNull();
    expect(page.querySelector('[aria-label="Batch conflict decisions"]')).toBeNull();
    expect(page.textContent).toContain('1 selected');
    await enableSelection(page);
    expect(
      page.querySelector<HTMLInputElement>('[aria-label="Select photos:a.jpg"]')?.checked,
    ).toBe(true);
  });

  it('preserves selected conflicts when moving through pages', async () => {
    const second = {
      ...conflict,
      id: 'conflict-2',
      location: { root_id: 'photos', path: 'b.jpg' },
    };
    const get = vi.spyOn(api, 'get').mockImplementation(async (path, options) => {
      if (path.endsWith('/versions')) return versions as never;
      const offset = Number(options?.query?.offset ?? 0);
      return {
        rows: [{ owner: 'local', conflict: offset === 0 ? conflict : second }],
        total: 501,
      } as never;
    });
    const page = await mount('ism-conflicts-page');
    await vi.waitFor(() => expect(page.querySelector('ism-recovery-versions')).not.toBeNull());
    await enableSelection(page);
    page.querySelector<HTMLInputElement>('[aria-label="Select photos:a.jpg"]')?.click();
    await page.updateComplete;

    page.querySelector<HTMLButtonElement>('[aria-label="Next page"]')?.click();
    await vi.waitFor(() => expect(page.textContent).toContain('b.jpg'));
    await vi.waitFor(() => expect(page.querySelector('ism-recovery-versions')).not.toBeNull());

    expect(get.mock.calls.some(([, options]) => options?.query?.offset === 50)).toBe(true);
    expect(page.textContent).toContain('1 total selected');
  });

  it('applies a batch choice to reviewed selections from multiple pages', async () => {
    const second = {
      ...conflict,
      id: 'conflict-2',
      location: { root_id: 'photos', path: 'b.jpg' },
    };
    vi.spyOn(api, 'get').mockImplementation(async (path, options) => {
      if (path.endsWith('/versions')) return versions as never;
      const offset = Number(options?.query?.offset ?? 0);
      return {
        rows: [{ owner: 'local', conflict: offset === 0 ? conflict : second }],
        total: 501,
      } as never;
    });
    const post = vi.spyOn(api, 'post').mockResolvedValue({
      items: [conflict, second].map((item) => ({
        target: { kind: 'conflict', id: item.id, peer_id: null },
        state: 'queued',
      })),
    } as never);
    const page = await mount('ism-conflicts-page');
    await vi.waitFor(() => expect(page.querySelector('ism-recovery-versions')).not.toBeNull());
    await enableSelection(page);
    page.querySelector<HTMLInputElement>('[aria-label="Select photos:a.jpg"]')?.click();
    page.querySelector<HTMLButtonElement>('[aria-label="Next page"]')?.click();
    await vi.waitFor(() => expect(page.textContent).toContain('b.jpg'));
    await vi.waitFor(() => expect(page.querySelector('ism-recovery-versions')).not.toBeNull());
    page.querySelector<HTMLInputElement>('[aria-label="Select photos:b.jpg"]')?.click();
    await page.updateComplete;

    [...page.querySelectorAll<HTMLButtonElement>('[aria-label="Batch conflict decisions"] button')]
      .find((button) => button.textContent?.trim() === 'Keep Beta')
      ?.click();

    await vi.waitFor(() =>
      expect(post).toHaveBeenCalledWith('/api/admin/recovery/decisions', {
        body: {
          decisions: [conflict, second].map((item) => ({
            target: { kind: 'conflict', id: item.id, peer_id: null },
            revision: versions.revision,
          })),
          resolution: 'keep_remote',
        },
      }),
    );
  });

  it('refreshes evidence for selected conflicts on other pages after a change', async () => {
    let revision = versions.revision;
    vi.spyOn(api, 'get').mockImplementation(async (path, options) => {
      if (path.endsWith('/versions')) return { ...versions, revision } as never;
      const offset = Number(options?.query?.offset ?? 0);
      return {
        rows: [
          {
            owner: 'local',
            conflict:
              offset === 0
                ? conflict
                : { ...conflict, id: 'conflict-2', location: { root_id: 'photos', path: 'b.jpg' } },
          },
        ],
        total: 51,
      } as never;
    });
    const post = vi.spyOn(api, 'post').mockResolvedValue({
      items: [{ target: { kind: 'conflict', id: conflict.id, peer_id: null }, state: 'queued' }],
    } as never);
    const page = await mount('ism-conflicts-page');
    await vi.waitFor(() => expect(page.querySelector('ism-recovery-versions')).not.toBeNull());
    await enableSelection(page);
    page.querySelector<HTMLInputElement>('[aria-label="Select photos:a.jpg"]')?.click();
    page.querySelector<HTMLButtonElement>('[aria-label="Next page"]')?.click();
    await vi.waitFor(() => expect(page.textContent).toContain('b.jpg'));
    revision = 'd'.repeat(64);
    events.emit({ type: 'conflicts_changed' });
    await page.updateComplete;
    await vi.waitFor(() => expect(page.textContent).toContain('1 of 1 ready'));
    [...page.querySelectorAll<HTMLButtonElement>('[aria-label="Batch conflict decisions"] button')]
      .find((button) => button.textContent?.trim() === 'Keep both')
      ?.click();
    await vi.waitFor(() =>
      expect(post).toHaveBeenCalledWith('/api/admin/recovery/decisions', {
        body: {
          decisions: [{ target: { kind: 'conflict', id: conflict.id, peer_id: null }, revision }],
          resolution: 'keep_both',
        },
      }),
    );
  });

  it('keeps unchanged previews mounted while refreshed evidence is loading', async () => {
    let resolve!: (value: typeof versions) => void;
    let refreshing = false;
    vi.spyOn(api, 'get').mockImplementation(async (path) => {
      if (!path.endsWith('/versions'))
        return { rows: [{ owner: 'local', conflict }], total: 1 } as never;
      if (refreshing)
        return (await new Promise<typeof versions>((done) => (resolve = done))) as never;
      return versions as never;
    });
    const page = await mount('ism-conflicts-page');
    await vi.waitFor(() => expect(page.querySelector('ism-recovery-versions')).not.toBeNull());
    const preview = page.querySelector('ism-recovery-versions');
    refreshing = true;
    events.emit({ type: 'conflicts_changed' });
    await page.updateComplete;
    expect(page.querySelector('ism-recovery-versions')).toBe(preview);
    expect(
      [...page.querySelectorAll<HTMLButtonElement>('button')].find(
        (button) => button.textContent?.trim() === 'Keep both',
      )?.disabled,
    ).toBe(true);
    resolve(versions);
    await vi.waitFor(() =>
      expect(
        [...page.querySelectorAll<HTMLButtonElement>('button')].find(
          (button) => button.textContent?.trim() === 'Keep both',
        )?.disabled,
      ).toBe(false),
    );
    expect(page.querySelector('ism-recovery-versions')).toBe(preview);
  });

  it('bounds off-page refreshes and preserves the remaining selections', async () => {
    vi.spyOn(api, 'post').mockResolvedValue({ items: [] } as never);
    const get = vi.spyOn(api, 'get').mockImplementation(async (path, options) => {
      if (path.endsWith('/versions')) return versions as never;
      const offset = Number(options?.query?.offset ?? 0);
      return {
        rows: Array.from({ length: Math.min(50, 101 - offset) }, (_, index) => ({
          owner: 'local',
          conflict: {
            ...conflict,
            id: `conflict-${offset + index}`,
            location: { root_id: 'photos', path: `${offset + index}.jpg` },
          },
        })),
        total: 101,
      } as never;
    });
    const page = await mount('ism-conflicts-page');
    await vi.waitFor(() => expect(page.textContent).toContain('Select multiple'));
    await enableSelection(page);
    await vi.waitFor(() =>
      expect(page.querySelectorAll('[aria-label^="Select photos:"]')).toHaveLength(50),
    );
    page.querySelector<HTMLInputElement>('[aria-label="Select all open conflicts"]')?.click();
    await vi.waitFor(() => expect(page.textContent).toContain('50 of 50 ready'));
    page.querySelector<HTMLButtonElement>('[aria-label="Next page"]')?.click();
    await vi.waitFor(() => expect(page.textContent).toContain('photos:50.jpg'));
    page.querySelector<HTMLInputElement>('[aria-label="Select all open conflicts"]')?.click();
    await vi.waitFor(() => expect(page.textContent).toContain('100 of 100 ready'));
    page.querySelector<HTMLButtonElement>('[aria-label="Next page"]')?.click();
    await vi.waitFor(() => expect(page.textContent).toContain('photos:100.jpg'));
    await vi.waitFor(() =>
      expect(get.mock.calls.filter(([path]) => path.endsWith('/versions'))).toHaveLength(101),
    );
    events.emit({ type: 'conflicts_changed' });
    await vi.waitFor(() =>
      expect(get.mock.calls.filter(([path]) => path.endsWith('/versions'))).toHaveLength(152),
    );
    await vi.waitFor(() => expect(page.textContent).toContain('50 of 100 ready'));
    expect(page.textContent).toContain('Revisit other pages to refresh remaining selections');
    expect(page.textContent).toContain('100 total selected');
    [...page.querySelectorAll<HTMLButtonElement>('[aria-label="Batch conflict decisions"] button')]
      .find((button) => button.textContent?.trim() === 'Keep both')
      ?.click();
    await vi.waitFor(() =>
      expect(vi.mocked(confirmAction)).toHaveBeenLastCalledWith(
        expect.objectContaining({
          message: expect.stringContaining(
            '50 selected conflicts are not ready and will remain selected. Revisit their pages',
          ),
        }),
      ),
    );
  });

  it('reviews peer-owned conflicts and keeps distinct targets across pages', async () => {
    const peerTarget = { kind: 'conflict', id: conflict.id, peer_id: 'receiver-1' };
    const peer = {
      target: peerTarget,
      title: 'peer-photo.jpg',
      state: 'needs_decision',
      code: 'choose_version',
      detail: null,
      can_fix: false,
      can_dismiss: false,
      replacement_operation_id: null,
      local_instance: 'Beta',
      peer_instance: 'Alpha',
    };
    const get = vi.spyOn(api, 'get').mockImplementation(async (path, options) => {
      if (path.endsWith('/versions')) return versions as never;
      const offset = Number(options?.query?.offset ?? 0);
      return {
        rows: [offset === 0 ? { owner: 'local', conflict } : { owner: 'peer', conflict: peer }],
        total: 51,
      } as never;
    });
    const post = vi.spyOn(api, 'post').mockResolvedValue({
      items: [
        { target: { kind: 'conflict', id: conflict.id, peer_id: null }, state: 'queued' },
        { target: peerTarget, state: 'queued' },
      ],
    } as never);
    const page = await mount('ism-conflicts-page');
    await vi.waitFor(() => expect(page.querySelector('ism-recovery-versions')).not.toBeNull());
    await enableSelection(page);
    page.querySelector<HTMLInputElement>('[aria-label="Select photos:a.jpg"]')?.click();
    page.querySelector<HTMLButtonElement>('[aria-label="Next page"]')?.click();
    await vi.waitFor(() => expect(page.textContent).toContain('peer-photo.jpg'));
    await vi.waitFor(() => expect(page.querySelector('ism-recovery-versions')).not.toBeNull());
    page.querySelector<HTMLInputElement>('[aria-label="Select peer-photo.jpg"]')?.click();
    await page.updateComplete;
    expect(page.textContent).toContain('2 total selected');
    expect(get).toHaveBeenCalledWith('/api/admin/recovery/versions', {
      query: { kind: 'conflict', id: conflict.id, peer_id: 'receiver-1' },
    });
    [...page.querySelectorAll<HTMLButtonElement>('[aria-label="Batch conflict decisions"] button')]
      .find((button) => button.textContent?.trim() === 'Keep both')
      ?.click();
    await vi.waitFor(() =>
      expect(post).toHaveBeenCalledWith('/api/admin/recovery/decisions', {
        body: {
          decisions: [
            {
              target: { kind: 'conflict', id: conflict.id, peer_id: null },
              revision: versions.revision,
            },
            { target: peerTarget, revision: versions.revision },
          ],
          resolution: 'keep_both',
        },
      }),
    );
  });

  it('does not prepare or decide a peer conflict while its evidence is unavailable', async () => {
    const peer = {
      target: { kind: 'conflict', id: 'peer-conflict', peer_id: 'receiver-1' },
      title: 'peer-photo.jpg',
      state: 'waiting',
      code: 'peer_evidence_unavailable',
      detail: 'Peer unavailable',
      can_fix: true,
      can_dismiss: false,
      replacement_operation_id: null,
    };
    const get = vi.spyOn(api, 'get').mockResolvedValue({
      rows: [{ owner: 'peer', conflict: peer }],
      total: 1,
    } as never);
    const post = vi.spyOn(api, 'post');
    const page = await mount('ism-conflicts-page');
    await vi.waitFor(() => expect(page.textContent).toContain('Waiting for the other instance'));
    expect(get.mock.calls.some(([path]) => path.endsWith('/versions'))).toBe(false);
    expect(page.textContent).not.toContain('Choose what to keep');
    expect(post).not.toHaveBeenCalled();
  });

  it('shows a failed automatic comparison and offers a retry', async () => {
    let fail = true;
    vi.spyOn(api, 'get').mockImplementation(async (path) => {
      if (!path.endsWith('/versions'))
        return { rows: [{ owner: 'local', conflict }], total: 1 } as never;
      if (fail) throw new Error('The peer could not provide its version');
      return versions as never;
    });
    const page = await mount('ism-conflicts-page');
    await vi.waitFor(() =>
      expect(page.textContent).toContain('The peer could not provide its version'),
    );
    expect(page.textContent).toContain('Try again');

    fail = false;
    [...page.querySelectorAll<HTMLButtonElement>('button')]
      .find((button) => button.textContent?.trim() === 'Try again')
      ?.click();
    await vi.waitFor(() => expect(page.querySelector('ism-recovery-versions')).not.toBeNull());
  });

  it('recovers a failed comparison after reconnecting and missing its notification', async () => {
    let fail = true;
    vi.spyOn(api, 'get').mockImplementation(async (path) => {
      if (!path.endsWith('/versions'))
        return { rows: [{ owner: 'local', conflict }], total: 1 } as never;
      if (fail) throw new Error('Temporary peer failure');
      return versions as never;
    });
    const page = await mount('ism-conflicts-page');
    await vi.waitFor(() => expect(page.textContent).toContain('Temporary peer failure'));
    fail = false;
    events.emit({ type: 'resync' });
    await vi.waitFor(() => expect(page.querySelector('ism-recovery-versions')).not.toBeNull());
  });

  it('retries unavailable evidence when peer recovery changes', async () => {
    let ready = false;
    vi.spyOn(api, 'get').mockImplementation(async (path) => {
      if (!path.endsWith('/versions'))
        return { rows: [{ owner: 'local', conflict }], total: 1 } as never;
      if (!ready) throw new Error('Evidence is being prepared');
      return versions as never;
    });
    const page = await mount('ism-conflicts-page');
    await vi.waitFor(() => expect(page.textContent).toContain('Evidence is being prepared'));
    ready = true;
    events.emit({ type: 'sync_changed' });
    await vi.waitFor(() => expect(page.querySelector('ism-recovery-versions')).not.toBeNull());
  });
});
