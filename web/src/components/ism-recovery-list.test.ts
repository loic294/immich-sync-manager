import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import type { RecoveryVersions } from '@/api/generated/RecoveryVersions';
import type { RecoveryView } from '@/api/generated/RecoveryView';
import { mount } from '@/test/helpers';
import '@/components/ism-recovery-list';

vi.mock('@/components/confirm', () => ({
  confirmAction: vi.fn(async () => true),
}));

const ISSUE: RecoveryView = {
  target: { kind: 'transfer', id: 'transfer-1', peer_id: null },
  title: 'Trip/photo.jpg',
  state: 'waiting',
  code: 'peer_unreachable',
  detail: 'Receiver is unreachable',
  can_fix: true,
  can_dismiss: false,
  replacement_operation_id: null,
};

const VERSIONS: RecoveryVersions = {
  share: 'Photos',
  revision: 'a'.repeat(64),
  receiving: [{ instance: 'Alpha', path: 'photo.xmp', state: 'file', files: [], directories: [] }],
  source: { instance: 'Beta', path: 'photo.xmp', state: 'file', files: [], directories: [] },
};

describe('sync recovery', () => {
  afterEach(() => vi.restoreAllMocks());

  it('keeps selection across pages and submits exact targets without claiming completion', async () => {
    vi.spyOn(api, 'get').mockImplementation(async (_path, options) => {
      const offset = Number(options?.query?.offset ?? 0);
      return {
        rows: [{ ...ISSUE, target: { ...ISSUE.target, id: `transfer-${offset + 1}` } }],
        total: 501,
      } as never;
    });
    const post = vi
      .spyOn(api, 'post')
      .mockResolvedValue({ items: [{ ...ISSUE, state: 'queued' }] } as never);
    const page = await mount('ism-recovery-list');
    await page.reload();
    await page.updateComplete;
    page.querySelector<HTMLInputElement>('tbody input')?.click();
    page
      .querySelector('ism-pagination')
      ?.dispatchEvent(new CustomEvent('page-change', { detail: { limit: 250, offset: 250 } }));
    await page.reload();
    await page.updateComplete;
    page.querySelector<HTMLInputElement>('tbody input')?.click();
    await page.updateComplete;
    expect(page.textContent).toContain('2 selected');
    [...page.querySelectorAll<HTMLButtonElement>('button')]
      .find((button) => button.textContent?.includes('Fix selected'))
      ?.click();
    await vi.waitFor(() =>
      expect(post).toHaveBeenCalledWith('/api/admin/recovery', {
        body: {
          targets: [
            { kind: 'transfer', id: 'transfer-1', peer_id: null },
            { kind: 'transfer', id: 'transfer-251', peer_id: null },
          ],
          all: false,
          resolution: null,
        },
      }),
    );
    await vi.waitFor(() => expect(page.textContent).toContain('0 fixed'));
    expect(page.querySelector('details')?.hasAttribute('open')).toBe(false);
    expect(page.textContent).not.toContain('Mark handled');
  });

  it('requests the complete outstanding set rather than the visible page', async () => {
    vi.spyOn(api, 'get').mockResolvedValue({ rows: [ISSUE], total: 700 } as never);
    const post = vi.spyOn(api, 'post').mockResolvedValue({ items: [ISSUE] } as never);
    const page = await mount('ism-recovery-list');
    await page.reload();
    await page.updateComplete;
    [...page.querySelectorAll<HTMLButtonElement>('button')]
      .find((button) => button.textContent?.includes('Fix all 700'))
      ?.click();
    await vi.waitFor(() =>
      expect(post).toHaveBeenCalledWith('/api/admin/recovery', {
        body: { targets: [], all: true, resolution: null },
      }),
    );
    expect(page.textContent).toContain('Independent sync continues');
  });

  it('exposes selected version comparisons from other pages before a batch decision', async () => {
    vi.spyOn(api, 'get').mockImplementation(async (path, options) => {
      if (path.endsWith('/versions')) return VERSIONS as never;
      const offset = Number(options?.query?.offset ?? 0);
      return {
        rows: [
          {
            ...ISSUE,
            target: { kind: 'conflict', id: `choice-${offset}`, peer_id: null },
            title: `Selected photo ${offset}`,
            state: 'needs_decision',
            can_fix: false,
          },
        ],
        total: 501,
      } as never;
    });
    const page = await mount('ism-recovery-list');
    await page.reload();
    await page.updateComplete;
    page.querySelector<HTMLInputElement>('tbody input')?.click();
    page
      .querySelector('ism-pagination')
      ?.dispatchEvent(new CustomEvent('page-change', { detail: { limit: 250, offset: 250 } }));
    await page.reload();
    await page.updateComplete;
    page.querySelector<HTMLInputElement>('tbody input')?.click();
    await page.updateComplete;
    [...page.querySelectorAll<HTMLButtonElement>('button')]
      .find((button) => button.textContent?.trim() === 'Compare selected versions')
      ?.click();
    await vi.waitFor(() => expect(page.querySelectorAll('ism-recovery-versions')).toHaveLength(2));
    await page.updateComplete;
    const review = page.querySelector('[aria-label="Selected conflicts on other pages"]');
    expect(review?.textContent).toContain('Selected photo 0');
    expect(review?.querySelector('ism-recovery-versions')).not.toBeNull();
    expect(page.querySelectorAll('ism-recovery-versions')).toHaveLength(2);
  });

  it('queues explicit in-app batch decisions and exposes no retry for non-retryable work', async () => {
    const conflict: RecoveryView = {
      ...ISSUE,
      target: { kind: 'conflict', id: 'conflict-1', peer_id: null },
      state: 'needs_decision',
      can_fix: false,
    };
    vi.spyOn(api, 'get').mockImplementation(async (path) =>
      path.endsWith('/versions')
        ? (VERSIONS as never)
        : ({
            rows: [conflict, { ...ISSUE, state: 'not_retryable', can_fix: false }],
            total: 2,
          } as never),
    );
    const post = vi.spyOn(api, 'post').mockResolvedValue({ items: [conflict] } as never);
    const page = await mount('ism-recovery-list');
    await page.reload();
    await page.updateComplete;
    expect(page.textContent).toContain('Not retryable');
    expect(page.querySelectorAll('tbody tr')[1]?.querySelectorAll('button')).toHaveLength(0);
    page.querySelector<HTMLInputElement>('tbody input')?.click();
    await page.updateComplete;
    [...page.querySelectorAll<HTMLButtonElement>('button')]
      .find((button) => button.textContent?.trim() === 'Compare selected versions')
      ?.click();
    await vi.waitFor(() => expect(page.querySelector('ism-recovery-versions')).not.toBeNull());
    await vi.waitFor(() => expect(page.busy).toBeUndefined());
    [...page.querySelectorAll<HTMLButtonElement>('button')]
      .find((button) => button.textContent?.trim() === 'Keep both')
      ?.click();
    await vi.waitFor(() =>
      expect(post).toHaveBeenCalledWith('/api/admin/recovery/decisions', {
        body: {
          decisions: [{ target: conflict.target, revision: VERSIONS.revision }],
          resolution: 'keep_both',
        },
      }),
    );
  });

  it('keeps successful comparisons and retries only the one that failed', async () => {
    const rows = ['ok', 'locked'].map(
      (id): RecoveryView => ({
        ...ISSUE,
        target: { kind: 'conflict', id, peer_id: null },
        title: id,
        state: 'needs_decision',
        can_fix: false,
      }),
    );
    let failing = true;
    vi.spyOn(api, 'get').mockImplementation(async (path, options) => {
      if (!path.endsWith('/versions')) return { rows, total: 2 } as never;
      if (options?.query?.id === 'locked' && failing) throw new Error('database is locked');
      return VERSIONS as never;
    });
    const page = await mount('ism-recovery-list');
    await page.reload();
    await page.updateComplete;
    page.querySelector<HTMLInputElement>('thead input')?.click();
    await page.updateComplete;
    [...page.querySelectorAll<HTMLButtonElement>('button')]
      .find((button) => button.textContent?.trim() === 'Compare selected versions')
      ?.click();
    await vi.waitFor(() => expect(page.querySelector('[role="alert"]')).not.toBeNull());
    expect(page.querySelectorAll('ism-recovery-versions')).toHaveLength(1);
    expect(page.querySelector('[role="alert"]')?.textContent).toContain('database is locked');
    expect(page.busy).toBeUndefined();
    failing = false;
    [...page.querySelectorAll<HTMLButtonElement>('button')]
      .find((button) => button.textContent?.trim() === 'Try again')
      ?.click();
    await vi.waitFor(() => expect(page.querySelectorAll('ism-recovery-versions')).toHaveLength(2));
    expect(page.querySelector('[role="alert"]')).toBeNull();
  });

  it('submits the reviewed revision without refetching files before queueing', async () => {
    const conflict: RecoveryView = {
      ...ISSUE,
      target: { kind: 'conflict', id: 'choice', peer_id: null },
      state: 'needs_decision',
      can_fix: false,
    };
    let revision = VERSIONS.revision;
    const get = vi
      .spyOn(api, 'get')
      .mockImplementation(async (path) =>
        path.endsWith('/versions')
          ? ({ ...VERSIONS, revision } as never)
          : ({ rows: [conflict], total: 1 } as never),
      );
    const post = vi.spyOn(api, 'post').mockResolvedValue({
      items: [{ ...conflict, state: 'queued' }],
    } as never);
    const page = await mount('ism-recovery-list');
    await page.reload();
    await page.updateComplete;
    [...page.querySelectorAll<HTMLButtonElement>('button')]
      .find((button) => button.textContent?.trim() === 'Compare versions')
      ?.click();
    await vi.waitFor(() => expect(page.querySelector('ism-recovery-versions')).not.toBeNull());
    await vi.waitFor(() => expect(page.busy).toBeUndefined());
    revision = 'b'.repeat(64);
    [...page.querySelectorAll<HTMLButtonElement>('button')]
      .find((button) => button.textContent?.trim() === 'Keep both')
      ?.click();
    await vi.waitFor(() =>
      expect(post).toHaveBeenCalledWith('/api/admin/recovery/decisions', {
        body: {
          decisions: [{ target: conflict.target, revision: VERSIONS.revision }],
          resolution: 'keep_both',
        },
      }),
    );
    expect(get.mock.calls.filter(([path]) => path.endsWith('/versions'))).toHaveLength(1);
  });

  it('keeps selection but removes batch version decisions when receiver evidence becomes unavailable', async () => {
    const conflict: RecoveryView = {
      ...ISSUE,
      target: { kind: 'conflict', id: 'conflict-1', peer_id: 'source-peer' },
      state: 'needs_decision',
      can_fix: false,
      local_instance: 'Alpha',
      peer_instance: 'Beta',
    };
    const get = vi.spyOn(api, 'get').mockResolvedValue({ rows: [conflict], total: 1 } as never);
    const post = vi.spyOn(api, 'post');
    const page = await mount('ism-recovery-list');
    await page.reload();
    await page.updateComplete;
    page.querySelector<HTMLInputElement>('tbody input')?.click();
    await page.updateComplete;
    expect(page.querySelector('[aria-label="Batch conflict decisions"]')?.textContent).toContain(
      'Keep Alpha',
    );
    get.mockResolvedValue({
      rows: [{ ...conflict, state: 'waiting', code: 'peer_recovery_unavailable', can_fix: true }],
      total: 1,
    } as never);
    await page.reload();
    await page.updateComplete;
    expect(page.textContent).toContain('1 selected');
    expect(page.querySelector('[aria-label="Batch conflict decisions"]')).toBeNull();
    expect(page.textContent).not.toContain('Keep both');
    expect(post).not.toHaveBeenCalled();
  });
});
