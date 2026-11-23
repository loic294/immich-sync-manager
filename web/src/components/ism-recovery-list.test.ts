import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import type { RecoveryView } from '@/api/generated/RecoveryView';
import { mount } from '@/test/helpers';
import '@/components/ism-recovery-list';

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

describe('sync recovery', () => {
  afterEach(() => vi.restoreAllMocks());

  it('starts with a manageable page when many files need attention', async () => {
    const get = vi.spyOn(api, 'get').mockResolvedValue({ rows: [ISSUE], total: 700 } as never);
    const page = await mount('ism-recovery-list');
    await page.reload();
    await page.updateComplete;
    expect(get).toHaveBeenCalledWith('/api/admin/recovery/page', {
      query: { limit: 25, offset: 0 },
    });
    expect(page.querySelector('ism-pagination')?.limit).toBe(25);
  });

  it('shows actionable guidance and directs version decisions to Conflicts', async () => {
    const rows: RecoveryView[] = [
      ISSUE,
      {
        ...ISSUE,
        title: 'Unreadable issue',
        code: 'invalid_saved_recovery',
        detail: 'saved recovery request has an invalid version decision',
        can_fix: false,
      },
      {
        ...ISSUE,
        title: 'Different versions',
        target: { kind: 'conflict', id: 'conflict-1', peer_id: null },
        state: 'needs_decision',
        code: 'version_conflict',
        can_fix: false,
      },
      {
        ...ISSUE,
        title: 'Related operation',
        target: { kind: 'operation', id: 'operation-1', peer_id: null },
        state: 'needs_decision',
        can_fix: false,
      },
      {
        ...ISSUE,
        title: 'Version choice',
        target: { kind: 'conflict', id: 'conflict-2', peer_id: null },
        state: 'needs_decision',
        code: 'choose_version',
        can_fix: false,
      },
      {
        ...ISSUE,
        title: 'Missing file',
        state: 'not_retryable',
        code: 'recovery_exhausted',
        can_fix: false,
      },
      {
        ...ISSUE,
        title: 'Stopped fix',
        code: 'recovery_worker_failed',
        detail: 'Recovery stopped unexpectedly.',
      },
    ];
    const get = vi.spyOn(api, 'get').mockResolvedValue({ rows, total: rows.length } as never);
    const post = vi.spyOn(api, 'post');
    const page = await mount('ism-recovery-list');
    await page.reload();
    await page.updateComplete;
    const rendered = [...page.querySelectorAll('tbody tr')];
    expect(rendered[0]?.querySelector('td:nth-child(2) > p')?.textContent).toContain(
      'Check that it is running and connected',
    );
    expect(rendered[1]?.querySelector('td:nth-child(2) > p')?.textContent).toContain(
      "can't read this saved issue",
    );
    expect(rendered[2]?.textContent).toContain('Review them again on the Conflicts page');
    expect(rendered[3]?.textContent).toContain('Find it on the Conflicts page');
    expect(rendered[4]?.textContent).toContain(
      'Choose which version to keep on the Conflicts page',
    );
    expect(rendered[5]?.textContent).toContain('Restore this file from a backup');
    expect(rendered[6]?.textContent).toContain('choose Fix selected to try again');
    expect(rendered[1]?.querySelector('details')?.open).toBe(false);
    expect(rendered[5]?.querySelector('button')).toBeNull();
    for (const row of [rendered[2], rendered[3], rendered[4]]) {
      expect(row?.querySelector('a')?.getAttribute('href')).toBe('/conflicts');
      expect(row?.querySelector('a')?.textContent).toContain('Go to Conflicts');
    }
    rendered[2]?.querySelector<HTMLInputElement>('input')?.click();
    await page.updateComplete;
    expect(page.textContent).toContain('1 selected');
    expect(page.querySelector('[aria-label="Batch conflict decisions"]')).toBeNull();
    expect(page.querySelector('ism-recovery-versions')).toBeNull();
    expect(page.textContent).not.toContain('Compare selected versions');
    expect(page.textContent).not.toContain('Keep both');
    expect(get.mock.calls.some(([path]) => path.endsWith('/versions'))).toBe(false);
    expect(post).not.toHaveBeenCalled();
  });

  it('keeps selection across pages, permits removal, and submits exact targets', async () => {
    vi.spyOn(api, 'get').mockImplementation(async (_path, options) => {
      const offset = Number(options?.query?.offset ?? 0);
      return {
        rows: [
          {
            ...ISSUE,
            title: `Photo ${offset}`,
            target: { ...ISSUE.target, id: `transfer-${offset + 1}` },
          },
        ],
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
    const offPage = page.querySelector('[aria-label="Selected issues on other pages"]');
    expect(offPage?.textContent).toContain('Photo 0');
    offPage?.querySelector<HTMLButtonElement>('button')?.click();
    await page.updateComplete;
    expect(page.textContent).toContain('1 selected');
    [...page.querySelectorAll<HTMLButtonElement>('button')]
      .find((button) => button.textContent?.includes('Fix selected'))
      ?.click();
    await vi.waitFor(() =>
      expect(post).toHaveBeenCalledWith('/api/admin/recovery', {
        body: {
          targets: [{ kind: 'transfer', id: 'transfer-251', peer_id: null }],
          all: false,
          resolution: null,
        },
      }),
    );
    await vi.waitFor(() => expect(page.textContent).toContain('0 fixed'));
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
    expect(page.textContent).toContain('Issues are handled per file');
  });

  it('keeps selected issues updated when recovery eligibility changes', async () => {
    const conflict: RecoveryView = {
      ...ISSUE,
      target: { kind: 'conflict', id: 'conflict-1', peer_id: 'source-peer' },
      state: 'needs_decision',
      can_fix: false,
    };
    const get = vi.spyOn(api, 'get').mockResolvedValue({ rows: [conflict], total: 1 } as never);
    const page = await mount('ism-recovery-list');
    await page.reload();
    await page.updateComplete;
    page.querySelector<HTMLInputElement>('tbody input')?.click();
    await page.updateComplete;
    const fix = [...page.querySelectorAll<HTMLButtonElement>('button')].find((button) =>
      button.textContent?.includes('Fix selected'),
    );
    expect(fix?.disabled).toBe(true);
    get.mockResolvedValue({
      rows: [{ ...conflict, state: 'waiting', code: 'peer_recovery_unavailable', can_fix: true }],
      total: 1,
    } as never);
    await page.reload();
    await page.updateComplete;
    expect(page.textContent).toContain('1 selected');
    expect(fix?.disabled).toBe(false);
    expect(page.querySelector('a[href="/conflicts"]')).toBeNull();
  });
});
