import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import type { RecoveryView } from '@/api/generated/RecoveryView';
import { mount } from '@/test/helpers';
import { mockLiveSnapshots } from '@/test/live';
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
  scopes: [],
};

describe('sync recovery', () => {
  afterEach(() => vi.restoreAllMocks());

  it('starts with a manageable page when many files need attention', async () => {
    const get = vi.spyOn(api, 'get').mockResolvedValue({ rows: [ISSUE], total: 700 } as never);
    const page = await mount('ism-recovery-list');
    await page.reload();
    await page.updateComplete;
    expect(get).toHaveBeenCalledWith('/api/admin/recovery/page', {
      query: {
        limit: 25,
        offset: 0,
        share: '',
        folder: '',
        issue_type: '',
        status: '',
        kind: '',
      },
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

  it('filters the whole queue and limits Fix all to the matching issues', async () => {
    const filters = [
      { share_name: 'Photos', folders: ['Trips', 'Trips/Coast'] },
      { share_name: 'Scans', folders: ['Archive'] },
    ];
    const get = vi.spyOn(api, 'get').mockImplementation(async (_path, options) => {
      return {
        rows: [ISSUE],
        total: options?.query?.share ? 34 : 701,
        filters,
        issue_types: [null, 'version_conflict', 'choose_version', 'constructor'],
      } as never;
    });

    const post = vi.spyOn(api, 'post').mockResolvedValue({ items: [ISSUE] } as never);
    const page = await mount('ism-recovery-list');
    await page.reload();
    await page.updateComplete;
    page.querySelector<HTMLInputElement>('tbody input')?.click();
    page
      .querySelector('ism-pagination')
      ?.dispatchEvent(new CustomEvent('page-change', { detail: { limit: 25, offset: 25 } }));
    await page.reload();
    await page.updateComplete;
    expect(page.selected.size).toBe(1);
    expect(page.offset).toBe(25);
    [...page.querySelectorAll<HTMLButtonElement>('button')]
      .find((button) => button.textContent?.trim() === 'Filters')
      ?.click();
    await page.updateComplete;
    expect(
      page.querySelector<HTMLSelectElement>('[aria-label="Filter by subfolder"]')?.disabled,
    ).toBe(true);
    expect(
      page.querySelector<HTMLSelectElement>('[aria-label="Filter by issue type"]')?.textContent,
    ).toContain('Constructor');

    const setFilter = async (label: string, value: string) => {
      const select = page.querySelector<HTMLSelectElement>(`[aria-label="${label}"]`);
      if (!select) throw new Error(`${label} is missing`);
      select.value = value;
      select.dispatchEvent(new Event('change'));
      await page.reload();
      await page.updateComplete;
    };
    await setFilter('Filter by share', 'Photos');
    expect(page.offset).toBe(0);
    expect(page.selected.size).toBe(0);
    const folder = page.querySelector<HTMLSelectElement>('[aria-label="Filter by subfolder"]');
    if (!folder) throw new Error('Folder filter is missing');
    expect([...folder.options].map((option) => option.textContent)).toEqual([
      'All folders',
      'Trips',
      'Trips/Coast',
    ]);
    await setFilter('Filter by subfolder', 'Trips/Coast');
    await setFilter('Filter by issue type', 'code:version_conflict');
    await setFilter('Filter by recovery status', 'needs_decision');
    await setFilter('Filter by item kind', 'conflict');
    const query = {
      share: 'Photos',
      folder: 'Trips/Coast',
      issue_type: 'code:version_conflict',
      status: 'needs_decision',
      kind: 'conflict',
    };
    expect(get).toHaveBeenCalledWith('/api/admin/recovery/page', {
      query: { limit: 25, offset: 0, ...query },
    });
    expect(page.textContent).toContain('Fix all 34 matching issues');
    expect(page.querySelector('[aria-controls="recovery-filters"]')?.textContent).toContain('5');
    [...page.querySelectorAll<HTMLButtonElement>('button')]
      .find((button) => button.textContent?.includes('Fix all 34 matching'))
      ?.click();
    await vi.waitFor(() =>
      expect(post).toHaveBeenCalledWith('/api/admin/recovery', {
        body: { targets: [], all: true, resolution: null },
        query,
      }),
    );
    await vi.waitFor(() => expect(page.busy).toBeUndefined());
    await setFilter('Filter by share', 'Scans');
    expect(page.folder).toBe('');
    [...page.querySelectorAll<HTMLButtonElement>('button')]
      .find((button) => button.textContent?.trim() === 'Clear filters')
      ?.click();
    await page.reload();
    await page.updateComplete;
    expect(page.filtering).toBe(false);
    expect(page.textContent).toContain('Fix all 701 issues');
    expect(page.share).toBe('');
    expect(page.issueType).toBe('');
    expect(page.status).toBe('');
    expect(page.kind).toBe('');
  });

  it('keeps the selected filters on live snapshots without polling', async () => {
    const live = mockLiveSnapshots();
    const filters = [{ share_name: 'Photos', folders: ['Trips'] }];
    const get = vi.spyOn(api, 'get').mockResolvedValue({
      rows: [ISSUE],
      total: 2,
      filters,
      issue_types: ['peer_unreachable'],
    } as never);
    const page = await mount('ism-recovery-list');
    await vi.waitFor(() => expect(page.textContent).toContain(ISSUE.title));
    [...page.querySelectorAll<HTMLButtonElement>('button')]
      .find((button) => button.textContent?.trim() === 'Filters')
      ?.click();
    await page.updateComplete;
    const share = page.querySelector<HTMLSelectElement>('[aria-label="Filter by share"]');
    if (!share) throw new Error('Share filter is missing');
    share.value = 'Photos';
    share.dispatchEvent(new Event('change'));
    await vi.waitFor(() => expect(page.filtering).toBe(false));
    const url = [...live.handlers.keys()].find((key) => key.includes('share=Photos'));
    if (!url) throw new Error('Filtered live subscription is missing');
    expect(live.handlers.size).toBe(1);
    const calls = get.mock.calls.length;
    live.push(url, {
      rows: [{ ...ISSUE, title: 'Updated photo.jpg' }],
      total: 1,
      filters,
      issue_types: ['peer_unreachable'],
    });
    await vi.waitFor(() => expect(page.textContent).toContain('Updated photo.jpg'));
    expect(page.share).toBe('Photos');
    expect(page.total).toBe(1);
    expect(page.textContent).toContain('Fix all 1 matching issue');
    expect(get).toHaveBeenCalledTimes(calls);
  });

  it('discards an old page while filters are changing and keeps Fix all disabled', async () => {
    let release: ((value: unknown) => void) | undefined;
    vi.spyOn(api, 'get').mockImplementation(async (_path, options) => {
      if (options?.query?.offset === 25) {
        return (await new Promise<unknown>((resolve) => {
          release = resolve;
        })) as never;
      }
      return {
        rows: options?.query?.share ? [] : [ISSUE],
        total: options?.query?.share ? 0 : 701,
        filters: [{ share_name: 'Photos', folders: ['Trips'] }],
        issue_types: ['peer_unreachable'],
      } as never;
    });
    const page = await mount('ism-recovery-list');
    await page.reload();
    [...page.querySelectorAll<HTMLButtonElement>('button')]
      .find((button) => button.textContent?.trim() === 'Filters')
      ?.click();
    await page.updateComplete;
    page
      .querySelector('ism-pagination')
      ?.dispatchEvent(new CustomEvent('page-change', { detail: { limit: 25, offset: 25 } }));
    await vi.waitFor(() => expect(release).toBeDefined());
    const share = page.querySelector<HTMLSelectElement>('[aria-label="Filter by share"]');
    if (!share) throw new Error('Share filter is missing');
    share.value = 'Photos';
    share.dispatchEvent(new Event('change'));
    await page.updateComplete;
    const fixAll = [...page.querySelectorAll<HTMLButtonElement>('button')].find((button) =>
      button.textContent?.includes('Fix all'),
    );
    expect(fixAll?.disabled).toBe(true);
    release?.({ rows: [ISSUE], total: 701, filters: [], issue_types: [] });
    await vi.waitFor(() => expect(page.filtering).toBe(false));
    await page.updateComplete;
    expect(page.rows).toEqual([]);
    expect(page.total).toBe(0);
    expect(page.textContent).toContain('No sync issues match these filters');
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
