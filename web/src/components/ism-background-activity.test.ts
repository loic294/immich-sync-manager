import { afterEach, describe, expect, it, vi } from 'vitest';
import { admin } from '@/api/admin';
import { events } from '@/api/events';
import type { BackgroundActivity } from '@/api/generated/BackgroundActivity';
import type { BackgroundActivityPage } from '@/api/generated/BackgroundActivityPage';
import { mount } from '@/test/helpers';
import '@/components/ism-background-activity';

const task: BackgroundActivity = {
  id: 'scan-run',
  task_key: 'share:archive',
  title: 'Share inventory scan',
  resource: 'Archive',
  status: 'running',
  phase: 'Enumerating files',
  done: 41,
  total: null,
  unit: 'files discovered',
  started_at: '2026-01-01T00:00:00Z',
  updated_at: '2026-01-01T00:00:01Z',
  finished_at: null,
  next_at: null,
  error: null,
  operation_id: null,
  page: '/peers',
  count: 1,
  reasons: [],
  examples: [],
};
function snapshot(current: BackgroundActivity[] = [task]): BackgroundActivityPage {
  return { current, history: [], total: 0, observed_at: new Date().toISOString(), warnings: [] };
}

describe('background activity', () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it('shows unknown totals honestly, refreshes measured progress, and keeps tasks across history pagination', async () => {
    const get = vi.spyOn(admin, 'backgroundActivity').mockResolvedValue(snapshot());
    const page = await mount('ism-background-activity');
    await vi.waitFor(() => expect(page.textContent).toContain('41 files discovered'));
    expect(page.querySelector('progress')).toBeNull();
    expect(page.textContent).toContain('Elapsed');
    get.mockResolvedValue(
      snapshot([
        { ...task, done: 15, total: 50, unit: 'files checked', phase: 'Reconciling files' },
      ]),
    );
    events.emit({ type: 'activity_changed' });
    await vi.waitFor(() => expect(page.textContent).toContain('15 / 50 files checked'));
    expect(page.querySelector('progress')?.value).toBe(30);
    expect(page.querySelector('progress')?.getAttribute('aria-label')).toBe(
      'Checking shared files: 30%',
    );
    page
      .querySelector('ism-pagination')
      ?.dispatchEvent(new CustomEvent('page-change', { detail: { limit: 25, offset: 25 } }));
    await vi.waitFor(() => expect(get).toHaveBeenCalledWith(25, 25));
    expect(page.textContent).toContain('Checking shared files');
    expect(page.querySelector('a')?.getAttribute('href')).toBe('/peers');
  });

  it.each([
    ['a missing start time', '', null],
    ['an invalid start time', 'not-a-date', null],
    ['an invalid finish time', task.started_at, 'not-a-date'],
  ])('does not invent an elapsed duration with %s', async (_label, started_at, finished_at) => {
    vi.spyOn(admin, 'backgroundActivity').mockResolvedValue(
      snapshot([
        {
          ...task,
          status: 'blocked',
          started_at,
          finished_at,
          phase: 'Saved activity could not be read',
        },
      ]),
    );
    const page = await mount('ism-background-activity');
    await vi.waitFor(() => expect(page.textContent).toContain('Saved activity could not be read'));
    expect(page.textContent).toContain('Started');
    expect(page.textContent).not.toContain('Elapsed');
    expect(page.textContent).not.toContain('Duration');
    expect(page.textContent).not.toContain('NaN');
  });

  it('groups bulk blockers into one row with reasons and examples, without a misleading progress bar', async () => {
    vi.spyOn(admin, 'backgroundActivity').mockResolvedValue(
      snapshot([
        task,
        {
          ...task,
          id: 'state:outgoing:peer:share:failed',
          title: 'Outgoing peer synchronization',
          resource: 'Peer B / Archive',
          status: 'blocked',
          phase: 'The receiving ISM reported a failure',
          done: 3_000_000,
          total: 3_000_000,
          unit: 'bytes',
          count: 3162,
          reasons: [
            { reason: 'receiver disk full', count: 2000 },
            { reason: '<item> changed on the receiver', count: 1162 },
          ],
          examples: [{ resource: 'Trip/a.example.jpg', detail: 'receiver disk full' }],
        },
      ]),
    );
    const page = await mount('ism-background-activity');
    await vi.waitFor(() => expect(page.textContent).toContain('3,162 files'));
    expect(page.querySelectorAll('#activity-panel li > details')).toHaveLength(1);
    expect(page.querySelector('#activity-panel progress')).toBeNull();
    expect(page.textContent).toContain('The receiving instance has run out of storage');
    expect(page.textContent).toContain('The file changed on the other instance');
    expect(page.textContent).not.toContain('receiver disk full');
    expect(page.textContent).toContain('2,000');
    expect(page.textContent).toContain('Examples (1 of 3,162)');
    expect(page.textContent).toContain('Trip/a.example.jpg');
    [...page.querySelectorAll<HTMLButtonElement>('[role=tab]')]
      .find((b) => b.textContent?.includes('Running'))
      ?.click();
    await page.updateComplete;
    expect(page.textContent).toContain('Checking shared files');
    expect(page.textContent).not.toContain('Peer B / Archive');
  });

  it('links grouped conflict comparisons to Conflicts', async () => {
    vi.spyOn(admin, 'backgroundActivity').mockResolvedValue(
      snapshot([
        {
          ...task,
          id: 'state:conflict-evidence:blocked',
          task_key: 'conflict-evidence:blocked',
          title: 'Conflict evidence preparation',
          resource: 'Conflicts',
          status: 'blocked',
          phase: 'The comparison cannot continue automatically',
          unit: 'conflicts',
          count: 3,
          page: '/conflicts',
        },
      ]),
    );
    const page = await mount('ism-background-activity');
    await vi.waitFor(() => expect(page.textContent).toContain('3 conflicts'));
    expect(page.querySelector('a[href="/conflicts"]')?.textContent).toContain('Open conflicts');
  });

  it('links remote blockers to the other instance without routing them locally', async () => {
    vi.spyOn(admin, 'backgroundActivity').mockResolvedValue(
      snapshot([
        {
          ...task,
          id: 'state:peer-attention:peer:conflict',
          task_key: 'peer-attention:peer:conflict',
          status: 'blocked',
          page: 'https://peer.example/ism/conflicts',
        },
      ]),
    );
    const page = await mount('ism-background-activity');
    await vi.waitFor(() => {
      const link = page.querySelector('a[href="https://peer.example/ism/conflicts"]');
      expect(link?.textContent).toContain('Open conflicts');
      expect(link?.getAttribute('rel')).toBe('external');
    });
  });

  it('takes version decisions to Conflicts instead of the sync queue', async () => {
    vi.spyOn(admin, 'backgroundActivity').mockResolvedValue(
      snapshot([
        {
          ...task,
          task_key: 'recovery:conflict:needs_decision',
          title: 'Recovery verification',
          resource: 'conflict',
          status: 'blocked',
          phase: 'Waiting for an administrator to choose a version',
          page: '/sync',
        },
      ]),
    );
    const page = await mount('ism-background-activity');
    await vi.waitFor(() => expect(page.textContent).toContain('Compare the versions in Conflicts'));
    expect(page.querySelector('a[href="/conflicts"]')?.textContent).toContain('Open conflicts');
    expect(page.querySelector('a[href="/sync?view=attention"]')).toBeNull();
  });

  it('explains newly reported waits and unknown comparison status in plain language', async () => {
    const data = snapshot([
      {
        ...task,
        id: 'state:queue',
        title: 'Peer file transfer queue',
        status: 'waiting',
        phase: 'Peer bookkeeping waits for active managed operations',
      },
      {
        ...task,
        id: 'state:inbox',
        title: 'Peer event application',
        status: 'waiting',
        phase: 'Received changes await dependency verification and application',
      },
      {
        ...task,
        id: 'state:comparison',
        title: 'Conflict evidence preparation',
        status: 'waiting',
        phase: 'The saved comparison state or worker could not be verified',
      },
      {
        ...task,
        id: 'state:slots',
        title: 'Peer file transfer queue',
        status: 'waiting',
        phase: 'ISM records 2 running transfers (limit 2); queue readiness is not fully known',
      },
    ]);
    data.warnings = ['Some conflict comparison work has no verifiable worker or saved state.'];
    vi.spyOn(admin, 'backgroundActivity').mockResolvedValue(data);
    const page = await mount('ism-background-activity');
    await vi.waitFor(() =>
      expect(page.textContent).toContain('Waiting for another file operation to finish'),
    );
    expect(page.textContent).toContain(
      'Waiting to check and apply changes from the other instance',
    );
    expect(page.textContent).toContain('Comparing file versions');
    expect(page.textContent).toContain("ISM can't confirm whether these comparisons are running");
    expect(page.textContent).toContain('2 of 2 transfer slots in use');
    expect(page.textContent).toContain("ISM can't confirm the status of some file comparisons");
    expect(page.textContent).not.toContain('bookkeeping');
    expect(page.textContent).not.toContain('dependency verification');
    expect(page.textContent).not.toContain('queue readiness');
    expect(page.textContent).not.toContain('evidence preparation');
  });

  it('explains dependent work and version conflicts without internal jargon', async () => {
    vi.spyOn(admin, 'backgroundActivity').mockResolvedValue(
      snapshot([
        {
          ...task,
          id: 'state:transfer-queue:peer:share',
          title: 'Peer file transfer queue',
          resource: 'Peer B / Archive',
          status: 'blocked',
          phase:
            'Next files in the queue are waiting: peer publication waits for a dependent inbox effect',
          count: 100,
          reasons: [{ reason: 'peer publication waits for a dependent inbox effect', count: 6 }],
          examples: [
            {
              resource: 'trip/photo.example.jpg',
              detail: 'peer publication waits for a dependent inbox effect',
            },
          ],
          page: '/sync',
        },
        {
          ...task,
          id: 'state:inbox:peer:share',
          title: 'Peer event application',
          resource: 'Peer B / Archive',
          status: 'blocked',
          phase: 'peer effect waits for conflict resolution',
          page: '/sync',
        },
      ]),
    );
    const page = await mount('ism-background-activity');
    await vi.waitFor(() =>
      expect(page.textContent).toContain('An earlier change to this file must finish first'),
    );
    expect(page.textContent).toContain('Check Sync issues if it needs attention.');
    expect(page.textContent).not.toContain('ISM will continue automatically.');
    expect(page.textContent).toContain('Reasons (6 of 100)');
    expect(page.textContent).toContain('Files have different versions');
    expect(page.textContent).not.toContain('dependent inbox');
    expect(page.textContent).not.toContain('peer publication');
    expect(page.textContent).not.toContain('peer effect');
    expect(page.querySelector('a[href="/sync?view=attention"]')?.textContent).toContain('sync');
    expect(page.querySelector('a[href="/conflicts"]')?.textContent).toContain('Open conflicts');
  });

  it('explains saved transfer failures while preserving unknown details', async () => {
    vi.spyOn(admin, 'backgroundActivity').mockResolvedValue(
      snapshot([
        {
          ...task,
          status: 'blocked',
          page: '/sync',
          error: 'downloaded data does not match its checksum',
          reasons: [
            { reason: 'peer file changed during the transfer', count: 1 },
            { reason: 'peer returned unexpected Content-Range (0, 4, 10)', count: 1 },
            { reason: 'peer returned 3 of 10 requested bytes', count: 1 },
            { reason: 'A specific server error', count: 1 },
          ],
        },
      ]),
    );
    const page = await mount('ism-background-activity');
    await vi.waitFor(() =>
      expect(page.textContent).toContain('The downloaded file did not match the original'),
    );
    expect(page.textContent).toContain('The file changed on the other instance during download');
    expect(page.textContent).toContain('The other instance sent an unexpected part of the file');
    expect(page.textContent).toContain('The other instance sent an incomplete file');
    expect(page.textContent).toContain('A specific server error');
    expect(page.textContent).not.toContain('Content-Range');
    expect(page.textContent).not.toContain('checksum');
  });

  it('uses plain language for current activity and older saved runs', async () => {
    const data = snapshot([
      {
        ...task,
        title: 'Peer inventory backfill',
        phase: 'Fetching inventory pages and saving durable receipts',
        unit: 'inventory entries checked',
      },
      {
        ...task,
        id: 'legacy-check',
        title: 'Prepare Immich write barrier',
        phase: 'Verifying checksum for trip/photo.example.jpg',
        unit: 'bytes hashed',
        total: 100,
      },
    ]);
    data.history = [
      {
        ...task,
        id: 'legacy-history',
        status: 'completed',
        phase: 'Inventory pagination will continue after trip/photo.example.jpg',
        reasons: [{ reason: '<item> changed on the receiver', count: 1 }],
      },
    ];
    vi.spyOn(admin, 'backgroundActivity').mockResolvedValue(data);
    const page = await mount('ism-background-activity');
    await vi.waitFor(() => expect(page.textContent).toContain('Getting the list of files'));
    expect(page.textContent).toContain('Checking the contents of trip/photo.example.jpg');
    expect(page.textContent).toContain('Continuing to check files after trip/photo.example.jpg');
    expect(page.textContent).toContain('The file changed on the other instance');
    expect(page.textContent).toContain('files checked');
    expect(page.textContent).not.toContain('write barrier');
    expect(page.textContent).not.toContain('durable receipts');
    expect(page.textContent).not.toContain('checksum');
    expect(page.textContent).not.toContain('pagination');
    expect(page.textContent).not.toContain('<item>');
  });

  it('explains an Immich safety wait without internal terminology', async () => {
    vi.spyOn(admin, 'backgroundActivity').mockResolvedValue(
      snapshot([
        {
          ...task,
          status: 'waiting',
          title: 'Library synchronization',
          phase: 'Immich library queue is paused by a write barrier; waiting for release',
        },
      ]),
    );
    const page = await mount('ism-background-activity');
    await vi.waitFor(() =>
      expect(page.textContent).toContain('Waiting for Immich to finish another file change'),
    );
    expect(page.textContent).not.toContain('write barrier');
    expect(page.textContent).not.toContain('queue');
  });

  it('keeps interrupted work visible until a later outcome is known', async () => {
    vi.spyOn(admin, 'backgroundActivity').mockResolvedValue(
      snapshot([
        {
          ...task,
          id: 'restarted',
          title: 'Peer file transfer',
          status: 'interrupted',
          phase:
            'ISM restarted before this run reported an outcome; queued work is recovered separately',
          finished_at: '2026-01-01T00:01:00Z',
        },
        task,
      ]),
    );
    const page = await mount('ism-background-activity');
    await vi.waitFor(() =>
      expect(page.querySelector('#activity-panel')?.textContent).toContain('File transfer'),
    );
    expect(page.querySelector('[aria-selected="true"]')?.textContent).toContain('Needs attention');
    expect(page.textContent).toContain('ISM restarted before this finished');
    expect(page.textContent).toContain('Duration');
    expect(page.textContent).toContain('1 min');
  });

  it('explains waiting and failed work, and opens related operations without unsupported actions', async () => {
    vi.spyOn(admin, 'backgroundActivity').mockResolvedValue(
      snapshot([
        {
          ...task,
          id: 'waiting',
          status: 'waiting',
          phase: 'Waiting for allowed file-transfer hours',
          next_at: '2026-01-02T00:00:00Z',
          done: 0,
        },
        {
          ...task,
          id: 'failed',
          status: 'failed',
          phase: 'Needs attention',
          error: 'Unverified file changes',
          operation_id: 'op-example',
        },
      ]),
    );
    const page = await mount('ism-background-activity');
    await vi.waitFor(() => expect(page.textContent).toContain('Unverified file changes'));
    const opened = vi.fn();
    page.addEventListener('operation-open', opened);
    [...page.querySelectorAll('button')]
      .find((b) => b.textContent === 'Open related operation')
      ?.click();
    expect(opened.mock.calls[0]?.[0].detail).toBe('op-example');
    expect(page.textContent).not.toContain('Cancel task');
    [...page.querySelectorAll<HTMLButtonElement>('[role=tab]')]
      .find((b) => b.textContent?.includes('Waiting'))
      ?.click();
    await page.updateComplete;
    expect(page.textContent).toContain('Waiting for the scheduled sync hours');
    expect(page.textContent).toContain('Scheduled for:');
  });

  it('preserves the last snapshot on errors and never presents a failed fetch as idle', async () => {
    const get = vi.spyOn(admin, 'backgroundActivity').mockResolvedValue(snapshot());
    const page = await mount('ism-background-activity');
    await vi.waitFor(() => expect(page.textContent).toContain('Enumerating files'));
    await page.reload();
    get.mockRejectedValue(new Error('connection lost'));
    await page.reload();
    await page.updateComplete;
    expect(page.textContent).toContain("Couldn't refresh");
    expect(page.textContent).toContain('Showing what was known');
    expect(page.textContent).toContain('Enumerating files');
    expect(page.textContent).not.toContain('ISM is idle');
  });

  it('reports unknown activity on initial failure and polls independently of live updates', async () => {
    vi.useFakeTimers();
    const get = vi.spyOn(admin, 'backgroundActivity').mockRejectedValue(new Error('unavailable'));
    const page = await mount('ism-background-activity');
    await page.reload();
    await page.updateComplete;
    expect(page.textContent).toContain('Current activity is unknown');
    get.mockResolvedValue(snapshot([]));
    await vi.advanceTimersByTimeAsync(10_000);
    await page.updateComplete;
    expect(page.textContent).toContain('ISM is idle');
    events.emit({ type: 'resync' });
    await vi.advanceTimersByTimeAsync(2000);
    expect(get.mock.calls.length).toBeGreaterThan(2);
  });
});
