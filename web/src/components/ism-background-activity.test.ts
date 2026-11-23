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
    get.mockResolvedValue(
      snapshot([
        { ...task, done: 15, total: 50, unit: 'files checked', phase: 'Reconciling files' },
      ]),
    );
    events.emit({ type: 'activity_changed' });
    await vi.waitFor(() => expect(page.textContent).toContain('15 / 50 files checked'));
    expect(page.querySelector('progress')?.value).toBe(15);
    page
      .querySelector('ism-pagination')
      ?.dispatchEvent(new CustomEvent('page-change', { detail: { limit: 25, offset: 25 } }));
    await vi.waitFor(() => expect(get).toHaveBeenCalledWith(25, 25));
    expect(page.textContent).toContain('Share inventory scan');
    expect(page.querySelector('a')?.getAttribute('href')).toBe('/peers');
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
    await vi.waitFor(() =>
      expect(page.textContent).toContain('Waiting for allowed file-transfer hours'),
    );
    expect(page.textContent).toContain('Next eligible:');
    expect(page.textContent).toContain('Unverified file changes');
    const opened = vi.fn();
    page.addEventListener('operation-open', opened);
    [...page.querySelectorAll('button')]
      .find((b) => b.textContent === 'Open related operation')
      ?.click();
    expect(opened.mock.calls[0]?.[0].detail).toBe('op-example');
    expect(page.textContent).not.toContain('Cancel task');
  });

  it('preserves the last snapshot on errors and never presents a failed fetch as idle', async () => {
    const get = vi.spyOn(admin, 'backgroundActivity').mockResolvedValue(snapshot());
    const page = await mount('ism-background-activity');
    await vi.waitFor(() => expect(page.textContent).toContain('Enumerating files'));
    await page.reload();
    get.mockRejectedValue(new Error('connection lost'));
    await page.reload();
    await page.updateComplete;
    expect(page.textContent).toContain('Previously received activity is shown below');
    expect(page.textContent).toContain('Enumerating files');
    expect(page.textContent).not.toContain('No background work');
    expect(page.textContent).toContain('Live updates are disconnected');
  });

  it('reports unknown activity on initial failure and polls independently of live updates', async () => {
    vi.useFakeTimers();
    const get = vi.spyOn(admin, 'backgroundActivity').mockRejectedValue(new Error('unavailable'));
    const page = await mount('ism-background-activity');
    await page.reload();
    await page.updateComplete;
    expect(page.textContent).toContain('Current activity is unknown');
    get.mockResolvedValue(snapshot([]));
    await vi.advanceTimersByTimeAsync(5000);
    await page.updateComplete;
    expect(page.textContent).toContain(
      'No background work or sync blockers reported at the last check',
    );
    events.emit({ type: 'resync' });
    await vi.advanceTimersByTimeAsync(1000);
    expect(get.mock.calls.length).toBeGreaterThan(2);
  });
});
