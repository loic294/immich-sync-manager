import { afterEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import type { AttentionItem } from '@/api/generated/AttentionItem';
import type { AttentionSnapshot } from '@/api/generated/AttentionSnapshot';
import {
  attentionError,
  blockers,
  buildAttention,
  dismissNotification,
  notifications,
  refreshAttention,
  startAttention,
} from '@/stores/attention';

const row: AttentionItem = {
  id: 'peer:beta',
  code: 'peer_offline',
  state: 'waiting',
  attention: 'notice',
  blocks_work: true,
  owner: 'local',
  instance_id: 'alpha',
  instance_name: 'Alpha',
  peer_id: 'beta',
  share_id: 'photos',
  share_name: 'Photos',
  count: 1,
  total_count: 1,
  next_at: null,
  observed_at: '2026-01-01T00:00:00Z',
  title: 'Sync is waiting for Beta',
  message: 'Reconnect the other instance to check its queued transfers.',
  href: '/peers',
  recheck_at: null,
  acknowledged: null,
  operation_id: null,
  unresolved: true,
  completeness: 'complete',
  examples: [],
};
const snapshot: AttentionSnapshot = {
  observed_at: '2026-01-01T00:00:00Z',
  completeness: 'complete',
  warnings: [],
  coverage: [],
  items: [row],
};

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  localStorage.clear();
});

describe('attention snapshots', () => {
  it('uses confirmed blocker and notification classifications independently', () => {
    const items = buildAttention({
      ...snapshot,
      items: [
        row,
        { ...row, id: 'ready', attention: 'none', blocks_work: false },
        { ...row, id: 'routine-wait', attention: 'none', blocks_work: true },
        { ...row, id: 'failure', attention: 'action_required', blocks_work: false },
        { ...row, id: 'acknowledged', attention: 'action_required', acknowledged: true },
      ],
    });
    expect(items.map((entry) => entry.id)).toEqual(['peer:beta', 'failure', 'acknowledged']);
    expect(items[0]).toMatchObject({ blocking: true, notifiable: true, action: 'Open peers' });
    expect(items[1]).toMatchObject({ blocking: false, notifiable: true });
    expect(items[2]).toMatchObject({ blocking: true, notifiable: false });
  });

  it('marks peer links as external while retaining their action labels', () => {
    const [item] = buildAttention({
      ...snapshot,
      items: [{ ...row, owner: 'receiver', href: 'https://receiver.example/conflicts' }],
    });
    expect(item).toMatchObject({ external: true, action: 'Open conflicts' });
  });

  it('preserves confirmed blockers and dismissed notices when evidence is partial', async () => {
    const get = vi.spyOn(api, 'get').mockResolvedValue(snapshot);
    const stop = startAttention();
    try {
      await refreshAttention();
      dismissNotification('peer:beta');
      get.mockResolvedValue({ ...snapshot, completeness: 'partial', items: [] });
      await refreshAttention();
      expect(attentionError.value).toBe(true);
      expect(blockers.value[0]).toMatchObject({ id: 'peer:beta', stale: true });
      get.mockResolvedValue({
        ...snapshot,
        completeness: 'partial',
        items: [{ ...row, completeness: 'unavailable', attention: 'none', blocks_work: false }],
      });
      await refreshAttention();
      expect(blockers.value[0]).toMatchObject({ id: 'peer:beta', stale: true });
      get.mockResolvedValue({ ...snapshot, completeness: 'partial' });
      await refreshAttention();
      expect(blockers.value).toHaveLength(1);
      expect(notifications.value).toEqual([]);
      get.mockResolvedValue({ ...snapshot, items: [] });
      await refreshAttention();
      get.mockResolvedValue(snapshot);
      await refreshAttention();
      expect(notifications.value).toHaveLength(1);
    } finally {
      stop();
    }
  });

  it('rechecks at a server-provided transition without polling', async () => {
    vi.useFakeTimers();
    const get = vi.spyOn(api, 'get').mockResolvedValue({
      ...snapshot,
      items: [{ ...row, next_at: new Date(Date.now() + 2000).toISOString() }],
    });

    const stop = startAttention();
    try {
      await vi.advanceTimersByTimeAsync(0);
      expect(get).toHaveBeenCalledTimes(1);
      get.mockResolvedValue({ ...snapshot, items: [] });
      await vi.advanceTimersByTimeAsync(2100);
      expect(get).toHaveBeenCalledTimes(2);
      expect(blockers.value).toEqual([]);
      await vi.advanceTimersByTimeAsync(60_000);
      expect(get).toHaveBeenCalledTimes(2);
    } finally {
      stop();
    }
  });

  it('clears absent covered sources and their persisted dismissals without clearing unknown sources', async () => {
    const remote = { ...row, id: 'receiver:beta', owner: 'receiver' as const };
    const get = vi.spyOn(api, 'get').mockResolvedValue({
      ...snapshot,
      items: [row, remote],
    });
    let stop = startAttention();
    try {
      await refreshAttention();
      dismissNotification(row.id);
      stop();
      get.mockResolvedValue({
        ...snapshot,
        completeness: 'partial',
        warnings: ['Remote status is unavailable.'],
        coverage: [{ code: row.code, owner: 'local', completeness: 'complete' }],
        items: [],
      });
      stop = startAttention();
      await refreshAttention();
      get.mockResolvedValue({ ...snapshot, completeness: 'partial', items: [row, remote] });
      await refreshAttention();
      expect(notifications.value.map((entry) => entry.id)).toEqual([row.id, remote.id]);
      get.mockResolvedValue({
        ...snapshot,
        completeness: 'partial',
        coverage: [{ code: row.code, owner: 'local', completeness: 'complete' }],
        items: [],
      });
      await refreshAttention();
      expect(blockers.value).toMatchObject([{ id: remote.id, stale: true }]);
    } finally {
      stop();
    }
  });

  it('keeps same-ID unavailable evidence and dismissals despite covered source absence', async () => {
    const get = vi.spyOn(api, 'get').mockResolvedValue(snapshot);
    const stop = startAttention();
    try {
      await refreshAttention();
      dismissNotification(row.id);
      get.mockResolvedValue({
        ...snapshot,
        completeness: 'partial',
        coverage: [{ code: row.code, owner: row.owner, completeness: 'complete' }],
        items: [{ ...row, attention: 'none', completeness: 'unavailable', blocks_work: false }],
      });
      await refreshAttention();
      expect(blockers.value).toMatchObject([{ id: row.id, stale: true }]);
      get.mockResolvedValue({ ...snapshot, completeness: 'partial' });
      await refreshAttention();
      expect(notifications.value).toEqual([]);
      get.mockResolvedValue({
        ...snapshot,
        completeness: 'partial',
        items: [{ ...row, attention: 'none', completeness: 'complete', blocks_work: false }],
      });
      await refreshAttention();
      expect(blockers.value).toEqual([]);
      get.mockResolvedValue({ ...snapshot, completeness: 'partial' });
      await refreshAttention();
      expect(notifications.value).toHaveLength(1);
    } finally {
      stop();
    }
  });

  it('rechecks partial snapshots at the authoritative transition before the retry interval', async () => {
    vi.useFakeTimers();
    const get = vi.spyOn(api, 'get').mockResolvedValue({
      ...snapshot,
      completeness: 'partial',
      warnings: ['Some readiness is unknown.'],
      items: [
        {
          ...row,
          recheck_at: new Date(Date.now() + 2000).toISOString(),
          next_at: new Date(Date.now() + 3_600_000).toISOString(),
        },
      ],
    });
    const stop = startAttention();
    try {
      await vi.advanceTimersByTimeAsync(0);
      expect(attentionError.value).toBe(true);
      expect(blockers.value).toHaveLength(1);
      get.mockResolvedValue({ ...snapshot, items: [] });
      await vi.advanceTimersByTimeAsync(2100);
      expect(get).toHaveBeenCalledTimes(2);
      expect(attentionError.value).toBe(false);
      expect(blockers.value).toEqual([]);
    } finally {
      stop();
    }
  });

  it('retains and marks previous blockers when refresh fails, then retries', async () => {
    vi.useFakeTimers();
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const get = vi.spyOn(api, 'get').mockResolvedValue(snapshot);
    const stop = startAttention();
    try {
      await vi.advanceTimersByTimeAsync(0);
      get.mockRejectedValueOnce(new Error('offline'));
      await refreshAttention();
      expect(attentionError.value).toBe(true);
      expect(blockers.value[0]).toMatchObject({ id: 'peer:beta', stale: true });
      await vi.advanceTimersByTimeAsync(60_000);
      expect(attentionError.value).toBe(false);
      expect(blockers.value[0]).not.toHaveProperty('stale');
    } finally {
      stop();
    }
  });

  it('ignores a snapshot that arrives after sign out', async () => {
    let resolve!: (value: typeof snapshot) => void;
    vi.spyOn(api, 'get').mockReturnValue(
      new Promise((done) => {
        resolve = done;
      }),
    );
    const stop = startAttention();
    stop();
    resolve(snapshot);
    await new Promise((done) => setTimeout(done, 0));
    expect(blockers.value).toEqual([]);
    expect(notifications.value).toEqual([]);
  });
});
