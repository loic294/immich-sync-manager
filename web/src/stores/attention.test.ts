import { afterEach, describe, expect, it, vi } from 'vitest';
import { admin } from '@/api/admin';
import type { BackgroundActivity } from '@/api/generated/BackgroundActivity';
import type { ConflictView } from '@/api/generated/ConflictView';
import type { Overview } from '@/api/generated/Overview';
import type { RecoveryView } from '@/api/generated/RecoveryView';
import {
  blockers,
  buildAttention,
  dismissNotification,
  notifications,
  refreshAttention,
  startAttention,
} from '@/stores/attention';

const overview: Overview = {
  instance_name: 'Alpha',
  servers: [],
  roots_total: 0,
  roots_unavailable: 0,
  operations_running: 0,
  operations_attention: 0,
  transfers_queued: 0,
  conflicts_open: 0,
  peers_online: 0,
  peers_total: 0,
  uncovered_libraries: 0,
  watchers: [],
  scans: [],
};
const row: BackgroundActivity = {
  id: 'state:queue',
  task_key: 'transfer-queue:peer:share',
  title: 'Transfer',
  resource: 'Beta / Photos',
  status: 'waiting',
  phase: 'Waiting',
  done: 0,
  total: null,
  unit: 'items',
  started_at: '2026-01-01T00:00:00Z',
  updated_at: '2026-01-01T00:00:00Z',
  finished_at: null,
  next_at: null,
  error: null,
  operation_id: null,
  page: '/sync',
  count: 2,
  reasons: [],
  examples: [],
};
const conflict: ConflictView = {
  id: 'one',
  share_name: 'Photos',
  location: { root_id: 'photos', path: 'image.jpg' },
  kind: 'changed_on_both',
  detail: '',
  local: { instance: 'Alpha', sha256: null, size: null, modified_at: null, deleted: false },
  remote: { instance: 'Beta', sha256: null, size: null, modified_at: null, deleted: false },
  peer_id: 'peer',
  status: 'open',
  created_at: '2026-01-01T00:00:00Z',
  moved_from: null,
};
const recovery: RecoveryView = {
  target: { kind: 'conflict', id: 'one', peer_id: null },
  title: 'image.jpg',
  state: 'queued',
  code: null,
  detail: null,
  can_fix: false,
  can_dismiss: false,
  replacement_operation_id: null,
};
const empty = { overview, activity: [], operations: [], conflicts: [], recovery: [], mappings: [] };

afterEach(() => {
  vi.restoreAllMocks();
  localStorage.clear();
});

describe('attention derivation', () => {
  it('explains offline and scheduled queues without claiming ready work is blocked', () => {
    const items = buildAttention({
      ...empty,
      activity: [
        { ...row, phase: 'Waiting for the peer to come online' },
        {
          ...row,
          task_key: 'transfer-queue:other:share',
          phase: 'Next files in the queue are waiting: Waiting for allowed file-transfer hours',
          reasons: [{ reason: 'Waiting for allowed file-transfer hours', count: 2 }],
        },
        {
          ...row,
          task_key: 'transfer-queue:ready:share',
          phase: 'Next file starts as soon as the transfer worker picks it up',
        },
      ],
    });
    expect(items).toHaveLength(2);
    expect(items[0]).toMatchObject({ href: '/peers', title: 'Sync is waiting for Beta' });
    expect(items[1]).toMatchObject({
      href: '/sync',
      title: 'Transfers are waiting for allowed hours',
    });
  });

  it('does not request another decision for queued or resolving conflicts', () => {
    expect(buildAttention({ ...empty, conflicts: [conflict], recovery: [recovery] })).toEqual([]);
    expect(
      buildAttention({
        ...empty,
        conflicts: [conflict],
        recovery: [{ ...recovery, state: 'needs_decision' }],
      })[0],
    ).toMatchObject({ href: '/conflicts', blocking: true });
    expect(buildAttention({ ...empty, conflicts: [{ ...conflict, status: 'resolving' }] })).toEqual(
      [],
    );
  });

  it('identifies an offline receiver with queued work without promising failed transfers will resume', () => {
    const peer = {
      ...row,
      task_key: 'peer:beta',
      resource: 'Beta',
      phase: 'Waiting for the peer to come online',
    };
    const queued = {
      ...row,
      task_key: 'outgoing:beta:Photos:queued',
      phase: 'Queued on the receiving ISM',
    };
    expect(buildAttention({ ...empty, activity: [peer, queued] })).toMatchObject([
      {
        title: 'Sync is waiting for Beta',
        href: '/peers',
        detail: 'Reconnect the other instance to check its queued transfers.',
      },
    ]);
    expect(
      buildAttention({
        ...empty,
        activity: [
          peer,
          queued,
          {
            ...row,
            task_key: 'transfer-queue:beta:Photos',
            resource: 'Beta / Photos',
            phase: 'Waiting for the peer to come online',
          },
        ],
      }),
    ).toHaveLength(1);
    expect(
      buildAttention({
        ...empty,
        activity: [peer, { ...queued, task_key: 'outgoing:beta:Photos:failed', status: 'blocked' }],
      }),
    ).toMatchObject([{ title: 'Transfers need attention on the other instance', href: '/sync' }]);
  });

  it('keeps acknowledged unresolved operations as blockers without a new notice', () => {
    const items = buildAttention({
      ...empty,
      operations: [
        {
          operation_id: 'one',
          title: 'Move',
          label: null,
          source: 'Alpha',
          destination: 'Alpha',
          status: 'recovery_required',
          progress: { done: 1, total: 1 },
          created_at: conflict.created_at,
          finished_at: conflict.created_at,
          handled_at: conflict.created_at,
          recovery_resolved: false,
        },
      ],
    });
    expect(items[0]).toMatchObject({ blocking: true, notifiable: false, href: '/operations' });
  });

  it('does not restore data from an in-flight request after sign out', async () => {
    let resolve!: (value: Overview) => void;
    vi.spyOn(admin, 'overview').mockReturnValue(
      new Promise((done) => {
        resolve = done;
      }),
    );
    vi.spyOn(admin, 'backgroundActivity').mockResolvedValue({
      current: [],
      history: [],
      total: 0,
      observed_at: '',
      warnings: [],
    });
    vi.spyOn(admin, 'operationPage').mockResolvedValue({ rows: [], total: 0 });
    vi.spyOn(admin, 'conflicts').mockResolvedValue([conflict]);
    vi.spyOn(admin, 'recoveryPage').mockResolvedValue({ rows: [], total: 0 });
    vi.spyOn(admin, 'mappings').mockResolvedValue([]);
    const stop = startAttention();
    stop();
    resolve(overview);
    await vi.waitFor(() => expect(admin.overview).toHaveBeenCalled());
    await new Promise((done) => setTimeout(done, 0));
    expect(blockers.value).toEqual([]);
    expect(notifications.value).toEqual([]);
  });

  it('keeps a dismissed notification hidden until its blocker resolves and returns', async () => {
    vi.spyOn(admin, 'overview').mockResolvedValue(overview);
    vi.spyOn(admin, 'backgroundActivity').mockResolvedValue({
      current: [],
      history: [],
      total: 0,
      observed_at: '',
      warnings: [],
    });
    vi.spyOn(admin, 'operationPage').mockResolvedValue({ rows: [], total: 0 });
    const fetchConflicts = vi.spyOn(admin, 'conflicts').mockResolvedValue([conflict]);
    vi.spyOn(admin, 'recoveryPage').mockResolvedValue({ rows: [], total: 0 });
    vi.spyOn(admin, 'mappings').mockResolvedValue([]);
    const stop = startAttention();
    try {
      await refreshAttention();
      expect(notifications.value).toHaveLength(1);
      dismissNotification(notifications.value[0].id);
      await refreshAttention();
      expect(notifications.value).toEqual([]);
      expect(blockers.value).toHaveLength(1);
      fetchConflicts.mockResolvedValue([]);
      await refreshAttention();
      expect(blockers.value).toEqual([]);
      fetchConflicts.mockResolvedValue([conflict]);
      await refreshAttention();
      expect(notifications.value).toHaveLength(1);
    } finally {
      stop();
    }
  });
});
