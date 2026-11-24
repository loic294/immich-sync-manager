import { signal } from '@preact/signals-core';
import { admin } from '@/api/admin';
import { events } from '@/api/events';
import type { BackgroundActivity } from '@/api/generated/BackgroundActivity';
import type { ConflictView } from '@/api/generated/ConflictView';
import type { MappingView } from '@/api/generated/MappingView';
import type { OperationActivity } from '@/api/generated/OperationActivity';
import type { Overview } from '@/api/generated/Overview';
import type { RecoveryView } from '@/api/generated/RecoveryView';
import { formatDate } from '@/util/format';

const DISMISSED_KEY = 'ism.dismissed-attention.v1';

export interface AttentionItem {
  id: string;
  title: string;
  detail: string;
  href: string;
  action: string;
  blocking: boolean;
  notifiable?: boolean;
}

interface AttentionData {
  overview: Overview;
  activity: BackgroundActivity[];
  operations: OperationActivity[];
  conflicts: ConflictView[];
  recovery: RecoveryView[];
  mappings: MappingView[];
}

export const blockers = signal<AttentionItem[]>([]);
export const notifications = signal<AttentionItem[]>([]);
export const attentionError = signal(false);

let dismissed = new Set<string>();
let timer: number | undefined;
let transitionTimer: number | undefined;
let pending: Promise<void> | undefined;
let refreshAgain = false;
let generation = 0;
let running = false;
let lastRefreshAt = 0;

function readDismissed(): Set<string> {
  try {
    const stored: unknown = JSON.parse(localStorage.getItem(DISMISSED_KEY) ?? '[]');
    return new Set(
      Array.isArray(stored) ? stored.filter((id): id is string => typeof id === 'string') : [],
    );
  } catch (error) {
    console.warn('[attention] could not read dismissed notifications', error);
    return new Set();
  }
}

function saveDismissed() {
  try {
    localStorage.setItem(DISMISSED_KEY, JSON.stringify([...dismissed]));
  } catch (error) {
    console.warn('[attention] could not save dismissed notifications', error);
  }
}

function item(
  id: string,
  title: string,
  detail: string,
  href: string,
  action: string,
  blocking = true,
): AttentionItem {
  return { id, title, detail, href, action, blocking };
}

function activityItem(row: BackgroundActivity): AttentionItem | undefined {
  const key = row.task_key;
  if (key.startsWith('transfer-queue:')) {
    const reason = row.reasons[0]?.reason ?? row.phase;
    if (row.phase.includes('peer to come online')) {
      return item(
        key,
        `Sync is waiting for ${row.resource.split(' / ')[0]}`,
        'Reconnect the other instance to resume transfers.',
        '/peers',
        'Open peers',
      );
    }
    if (
      row.phase.startsWith('Next files in the queue are waiting:') &&
      reason.includes('allowed file-transfer hours')
    ) {
      const when = row.next_at ? ` Transfers can resume ${formatDate(row.next_at)}.` : '';
      return item(
        key,
        'Transfers are waiting for allowed hours',
        `Change the hours or add an override in Sync.${when}`,
        '/sync',
        'Open Sync',
      );
    }
    if (row.status === 'blocked') {
      return item(
        key,
        'Transfers cannot continue',
        'Check the shared folder and any unresolved operations.',
        '/sync',
        'Open Sync',
      );
    }
    return undefined;
  }
  if (
    key.startsWith('transfer-failed:') ||
    (key.startsWith('outgoing:') && row.status === 'blocked')
  ) {
    return item(
      key,
      key.startsWith('outgoing:')
        ? 'Transfers need attention on the other instance'
        : 'Transfers need attention',
      key.startsWith('outgoing:')
        ? 'Open Sync to review the reported problem on the receiving instance.'
        : 'Open Sync to review and repair the failed transfers.',
      '/sync',
      'Open Sync',
    );
  }
  if (key.startsWith('inbox:') || key.startsWith('inventory:')) {
    if (row.status !== 'blocked') return undefined;
    return item(
      key,
      'Changes from a peer cannot be applied',
      'Open peers to review the affected share and resolve the problem.',
      '/peers',
      'Open peers',
    );
  }
  if (key.startsWith('watch:') && row.status === 'blocked') {
    return item(
      key,
      `File watching paused for ${row.resource}`,
      'Check the folder mapping to resume change detection.',
      '/mappings',
      'Open folder mappings',
    );
  }
  if (key.startsWith('library:') && row.status === 'blocked') {
    return item(
      key,
      `Library sync needs attention`,
      `Check ${row.resource} and its server connection.`,
      '/mappings',
      'Open folder mappings',
    );
  }
  return undefined;
}

export function buildAttention(data: AttentionData): AttentionItem[] {
  const items: AttentionItem[] = [];
  for (const server of data.overview.servers) {
    const m = server.maintenance;
    if (m.drift !== null) {
      items.push(
        item(
          `server:${server.id}:drift:${m.epoch}`,
          `Writes to ${server.name} are paused`,
          'Restore the required Immich settings to resume writes.',
          '/servers',
          'Open servers',
        ),
      );
    } else if (!server.compatible || m.phase === 'blocked' || m.phase === 'recovery_required') {
      items.push(
        item(
          `server:${server.id}:${m.phase}:${m.epoch}`,
          `${server.name} needs attention`,
          'Check server status before continuing file changes.',
          '/servers',
          'Open servers',
        ),
      );
    }
  }
  for (const row of data.activity) {
    const entry = activityItem(row);
    if (entry) items.push(entry);
  }
  for (const peer of data.activity) {
    if (!peer.task_key.startsWith('peer:')) continue;
    const id = peer.task_key.slice('peer:'.length);
    const hasOutgoingQueue = data.activity.some(
      (row) =>
        row.task_key.startsWith(`outgoing:${id}:`) &&
        row.status === 'waiting' &&
        row.phase === 'Queued on the receiving ISM',
    );
    const hasIncomingQueue = data.activity.some(
      (row) =>
        row.task_key.startsWith(`transfer-queue:${id}:`) &&
        row.phase.includes('peer to come online'),
    );
    if (hasOutgoingQueue && !hasIncomingQueue) {
      items.push(
        item(
          peer.task_key,
          `Sync is waiting for ${peer.resource}`,
          'Reconnect the other instance to check its queued transfers.',
          '/peers',
          'Open peers',
        ),
      );
    }
  }
  for (const mapping of data.mappings) {
    if (!mapping.available) {
      items.push(
        item(
          `mapping:${mapping.root_id}:${mapping.availability_detail ?? ''}`,
          `${mapping.name} folder is unavailable`,
          'Reconnect or remount the folder to resume file changes.',
          '/mappings',
          'Open folder mappings',
        ),
      );
    }
  }
  for (const op of data.operations) {
    const isBlocking = op.status === 'recovery_required' && !op.recovery_resolved;
    if (!isBlocking && op.handled_at !== null) continue;
    items.push({
      ...item(
        `operation:${op.operation_id}:${op.status}:${op.finished_at ?? op.created_at}`,
        `${op.label ?? op.title} needs attention`,
        isBlocking
          ? 'Review recovery options before more work can continue.'
          : 'Open the operation to review what happened.',
        '/operations',
        'Open operations',
        isBlocking,
      ),
      notifiable: op.handled_at === null,
    });
  }
  const recovery = new Map(
    data.recovery.filter((r) => r.target.kind === 'conflict').map((r) => [r.target.id, r]),
  );
  const choices = data.conflicts.filter((conflict) => {
    const state = recovery.get(conflict.id)?.state;
    return conflict.status === 'open' && (!state || state === 'needs_decision');
  });
  if (choices.length) {
    items.push(
      item(
        `conflicts:${choices
          .map((c) => c.id)
          .sort()
          .join(',')}`,
        `${choices.length} ${choices.length === 1 ? 'file needs' : 'files need'} a choice`,
        'Choose which version to keep. Other files can continue syncing.',
        '/conflicts',
        'Open conflicts',
      ),
    );
  }
  for (const row of data.recovery) {
    if (row.state !== 'not_retryable') continue;
    const href = row.target.kind === 'conflict' ? '/conflicts' : '/sync';
    items.push(
      item(
        `recovery:${row.target.kind}:${row.target.peer_id ?? ''}:${row.target.id}:${row.state}:${row.code ?? ''}`,
        'A sync issue needs attention',
        'ISM cannot fix this automatically. Review the available repair options.',
        href,
        href === '/conflicts' ? 'Open conflicts' : 'Open Sync',
      ),
    );
  }
  return items;
}

async function loadAttention(epoch: number) {
  const [overview, activity, attention, recoveryOperations, conflicts, recovery, mappings] =
    await Promise.all([
      admin.overview(),
      admin.backgroundActivity(1),
      admin.operationPage('attention', 5000, 0),
      admin.operationPage('recovery_required', 5000, 0),
      admin.conflicts(),
      admin.recoveryPage(5000, 0),
      admin.mappings(),
    ]);
  if (!running || epoch !== generation) return;
  const operations = [
    ...new Map(
      [...attention.rows, ...recoveryOperations.rows].map((row) => [row.operation_id, row]),
    ).values(),
  ];
  const items = buildAttention({
    overview,
    activity: activity.current,
    operations,
    conflicts,
    recovery: recovery.rows,
    mappings,
  });
  // Keep old dismissals when a paginated source cannot provide a complete snapshot.
  const complete =
    attention.total <= attention.rows.length &&
    recoveryOperations.total <= recoveryOperations.rows.length &&
    recovery.total <= recovery.rows.length &&
    !(conflicts.length === 500 && overview.conflicts_open > conflicts.length);
  if (complete) {
    const ids = new Set(items.map((entry) => entry.id));
    dismissed = new Set([...dismissed].filter((id) => ids.has(id)));
    saveDismissed();
  }
  blockers.value = items.filter((entry) => entry.blocking);
  notifications.value = items.filter(
    (entry) => entry.notifiable !== false && !dismissed.has(entry.id),
  );
  attentionError.value = !complete || activity.warnings.length > 0;
  window.clearTimeout(transitionTimer);
  const next = activity.current
    .map((row) => row.next_at && Date.parse(row.next_at))
    .filter((at): at is number => typeof at === 'number' && Number.isFinite(at) && at > Date.now())
    .sort((a, b) => a - b)[0];
  if (next !== undefined)
    transitionTimer = window.setTimeout(
      () => void refreshAttention(),
      Math.min(2_147_483_647, Math.max(1000, next - Date.now() + 100)),
    );
}

export function refreshAttention(): Promise<void> {
  if (!running) return Promise.resolve();
  if (pending) {
    refreshAgain = true;
    return pending;
  }
  const epoch = generation;
  pending = (async () => {
    do {
      refreshAgain = false;
      lastRefreshAt = Date.now();
      try {
        await loadAttention(epoch);
      } catch (error) {
        if (epoch === generation && running) {
          attentionError.value = true;
          console.error('[attention] could not refresh attention state', error);
        }
      }
    } while (refreshAgain && epoch === generation && running);
  })().finally(() => {
    pending = undefined;
    if (running && epoch !== generation) void refreshAttention();
  });
  return pending;
}

export function startAttention(): () => void {
  running = true;
  generation++;
  dismissed = readDismissed();
  const stop = events.on(() => {
    if (timer !== undefined) return;
    timer = window.setTimeout(
      () => {
        timer = undefined;
        void refreshAttention();
      },
      Math.max(500, 1500 - (Date.now() - lastRefreshAt)),
    );
  });
  void refreshAttention();
  return () => {
    running = false;
    generation++;
    stop();
    window.clearTimeout(timer);
    window.clearTimeout(transitionTimer);
    timer = undefined;
    transitionTimer = undefined;
    blockers.value = [];
    notifications.value = [];
    attentionError.value = false;
  };
}

export function dismissNotification(id: string) {
  dismissed.add(id);
  saveDismissed();
  notifications.value = notifications.value.filter((entry) => entry.id !== id);
}

export function dismissAllNotifications() {
  for (const entry of notifications.value) dismissed.add(entry.id);
  saveDismissed();
  notifications.value = [];
}
