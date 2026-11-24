import { signal } from '@preact/signals-core';
import { admin } from '@/api/admin';
import { events } from '@/api/events';
import type { AttentionCode } from '@/api/generated/AttentionCode';
import type { AttentionOwner } from '@/api/generated/AttentionOwner';
import type { AttentionSnapshot } from '@/api/generated/AttentionSnapshot';

const DISMISSED_KEY = 'ism.dismissed-attention.v2';
const LEGACY_DISMISSED_KEY = 'ism.dismissed-attention.v1';

export interface AttentionItem {
  id: string;
  code: AttentionCode;
  owner: AttentionOwner;
  peerId: string | null;
  title: string;
  detail: string;
  href: string;
  action: string;
  external: boolean;
  blocking: boolean;
  notifiable: boolean;
  stale?: boolean;
}

const ACTIONS: Record<string, string> = {
  '/overview': 'Open overview',
  '/operations': 'Open operations',
  '/sync': 'Open Sync',
  '/servers': 'Open servers',
  '/mappings': 'Open folder mappings',
  '/conflicts': 'Open conflicts',
  '/peers': 'Open peers',
};

export const blockers = signal<AttentionItem[]>([]);
export const notifications = signal<AttentionItem[]>([]);
export const attentionError = signal(false);

type Source = { code: string; owner: string; peerId?: string | null };
let dismissed = new Map<string, Source | null>();
let timer: number | undefined;
let transitionTimer: number | undefined;
let pending: Promise<void> | undefined;
let refreshAgain = false;
let generation = 0;
let running = false;
let lastRefreshAt = 0;

function readDismissed(): Map<string, Source | null> {
  try {
    const stored: unknown = JSON.parse(
      localStorage.getItem(DISMISSED_KEY) ?? localStorage.getItem(LEGACY_DISMISSED_KEY) ?? '[]',
    );
    const result = new Map<string, Source | null>();
    if (!Array.isArray(stored)) return result;
    for (const entry of stored) {
      if (typeof entry === 'string') result.set(entry, null);
      else if (entry && typeof entry === 'object' && typeof entry.id === 'string') {
        const source =
          typeof entry.code === 'string' && typeof entry.owner === 'string'
            ? {
                code: entry.code,
                owner: entry.owner,
                ...(typeof entry.peerId === 'string' || entry.peerId === null
                  ? { peerId: entry.peerId }
                  : {}),
              }
            : null;
        result.set(entry.id, source);
      }
    }
    return result;
  } catch (error) {
    console.warn('[attention] could not read dismissed notifications', error);
    return new Map();
  }
}

function saveDismissed() {
  try {
    localStorage.setItem(
      DISMISSED_KEY,
      JSON.stringify([...dismissed].map(([id, source]) => ({ id, ...source }))),
    );
  } catch (error) {
    console.warn('[attention] could not save dismissed notifications', error);
  }
}

export function buildAttention(snapshot: AttentionSnapshot): AttentionItem[] {
  return snapshot.items
    .filter((entry) => entry.attention !== 'none')
    .map((entry) => {
      const url = new URL(entry.href, window.location.href);
      return {
        id: entry.id,
        code: entry.code,
        owner: entry.owner,
        peerId: entry.peer_id,
        title: entry.title,
        detail: entry.message,
        href: entry.href,
        action: ACTIONS[url.pathname] ?? 'Open details',
        external: url.origin !== window.location.origin,
        blocking: entry.blocks_work,
        notifiable: entry.attention !== 'none' && entry.acknowledged !== true,
      };
    });
}

async function loadAttention(epoch: number) {
  const snapshot = await admin.attention();
  if (!running || epoch !== generation) return;
  const items = buildAttention(snapshot);
  const complete = snapshot.completeness === 'complete' && snapshot.warnings.length === 0;
  const observed = new Map(snapshot.items.map((entry) => [entry.id, entry]));
  const coverage = snapshot.coverage;
  const cleared = (id: string, source: Source | null) => {
    const current = observed.get(id);
    if (current) return current.completeness === 'complete' && current.attention === 'none';
    return (
      complete ||
      (source !== null &&
        coverage.some(
          (entry) =>
            entry.completeness === 'complete' &&
            entry.code === source.code &&
            entry.owner === source.owner &&
            (entry.peer_id == null || entry.peer_id === source.peerId),
        ))
    );
  };
  for (const [id, source] of dismissed) {
    if (cleared(id, source)) dismissed.delete(id);
    else {
      const current = observed.get(id);
      if (current && current.attention !== 'none')
        dismissed.set(id, { code: current.code, owner: current.owner, peerId: current.peer_id });
    }
  }
  saveDismissed();
  const seen = new Set(items.map((entry) => entry.id));
  const retain = (entries: AttentionItem[]) =>
    entries
      .filter((entry) => !seen.has(entry.id) && !cleared(entry.id, entry))
      .map((entry) => ({ ...entry, stale: true }));
  blockers.value = [...items.filter((entry) => entry.blocking), ...retain(blockers.value)];
  notifications.value = [
    ...items.filter((entry) => entry.notifiable && !dismissed.has(entry.id)),
    ...retain(notifications.value).filter((entry) => !dismissed.has(entry.id)),
  ];
  attentionError.value =
    snapshot.warnings.length > 0 ||
    snapshot.completeness === 'unavailable' ||
    coverage.some((entry) => entry.completeness === 'unavailable');
  window.clearTimeout(transitionTimer);
  const next = snapshot.items
    .map((entry) => {
      const when = entry.recheck_at ?? entry.next_at;
      return when && Date.parse(when);
    })
    .filter((at): at is number => typeof at === 'number' && Number.isFinite(at) && at > Date.now())
    .sort((a, b) => a - b)[0];
  const delay = next === undefined ? undefined : Math.max(1000, next - Date.now() + 100);
  const recheck = complete ? delay : Math.min(delay ?? 60_000, 60_000);
  if (recheck !== undefined)
    transitionTimer = window.setTimeout(
      () => void refreshAttention(),
      Math.min(2_147_483_647, recheck),
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
          blockers.value = blockers.value.map((entry) => ({ ...entry, stale: true }));
          notifications.value = notifications.value.map((entry) => ({ ...entry, stale: true }));
          console.error('[attention] could not refresh attention state', error);
          window.clearTimeout(transitionTimer);
          transitionTimer = window.setTimeout(() => void refreshAttention(), 60_000);
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
  const entry = notifications.value.find((item) => item.id === id);
  if (!entry) return;
  dismissed.set(id, { code: entry.code, owner: entry.owner, peerId: entry.peerId });
  saveDismissed();
  notifications.value = notifications.value.filter((entry) => entry.id !== id);
}

export function dismissAllNotifications() {
  for (const entry of notifications.value)
    dismissed.set(entry.id, { code: entry.code, owner: entry.owner, peerId: entry.peerId });
  saveDismissed();
  notifications.value = [];
}
