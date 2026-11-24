import { signal } from '@preact/signals-core';
import { admin } from '@/api/admin';
import { events } from '@/api/events';
import type { AttentionSnapshot } from '@/api/generated/AttentionSnapshot';

const DISMISSED_KEY = 'ism.dismissed-attention.v1';

export interface AttentionItem {
  id: string;
  title: string;
  detail: string;
  href: string;
  action: string;
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

export function buildAttention(snapshot: AttentionSnapshot): AttentionItem[] {
  return snapshot.items
    .filter((entry) => entry.attention !== 'none')
    .map((entry) => {
      const path = entry.href.split('?')[0];
      return {
        id: entry.id,
        title: entry.title,
        detail: entry.message,
        href: entry.href,
        action: ACTIONS[path] ?? 'Open details',
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
  if (complete) {
    const ids = new Set(items.map((entry) => entry.id));
    dismissed = new Set([...dismissed].filter((id) => ids.has(id)));
    saveDismissed();
  }
  const seen = new Set(snapshot.items.map((entry) => entry.id));
  const retain = (entries: AttentionItem[]) =>
    complete
      ? []
      : entries.filter((entry) => !seen.has(entry.id)).map((entry) => ({ ...entry, stale: true }));
  blockers.value = [...items.filter((entry) => entry.blocking), ...retain(blockers.value)];
  notifications.value = [
    ...items.filter((entry) => entry.notifiable && !dismissed.has(entry.id)),
    ...retain(notifications.value).filter((entry) => !dismissed.has(entry.id)),
  ];
  attentionError.value = !complete;
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
  dismissed.add(id);
  saveDismissed();
  notifications.value = notifications.value.filter((entry) => entry.id !== id);
}

export function dismissAllNotifications() {
  for (const entry of notifications.value) dismissed.add(entry.id);
  saveDismissed();
  notifications.value = [];
}
