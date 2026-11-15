import { signal } from '@preact/signals-core';
import { admin } from '@/api/admin';
import { events } from '@/api/events';
import type { Overview } from '@/api/generated/Overview';

/** The dashboard counters; the menu uses them for its attention dots. */
export const overview = signal<Overview | undefined>(undefined);

let pending: Promise<void> | undefined;

export function refreshOverview(): Promise<void> {
  pending ??= admin
    .overview()
    .then((value) => {
      overview.value = value;
    })
    .catch(() => undefined)
    .finally(() => {
      pending = undefined;
    });
  return pending;
}

/** Loads the overview and refreshes it on every change event. Returns the stop function. */
export function startOverview(): () => void {
  void refreshOverview();
  return events.on(() => void refreshOverview());
}

export function serversNeedAttention(): boolean {
  const value = overview.value;
  if (!value) return false;
  return (
    value.uncovered_libraries > 0 ||
    value.servers.some(
      (server) =>
        !server.compatible ||
        server.maintenance.drift !== null ||
        server.maintenance.phase === 'blocked' ||
        server.maintenance.phase === 'recovery_required',
    )
  );
}
