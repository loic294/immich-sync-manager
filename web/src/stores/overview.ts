import { signal } from '@preact/signals-core';
import { admin, createAdmin } from '@/api/admin';
import { isAbortError } from '@/api/errors';
import type { Overview } from '@/api/generated/Overview';
import { LiveScope } from '@/api/live';

/** The dashboard counters; the menu uses them for its attention dots. */
export const overview = signal<Overview | undefined>(undefined);
export const overviewError = signal<string | undefined>(undefined);

let pending: Promise<void> | undefined;
let scope: LiveScope | undefined;
let generation = 0;
let refreshAgain = false;

export function refreshOverview(): Promise<void> {
  if (pending) {
    refreshAgain = true;
    return pending;
  }
  refreshAgain = false;
  const epoch = generation;
  pending ??= (scope ? createAdmin(scope) : admin)
    .overview()
    .then((value) => {
      overview.value = value;
      overviewError.value = undefined;
    })
    .catch((error: unknown) => {
      if (isAbortError(error)) return;
      overviewError.value = error instanceof Error ? error.message : String(error);
      console.error('[overview] could not load live overview', error);
    })
    .finally(() => {
      pending = undefined;
      if (scope && (epoch !== generation || refreshAgain)) void refreshOverview();
    });
  return pending;
}

/** Subscribes to pushed dashboard counters, shared by the dashboard and navigation. */
export function startOverview(): () => void {
  generation++;
  scope?.dispose();
  const current = new LiveScope(() => void refreshOverview());
  scope = current;
  void refreshOverview();
  return () => {
    generation++;
    current.dispose();
    if (scope === current) scope = undefined;
    overview.value = undefined;
    overviewError.value = undefined;
  };
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
