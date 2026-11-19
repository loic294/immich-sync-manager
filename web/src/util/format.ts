const UNITS = ['B', 'KB', 'MB', 'GB', 'TB'] as const;

/** `1536` -> `1.5 KB`. */
export function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes < 0) return '—';
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < UNITS.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${unit === 0 ? value : value.toFixed(value < 10 ? 1 : 0)} ${UNITS[unit]}`;
}

export function formatDuration(seconds: number): string {
  const value = Math.max(0, Math.ceil(seconds));
  if (value < 60) return `${value} sec`;
  const minutes = Math.ceil(value / 60);
  if (minutes < 60) return `${minutes} min`;
  if (minutes < 1440) return `${Math.floor(minutes / 60)} hr ${minutes % 60} min`;
  const days = Math.floor(minutes / 1440);
  return `${days} ${days === 1 ? 'day' : 'days'} ${Math.floor((minutes % 1440) / 60)} hr`;
}

/** A timestamp in the browser's locale, or `—`. */
export function formatDate(value: string | null | undefined): string {
  if (!value) return '—';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

/** `photos:2024/Trip/x.jpg` style label for a location. */
export function formatLocation(location: { root_id: string; path: string }): string {
  return location.path ? `${location.root_id}:${location.path}` : `${location.root_id}:/`;
}

/** `snake_case` -> `Snake case`. */
export function humanize(value: string): string {
  const text = value.replaceAll('_', ' ');
  return text.charAt(0).toUpperCase() + text.slice(1);
}

/** Percent of `done` in `total`, 0-100. */
export function percent(done: number, total: number): number {
  if (total <= 0) return 0;
  return Math.min(100, Math.round((done / total) * 100));
}

/** Seconds until `dueAt` as `1:05`, `0:00` once due, or `—`. */
export function formatCountdown(dueAt: string | null | undefined, now = Date.now()): string {
  if (!dueAt) return '—';
  const due = new Date(dueAt).getTime();
  if (Number.isNaN(due)) return '—';
  const seconds = Math.max(0, Math.ceil((due - now) / 1000));
  return `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, '0')}`;
}
