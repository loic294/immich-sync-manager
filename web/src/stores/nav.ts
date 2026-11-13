import { type Signal, signal } from '@preact/signals-core';

/** `/a/b/` and `/a/b` are the same page; `` is `/`. */
export function normalisePath(pathname: string): string {
  const trimmed = pathname.replace(/\/+$/, '');
  return trimmed === '' ? '/' : trimmed;
}

/** The current page path, kept in step with the address bar by the router. */
export const currentPath: Signal<string> = signal(normalisePath(window.location.pathname));

/** The href of an in-app page. */
export function linkTo(path: string): string {
  return path;
}

/** Navigates without reloading; the router renders the page. */
export function navigate(path: string, options: { replace?: boolean } = {}): void {
  if (options.replace) window.history.replaceState(null, '', path);
  else window.history.pushState(null, '', path);
  window.dispatchEvent(new PopStateEvent('popstate'));
}

/** Re-reads the address bar into `currentPath` (the router calls it on every navigation). */
export function syncFromLocation(): void {
  currentPath.value = normalisePath(window.location.pathname);
}

/** Keeps `currentPath` in step with Back/Forward. Returns the stop function. */
export function startNavSync(): () => void {
  window.addEventListener('popstate', syncFromLocation);
  syncFromLocation();
  return () => window.removeEventListener('popstate', syncFromLocation);
}
