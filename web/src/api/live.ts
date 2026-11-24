import { type ApiClient, api, type GetOptions, withQuery } from '@/api/client';
import { ApiError, parseErrorBody } from '@/api/errors';
import { events, type Snapshot } from '@/api/events';

const LIVE_PATHS = new Set([
  '/overview',
  '/attention',
  '/settings',
  '/servers',
  '/accounts',
  '/libraries',
  '/mappings',
  '/trash',
  '/peers',
  '/shares',
  '/sync',
  '/sync/schedule',
  '/sync/overrides',
  '/conflicts/page',
  '/operations/page',
  '/operations/background',
  '/sync/page',
  '/recovery/page',
  '/recovery/versions',
]);

interface Entry {
  value?: unknown;
  error?: Error;
  loaded: boolean;
  pending: Promise<unknown>;
  stop: () => void;
  cancel: () => void;
}

/** One view's subscriptions. Live renders reuse pushed values; explicit reloads start a new scope. */
export class LiveScope implements Pick<ApiClient, 'get'> {
  private readonly entries = new Map<string, Entry>();

  constructor(private readonly changed: () => void) {}

  get<T>(path: string, options?: GetOptions): Promise<T> {
    const relative = path.startsWith('/api/admin/') ? path.slice('/api/admin'.length) : '';
    const subscribable = LIVE_PATHS.has(relative) || /^\/operations\/[^/]+\/detail$/.test(relative);
    const url = withQuery(subscribable ? relative : path, options?.query);
    return this.read<T>(url, subscribable, () => api.get<T>(path, options));
  }

  snapshot<T>(url: string, initial: () => Promise<T>, body: object): Promise<T> {
    return this.read(url, true, initial, body);
  }

  private read<T>(
    url: string,
    subscribable: boolean,
    initial: () => Promise<T>,
    body?: object,
  ): Promise<T> {
    const key = body ? `${url}\n${JSON.stringify(body)}` : url;
    const existing = this.entries.get(key);
    if (existing) {
      if (existing.error) return Promise.reject(existing.error);
      return existing.loaded
        ? Promise.resolve(existing.value as T)
        : (existing.pending as Promise<T>);
    }

    let resolve!: (value: unknown) => void;
    let reject!: (error: Error) => void;
    const pending = new Promise<unknown>((done, fail) => {
      resolve = done;
      reject = fail;
    });
    const entry: Entry = {
      loaded: false,
      pending,
      stop: () => {},
      cancel: () => reject(new DOMException('Live view closed.', 'AbortError')),
    };
    this.entries.set(key, entry);
    let settled = false;
    let pushed = false;
    let deadline: number | undefined;
    const cancel = entry.cancel;
    entry.cancel = () => {
      window.clearTimeout(deadline);
      cancel();
    };
    const accept = (snapshot: Snapshot | Error) => {
      if (this.entries.get(key) !== entry) return;
      window.clearTimeout(deadline);
      const loaded = settled;
      settled = true;
      if (snapshot instanceof Error || snapshot.status < 200 || snapshot.status >= 300) {
        const body = snapshot instanceof Error ? undefined : parseErrorBody(snapshot.data);
        entry.error =
          snapshot instanceof Error
            ? snapshot
            : new ApiError({
                status: snapshot.status,
                code: body?.code ?? 'live_snapshot',
                message: body?.message ?? 'Could not load live data.',
              });
        reject(entry.error);
      } else {
        entry.loaded = true;
        entry.error = undefined;
        entry.value = snapshot.data;
        resolve(snapshot.data);
      }
      if (loaded) this.changed();
    };
    if (subscribable) {
      if (events.running)
        deadline = window.setTimeout(
          () =>
            accept(
              new ApiError({
                status: 0,
                code: 'live_timeout',
                message:
                  'The WebSocket did not deliver live data. Check the server and reverse proxy; no polling fallback is used.',
              }),
            ),
          30_000,
        );
      entry.stop = events.subscribe(
        url,
        (snapshot) => {
          pushed = true;
          accept(snapshot);
        },
        body,
      );
    }
    // The shell starts live updates before mounting views. Standalone/initial reads can use HTTP,
    // but a failed live connection never falls back to repeated HTTP requests.
    if (!subscribable || !events.running) {
      void initial().then(
        (data) => {
          if (!pushed) accept({ status: 200, data });
        },
        (error: unknown) => {
          if (!pushed) accept(error instanceof Error ? error : new Error(String(error)));
        },
      );
    }
    return pending as Promise<T>;
  }

  dispose() {
    for (const entry of this.entries.values()) {
      entry.stop();
      entry.cancel();
    }
    this.entries.clear();
  }
}
