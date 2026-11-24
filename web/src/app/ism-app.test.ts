import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import { events } from '@/api/events';
import type { BackgroundActivity } from '@/api/generated/BackgroundActivity';
import type { SessionInfo } from '@/api/generated/SessionInfo';
import type { AppRoute } from '@/app/routes';
import { blockers } from '@/stores/attention';
import { session } from '@/stores/session';
import { mount } from '@/test/helpers';
import '@/app/ism-app';

const until = (check: () => unknown) => vi.waitFor(check, { interval: 1, timeout: 2000 });

const BASE: SessionInfo = {
  needs_setup: false,
  authenticated: false,
  username: null,
  instance_id: 'instance-a',
  instance_name: 'Instance A',
  version: '0.1.0',
};

const STUB_ROUTES: readonly AppRoute[] = [
  {
    path: '/overview',
    tag: 'ism-route-placeholder',
    load: () => import('@/components/ism-route-placeholder'),
    label: 'Overview',
    icon: 'gauge',
    menu: 'tools',
    props: { message: 'stub overview' },
  },
];

function serveSession(info: SessionInfo) {
  vi.spyOn(api, 'get').mockImplementation(async (path: string) => {
    if (path === '/api/admin/session') return info as never;
    throw new Error(`unexpected GET ${path}`);
  });
}

describe('ism-app', () => {
  beforeEach(() => {
    session.value = undefined;
    vi.spyOn(events, 'start').mockImplementation(() => undefined);
    vi.spyOn(events, 'stop').mockImplementation(() => undefined);
    window.history.replaceState(null, '', '/overview');
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('asks for the first administrator on a new instance', async () => {
    serveSession({ ...BASE, needs_setup: true });
    const app = await mount('ism-app', { routes: STUB_ROUTES });
    await until(() => expect(app.querySelector('ism-login-page')).not.toBeNull());
    await app.querySelector('ism-login-page')?.updateComplete;
    expect(app.querySelector('input[name="instance_name"]')).not.toBeNull();
    expect(app.querySelector('nav')).toBeNull();
    expect(events.start).not.toHaveBeenCalled();
  });

  it('shows sign-in when signed out', async () => {
    serveSession(BASE);
    const app = await mount('ism-app', { routes: STUB_ROUTES });
    await until(() => expect(app.textContent).toContain('Sign in to continue'));
    expect(app.querySelector('input[name="instance_name"]')).toBeNull();
  });

  it('renders the menu and page once signed in, and starts live events', async () => {
    vi.spyOn(api, 'get').mockImplementation(async (path: string) => {
      if (path === '/api/admin/session')
        return { ...BASE, authenticated: true, username: 'admin' } as never;
      if (path === '/api/admin/attention')
        return { completeness: 'complete', warnings: [], items: [] } as never;
      if (path === '/api/admin/overview') return undefined as never;
      throw new Error(`unexpected GET ${path}`);
    });
    const app = await mount('ism-app', { routes: STUB_ROUTES });
    await until(() => expect(app.textContent).toContain('stub overview'));
    expect(app.querySelector('nav a[aria-current="page"]')?.textContent).toContain('Overview');
    expect(app.textContent).toContain('Instance A');
    expect(events.start).toHaveBeenCalledOnce();
  });

  it('shows active blockers on the three work pages only while signed in', async () => {
    const routes: AppRoute[] = ['/overview', '/operations', '/sync', '/settings'].map((path) => ({
      path,
      tag: 'ism-route-placeholder',
      load: () => import('@/components/ism-route-placeholder'),
      label: path,
      icon: 'gauge',
    }));
    vi.spyOn(api, 'get').mockImplementation(async (path: string) => {
      if (path === '/api/admin/session')
        return { ...BASE, authenticated: true, username: 'admin' } as never;
      if (path === '/api/admin/overview')
        return {
          instance_name: 'Instance A',
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
        } as never;
      if (path === '/api/admin/operations/background')
        return { current: [], history: [], total: 0, observed_at: '', warnings: [] } as never;
      if (path === '/api/admin/operations/page' || path === '/api/admin/recovery/page')
        return { rows: [], total: 0 } as never;
      if (path === '/api/admin/conflicts' || path === '/api/admin/mappings') return [] as never;
      throw new Error(`unexpected GET ${path}`);
    });
    const app = await mount('ism-app', { routes });
    await until(() => expect(app.querySelector('ism-attention-bell')).not.toBeNull());
    await vi.waitFor(() => expect(api.get).toHaveBeenCalledWith('/api/admin/attention'));
    blockers.value = [
      {
        id: 'peer:example',
        title: 'Sync is waiting',
        detail: 'Reconnect the other instance.',
        href: '/peers',
        action: 'Open peers',
        blocking: true,
        notifiable: true,
      },
    ];
    for (const path of ['/overview', '/operations', '/sync']) {
      window.history.pushState(null, '', path);
      window.dispatchEvent(new PopStateEvent('popstate'));
      await until(() => expect(app.querySelector('ism-blocker-banners')).not.toBeNull());
      await app.querySelector('ism-blocker-banners')?.updateComplete;
      expect(app.querySelector('ism-blocker-banners')?.textContent).toContain('Sync is waiting');
    }
    window.history.pushState(null, '', '/settings');
    window.dispatchEvent(new PopStateEvent('popstate'));
    await until(() => expect(app.querySelector('ism-blocker-banners')).toBeNull());
  });

  it('opens sync issues from an activity link through the app router', async () => {
    const activity: BackgroundActivity = {
      id: 'blocked-transfer',
      task_key: 'transfer:example',
      title: 'Peer file transfer queue',
      resource: 'Example share',
      status: 'blocked',
      phase: 'peer publication waits for a dependent inbox effect',
      done: 0,
      total: null,
      unit: 'files',
      started_at: '2026-01-01T00:00:00Z',
      updated_at: '2026-01-01T00:00:00Z',
      finished_at: null,
      next_at: null,
      error: null,
      operation_id: null,
      page: '/sync',
      count: 1,
      reasons: [],
      examples: [],
    };
    vi.spyOn(api, 'get').mockImplementation(async (path: string) => {
      if (path === '/api/admin/session')
        return { ...BASE, authenticated: true, username: 'admin' } as never;
      if (path === '/api/admin/overview') return undefined as never;
      if (path === '/api/admin/operations/background')
        return {
          current: [activity],
          history: [],
          total: 0,
          observed_at: new Date().toISOString(),
          warnings: [],
        } as never;
      if (path === '/api/admin/sync') return { allowed_now: true, override_active: null } as never;
      if (path === '/api/admin/sync/schedule') return { timezone: 'UTC', windows: [] } as never;
      if (path.endsWith('/page')) return { rows: [], total: 0 } as never;
      if (
        path === '/api/admin/sync/overrides' ||
        path === '/api/admin/shares' ||
        path === '/api/admin/peers'
      )
        return [] as never;
      throw new Error(`unexpected GET ${path}`);
    });
    window.history.replaceState(null, '', '/operations');
    const app = await mount('ism-app');
    await until(() => expect(app.querySelector('a[href="/sync?view=attention"]')).not.toBeNull());
    app.querySelector<HTMLAnchorElement>('a[href="/sync?view=attention"]')?.click();
    await until(() =>
      expect(app.querySelector('#sync-tab-attention')?.getAttribute('aria-selected')).toBe('true'),
    );
    expect(window.location.pathname + window.location.search).toBe('/sync?view=attention');
    expect(app.querySelector('ism-recovery-list')).not.toBeNull();
  });
});
