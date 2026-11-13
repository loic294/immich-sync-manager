import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '@/api/client';
import { events } from '@/api/events';
import type { SessionInfo } from '@/api/generated/SessionInfo';
import type { AppRoute } from '@/app/routes';
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
      if (path === '/api/admin/overview') return undefined as never;
      throw new Error(`unexpected GET ${path}`);
    });
    const app = await mount('ism-app', { routes: STUB_ROUTES });
    await until(() => expect(app.textContent).toContain('stub overview'));
    expect(app.querySelector('nav a[aria-current="page"]')?.textContent).toContain('Overview');
    expect(app.textContent).toContain('Instance A');
    expect(events.start).toHaveBeenCalledOnce();
  });
});
