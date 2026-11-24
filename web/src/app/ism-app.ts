import { effect } from '@preact/signals-core';
import { html, nothing } from 'lit';
import { classMap } from 'lit/directives/class-map.js';
import { live } from 'lit/directives/live.js';
import { createRef, ref } from 'lit/directives/ref.js';
import { events } from '@/api/events';
import { type AppRouter, createAppRouter } from '@/app/router';
import { type AppRoute, routes as appRoutes, notFound, redirects, routeAt } from '@/app/routes';
import { startTheme } from '@/app/theme';
import { IsmElement } from '@/base/ism-element';
import { attentionError, blockers, startAttention } from '@/stores/attention';
import { currentPath, linkTo, startNavSync } from '@/stores/nav';
import { startOverview } from '@/stores/overview';
import { logout, refreshSession, session, sessionError } from '@/stores/session';
import '@/app/ism-theme-toggle';
import '@/components/ism-attention-bell';
import '@/components/ism-blocker-banners';
import '@/components/ism-icon';
import '@/components/ism-toast-host';
import '@/pages/ism-login-page';

const APP_NAME = 'Immich Sync Manager';
const TOGGLE_ID = 'ism-menu-toggle';
const MENU_ID = 'ism-menu';
const FOCUS_DELAY_MS = 150;

/**
 * The application shell, the one element in `index.html`.
 *
 * Until the administrator is signed in it shows `<ism-login-page>` (first-run setup or sign-in).
 * Once signed in it renders the drawer layout: the menu built from `app/routes.ts`, a top bar with
 * the page heading, the instance name, the theme toggle and Sign out, the connection banner and the
 * router outlet. It owns the theme, nav sync, overview store and the live-events connection.
 */
export class IsmApp extends IsmElement {
  static override properties = {
    routes: { attribute: false },
    menuOpen: { state: true },
  };

  declare routes: readonly AppRoute[];
  declare menuOpen: boolean;

  private router: AppRouter | undefined;
  private stopServices: (() => void)[] = [];
  private stopSignedIn: (() => void)[] = [];
  private focusTimer: number | undefined;
  private menuButton = createRef<HTMLButtonElement>();

  constructor() {
    super();
    this.routes = appRoutes;
    this.menuOpen = false;
  }

  override connectedCallback() {
    super.connectedCallback();
    this.classList.add('block');
    this.stopServices = [
      startTheme(),
      startNavSync(),
      effect(() => {
        document.title = `${this.pageFor(currentPath.value).label} · ${APP_NAME}`;
      }),
      effect(() => {
        if (session.value?.authenticated) this.signedIn();
        else this.signedOut();
      }),
    ];
    this.router = createAppRouter(this, { routes: this.routes });
    void refreshSession();
  }

  override disconnectedCallback() {
    super.disconnectedCallback();
    this.router?.dispose();
    this.router = undefined;
    this.signedOut();
    for (const stop of this.stopServices.splice(0)) stop();
    window.clearTimeout(this.focusTimer);
  }

  private signedIn() {
    if (this.stopSignedIn.length > 0) return;
    events.start();
    this.stopSignedIn = [startOverview(), startAttention(), () => events.stop()];
  }

  private signedOut() {
    for (const stop of this.stopSignedIn.splice(0)) stop();
  }

  private pageFor(path: string): AppRoute {
    return routeAt(path, this.routes) ?? routeAt(redirects[path] ?? '', this.routes) ?? notFound;
  }

  private toggleMenu() {
    if (this.menuOpen) {
      this.closeMenu();
      return;
    }
    this.menuOpen = true;
    window.clearTimeout(this.focusTimer);
    this.focusTimer = window.setTimeout(() => {
      if (!this.menuOpen) return;
      const menu = this.querySelector(`#${MENU_ID}`);
      const target =
        menu?.querySelector<HTMLElement>('a[aria-current="page"]') ??
        menu?.querySelector<HTMLElement>('a');
      target?.focus();
    }, FOCUS_DELAY_MS);
  }

  private closeMenu() {
    if (!this.menuOpen) return;
    this.menuOpen = false;
    this.menuButton.value?.focus();
  }

  private onToggleChange(event: Event) {
    if ((event.target as HTMLInputElement).checked) this.menuOpen = true;
    else this.closeMenu();
  }

  private onKeydown(event: KeyboardEvent) {
    if (event.key === 'Escape' && !event.defaultPrevented) this.closeMenu();
  }

  private onMenuClick(event: Event) {
    if (event.target instanceof Element && event.target.closest('a[href], button')) {
      this.closeMenu();
    }
  }

  private renderLink(route: AppRoute) {
    const active = currentPath.value === route.path;
    return html`
      <li>
        <a
          href=${linkTo(route.path)}
          class=${classMap({ 'menu-active': active, 'transition-shadow': true })}
          aria-current=${active ? 'page' : nothing}
        >
          <ism-icon name=${route.icon} size="sm"></ism-icon>
          ${route.label}
          ${route.attention?.() ? html`<span class="status status-warning" role="img" aria-label="Needs attention"></span>` : nothing}
        </a>
      </li>
    `;
  }

  private renderConnectionBanner() {
    const state = events.connection.value;
    if (state !== 'reconnecting') return nothing;
    return html`
      <div role="alert" class="alert alert-warning flex-none rounded-none">
        <ism-icon name="wifi-off"></ism-icon>
        <div>
          <p>Live updates unavailable. Reconnecting… Displayed data may be stale; no polling fallback is used.</p>
          <p class="text-sm">Ensure the reverse proxy forwards WebSocket upgrades for /api/admin/live, preserves Host, and allows idle connections for more than 30 seconds. If the session expired, sign in again.</p>
        </div>
      </div>
    `;
  }

  private renderGate() {
    const info = session.value;
    if (!info) {
      return html`
        <div class="flex h-screen items-center justify-center p-6">
          ${
            sessionError.value
              ? html`<div role="alert" class="alert alert-error alert-soft max-w-xl">
                  <ism-icon name="circle-alert"></ism-icon>
                  <span>Cannot reach the server: ${sessionError.value}</span>
                  <button type="button" class="btn btn-sm" @click=${() => refreshSession()}>Retry</button>
                </div>`
              : html`<span class="loading loading-spinner loading-lg" aria-label="Loading"></span>`
          }
        </div>
      `;
    }
    return html`<ism-login-page .setup=${info.needs_setup}></ism-login-page>`;
  }

  protected override render() {
    if (!session.value?.authenticated) {
      return html`${this.renderGate()}<ism-toast-host></ism-toast-host>`;
    }
    const info = session.value;
    const open = this.menuOpen;
    return html`
      <div class="drawer lg:drawer-open h-screen w-screen grid-rows-1 overflow-hidden" @keydown=${this.onKeydown}>
        <input
          id=${TOGGLE_ID}
          type="checkbox"
          class="drawer-toggle"
          tabindex="-1"
          aria-hidden="true"
          .checked=${live(open)}
          @change=${this.onToggleChange}
        />
        <div class="drawer-content flex min-h-0 min-w-0 flex-col">
          <header class="navbar min-h-12 flex-none gap-2 border-b border-base-300 bg-base-100 px-2">
            <div class="flex min-w-0 flex-1 items-center gap-2">
              <button
                type="button"
                class="btn btn-ghost btn-sm btn-square lg:hidden"
                aria-label="Menu"
                aria-controls=${MENU_ID}
                aria-expanded=${open ? 'true' : 'false'}
                ${ref(this.menuButton)}
                @click=${this.toggleMenu}
              >
                <ism-icon name="menu"></ism-icon>
              </button>
              <h1 class="truncate text-lg font-semibold">${this.pageFor(currentPath.value).label}</h1>
            </div>
            <div class="flex flex-none items-center gap-2">
              <span class="badge badge-ghost hidden sm:inline-flex" title="This instance">
                <ism-icon name="server" size="xs"></ism-icon>${info.instance_name}
              </span>
              <ism-attention-bell></ism-attention-bell>
              <ism-theme-toggle></ism-theme-toggle>
              <div class="tooltip tooltip-bottom" data-tip=${`Sign out ${info.username ?? ''}`}>
                <button type="button" class="btn btn-ghost btn-sm btn-square" aria-label="Sign out" @click=${() => logout()}>
                  <ism-icon name="log-out" size="sm"></ism-icon>
                </button>
              </div>
            </div>
          </header>
          ${this.renderConnectionBanner()}
          <main class="flex min-h-0 flex-1 flex-col overflow-hidden">
            ${
              ['/overview', '/operations', '/sync'].includes(currentPath.value) &&
              (blockers.value.length > 0 || attentionError.value)
                ? html`<div class="mx-auto flex max-h-[40vh] w-full max-w-6xl flex-none flex-col gap-2 overflow-y-auto px-4 pt-4">
                    <ism-blocker-banners class="flex flex-col gap-2"></ism-blocker-banners>
                  </div>`
                : nothing
            }
            <div class="min-h-0 flex-1 overflow-hidden">${this.router?.outlet()}</div>
          </main>
        </div>
        <div class="drawer-side">
          <label for=${TOGGLE_ID} class="drawer-overlay" aria-label="Close menu"></label>
          <aside
            id=${MENU_ID}
            class="flex h-full w-64 flex-col border-r border-base-300 bg-base-200"
            @click=${this.onMenuClick}
          >
            <div class="flex flex-none items-center gap-2 px-4 py-3">
              <ism-icon name="folder-sync" size="lg" class="text-primary"></ism-icon>
              <span class="truncate text-sm font-semibold">${APP_NAME}</span>
            </div>
            <nav aria-label="Main" class="flex min-h-0 flex-1 flex-col">
              <ul class="menu min-h-0 w-full flex-1 flex-nowrap overflow-y-auto">
                ${this.routes.filter((route) => route.menu === 'tools').map((r) => this.renderLink(r))}
              </ul>
              <div class="divider my-0 flex-none"></div>
              <ul class="menu w-full flex-none">
                ${this.routes.filter((route) => route.menu === 'footer').map((r) => this.renderLink(r))}
              </ul>
              <p class="flex-none px-4 py-2 text-xs text-base-content/50">v${info.version}</p>
            </nav>
          </aside>
        </div>
      </div>
      <ism-toast-host></ism-toast-host>
    `;
  }
}

customElements.define('ism-app', IsmApp);

declare global {
  interface HTMLElementTagNameMap {
    'ism-app': IsmApp;
  }
}
