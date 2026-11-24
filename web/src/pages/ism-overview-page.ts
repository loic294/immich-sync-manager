import { html, nothing } from 'lit';
import { createRef, ref } from 'lit/directives/ref.js';
import { admin } from '@/api/admin';
import type { ImmichServerView } from '@/api/generated/ImmichServerView';
import type { OperationActivity } from '@/api/generated/OperationActivity';
import type { Overview } from '@/api/generated/Overview';
import type { ScanScheduleView } from '@/api/generated/ScanScheduleView';
import type { WatchStatusView } from '@/api/generated/WatchStatusView';
import { IsmPage } from '@/base/ism-page';
import type { IconName } from '@/components/ism-icon';
import type { IsmOperationDetail } from '@/components/ism-operation-detail';
import { overview, refreshOverview } from '@/stores/overview';
import { formatCountdown, formatDate, humanize } from '@/util/format';
import { statusBadge } from '@/util/status';
import '@/components/ism-operation-table';
import '@/components/ism-operation-detail';

/** Dashboard: what needs attention, with links to the page that fixes it. */
export class IsmOverviewPage extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    now: { state: true },
    operations: { state: true },
  };

  /** Drives the scan countdowns. */
  declare now: number;
  declare operations: OperationActivity[];
  private detail = createRef<IsmOperationDetail>();
  private clock: number | undefined;

  constructor() {
    super();
    this.now = Date.now();
    this.operations = [];
  }

  override connectedCallback() {
    super.connectedCallback();
    this.clock = window.setInterval(() => {
      if (overview.value?.scans.some((s) => s.due_at !== null)) this.now = Date.now();
    }, 1000);
  }

  override disconnectedCallback() {
    super.disconnectedCallback();
    window.clearInterval(this.clock);
  }

  protected override async load() {
    const [, page] = await Promise.all([
      overview.value ? Promise.resolve() : refreshOverview(),
      this.data.operationPage('recent', 10),
    ]);
    this.operations = page.rows;
    if (!overview.value) throw new Error('Cannot load the overview.');
  }

  private stat(label: string, value: string | number, href: string, icon: IconName, warn = false) {
    return html`
      <a href=${href} class="card card-border bg-base-100 transition-shadow hover:shadow-sm">
        <div class="card-body flex-row items-center gap-3 p-4">
          <ism-icon name=${icon} size="lg" class=${warn ? 'text-warning' : 'text-primary'}></ism-icon>
          <div>
            <div class="text-2xl font-semibold">${value}</div>
            <div class="text-sm text-base-content/70">${label}</div>
          </div>
        </div>
      </a>
    `;
  }

  private server(server: ImmichServerView) {
    const m = server.maintenance;
    return html`
      <tr>
        <td>
          <div class="font-medium">${server.name}</div>
          <div class="text-xs text-base-content/60">${server.url}</div>
        </td>
        <td>
          ${server.version ?? '—'}
          ${
            server.compatible
              ? nothing
              : html`<span class="badge badge-error badge-soft">Incompatible</span>`
          }
        </td>
        <td><span class=${statusBadge(m.phase)}>${humanize(m.phase)}</span></td>
        <td class="text-sm">
          ${m.active_ops > 0 ? `${m.active_ops} running` : ''}
          ${m.queue_paused ? 'Library queue paused. ' : ''}
          ${m.message ?? ''}
        </td>
      </tr>
    `;
  }

  private drift(server: ImmichServerView) {
    return html`
      <div role="alert" class="alert alert-warning alert-soft">
        <ism-icon name="triangle-alert"></ism-icon>
        <div>
          <div class="font-medium">${server.name}: writes are blocked</div>
          <div class="text-sm">${server.maintenance.drift} ISM detects file changes itself, so these Immich settings must stay off.</div>
        </div>
        <button type="button" class="btn btn-sm" @click=${() => this.act(`reapply:${server.id}`, () => admin.reapplyImmichSettings(server.id), 'Immich settings re-applied.')}>
          ${this.spin(`reapply:${server.id}`)}Turn them off again
        </button>
      </div>
    `;
  }

  private scan(scan: ScanScheduleView) {
    let state: unknown;
    if (scan.running) state = html`<span class="badge badge-info badge-soft">Scanning</span>`;
    else if (scan.waiting_for_barrier)
      state = html`<span class="badge badge-warning badge-soft">Waiting for running operations</span>`;
    else if (scan.due_at)
      state = html`<span class="font-mono">${formatCountdown(scan.due_at, this.now)}</span>`;
    else if (scan.last_error)
      state = html`<span class="badge badge-error badge-soft">Failed</span>`;
    else
      state = html`<span class="text-base-content/60">Done ${formatDate(scan.last_finished_at)}</span>`;
    return html`
      <tr>
        <td class="font-medium">${scan.library_name ?? scan.library_id}</td>
        <td>${state}</td>
        <td class="text-xs text-base-content/70">${scan.last_error ?? scan.reasons.join(', ')}</td>
      </tr>
    `;
  }

  private watcher(w: WatchStatusView) {
    return html`
      <tr>
        <td class="font-medium">${w.name}</td>
        <td>${humanize(w.effective_mode ?? w.mode)}</td>
        <td class="text-sm">${formatDate(w.last_pass_at)}</td>
        <td>
          ${w.paused ? html`<span class="badge badge-warning badge-soft">Paused</span>` : nothing}
          ${w.warnings.map((warning) => html`<div class="text-xs text-warning">${warning}</div>`)}
        </td>
      </tr>
    `;
  }

  private detection(o: Overview) {
    const pending = o.scans.filter((s) => s.running || s.due_at !== null || s.last_error);
    return html`
      <section class="card card-border bg-base-100">
        <div class="card-body p-4">
          <h2 class="card-title text-base">File watching</h2>
          ${
            o.watchers.length === 0
              ? html`<p class="text-sm text-base-content/70">No watched folder. Folders of an Immich library or a peer share are watched.</p>`
              : html`<div class="overflow-x-auto">
                  <table class="table table-sm">
                    <thead><tr><th>Folder</th><th>Mode</th><th>Last pass</th><th></th></tr></thead>
                    <tbody>${o.watchers.map((w) => this.watcher(w))}</tbody>
                  </table>
                </div>`
          }
          <h3 class="font-medium">Library scans</h3>
          ${
            pending.length === 0
              ? html`<p class="text-sm text-base-content/70">No scan pending.</p>`
              : html`<div class="overflow-x-auto">
                  <table class="table table-sm">
                    <thead><tr><th>Library</th><th>Starts in</th><th>Why</th></tr></thead>
                    <tbody>${pending.map((s) => this.scan(s))}</tbody>
                  </table>
                </div>`
          }
        </div>
      </section>
    `;
  }

  protected override render() {
    return html`${this.frame(() => {
      const o = overview.value;
      if (!o) return html``;
      return html`
        ${o.servers.filter((s) => s.maintenance.drift !== null).map((s) => this.drift(s))}
        <h2 class="sr-only">Summary</h2>
        <div class="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
          ${this.stat('Operations running', o.operations_running, '/operations', 'activity')}
          ${this.stat('Operations needing attention', o.operations_attention, '/operations', 'triangle-alert', o.operations_attention > 0)}
          ${this.stat('Files queued for sync', o.transfers_queued, '/sync', 'arrow-left-right')}
          ${this.stat('Open conflicts', o.conflicts_open, '/conflicts', 'git-compare-arrows', o.conflicts_open > 0)}
          ${this.stat('Peers online', `${o.peers_online} / ${o.peers_total}`, '/peers', 'network', o.peers_online < o.peers_total)}
          ${this.stat('Unavailable folders', `${o.roots_unavailable} / ${o.roots_total}`, '/mappings', 'folder-sync', o.roots_unavailable > 0)}
          ${this.stat('Libraries without an account', o.uncovered_libraries, '/servers', 'users', o.uncovered_libraries > 0)}
        </div>
        <section class="min-w-0 overflow-hidden rounded-box border border-base-300 bg-base-100">
          <div class="flex items-center justify-between gap-3 p-4">
            <h2 class="text-base font-semibold">Running & recent operations</h2>
            <a href="/operations" class="link link-hover text-sm">View all operations</a>
          </div>
          <ism-operation-table .rows=${this.operations}
            @operation-open=${(e: CustomEvent<string>) => this.detail.value?.show(e.detail)}></ism-operation-table>
        </section>
        <section class="card card-border bg-base-100">
          <div class="card-body p-4">
            <h2 class="card-title text-base">Immich servers</h2>
            ${
              o.servers.length === 0
                ? html`<p class="text-sm text-base-content/70">
                    No server yet. <a class="link" href="/servers">Add an Immich server</a> to get started.
                  </p>`
                : html`<div class="overflow-x-auto">
                    <table class="table table-sm">
                      <thead><tr><th>Server</th><th>Version</th><th>Maintenance</th><th></th></tr></thead>
                      <tbody>${o.servers.map((s) => this.server(s))}</tbody>
                    </table>
                  </div>`
            }
          </div>
        </section>
        ${this.detection(o)}
      `;
    })}<ism-operation-detail ${ref(this.detail)}></ism-operation-detail>`;
  }
}

customElements.define('ism-overview-page', IsmOverviewPage);

declare global {
  interface HTMLElementTagNameMap {
    'ism-overview-page': IsmOverviewPage;
  }
}
