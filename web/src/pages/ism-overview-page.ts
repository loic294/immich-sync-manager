import { html, nothing } from 'lit';
import type { ImmichServerView } from '@/api/generated/ImmichServerView';
import { IsmPage } from '@/base/ism-page';
import type { IconName } from '@/components/ism-icon';
import { overview, refreshOverview } from '@/stores/overview';
import { humanize } from '@/util/format';
import { statusBadge } from '@/util/status';

/** Dashboard: what needs attention, with links to the page that fixes it. */
export class IsmOverviewPage extends IsmPage {
  protected override async load() {
    await refreshOverview();
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
          ${m.seconds_until_reconcile !== null ? html`Scan in ${Math.ceil(m.seconds_until_reconcile / 60)} min` : nothing}
          ${m.message ?? ''}
        </td>
      </tr>
    `;
  }

  protected override render() {
    return this.frame(() => {
      const o = overview.value;
      if (!o) return html``;
      return html`
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
      `;
    });
  }
}

customElements.define('ism-overview-page', IsmOverviewPage);

declare global {
  interface HTMLElementTagNameMap {
    'ism-overview-page': IsmOverviewPage;
  }
}
