import { html, nothing } from 'lit';
import { admin } from '@/api/admin';
import { events } from '@/api/events';
import type { BackgroundActivity } from '@/api/generated/BackgroundActivity';
import type { BackgroundActivityPage } from '@/api/generated/BackgroundActivityPage';
import { IsmPage } from '@/base/ism-page';
import type { PageChange } from '@/components/ism-pagination';
import { formatBytes, formatDate, formatDuration, humanize } from '@/util/format';
import '@/components/ism-pagination';

export class IsmBackgroundActivity extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    snapshot: { state: true },
    limit: { state: true },
    offset: { state: true },
  };
  declare snapshot: BackgroundActivityPage | undefined;
  declare limit: number;
  declare offset: number;
  protected override reloadOn = [
    'activity_changed',
    'operation_updated',
    'operation_committed',
    'maintenance_changed',
    'sync_changed',
    'watch_changed',
    'library_scanned',
    'settings_changed',
    'conflicts_changed',
  ] as const;
  protected override liveReloadInterval = 1000;
  private timer: number | undefined;
  private receivedAt = 0;

  constructor() {
    super();
    this.limit = 25;
    this.offset = 0;
  }

  override connectedCallback() {
    super.connectedCallback();
    this.timer = window.setInterval(() => {
      this.requestUpdate();
      void this.reload();
    }, 5000);
  }

  override disconnectedCallback() {
    super.disconnectedCallback();
    window.clearInterval(this.timer);
  }

  protected override async load() {
    const { limit, offset } = this;
    const snapshot = await admin.backgroundActivity(limit, offset);
    if (limit !== this.limit || offset !== this.offset) return;
    this.snapshot = snapshot;
    this.receivedAt = Date.now();
    if (offset > 0 && offset >= snapshot.total) {
      this.offset = Math.max(0, Math.ceil(snapshot.total / limit) - 1) * limit;
      void this.reload();
    }
  }

  private rows(rows: BackgroundActivity[]) {
    return html`<ul class="divide-y divide-base-300">
      ${rows.map((task) => {
        const active = task.status === 'running';
        const failed = ['failed', 'blocked', 'interrupted'].includes(task.status);
        const bytes = task.unit.startsWith('bytes');
        const count = (n: number) => (bytes ? formatBytes(n) : n.toLocaleString());
        const measured = task.total !== null && task.total > 0;
        const elapsed =
          (Date.parse(task.finished_at ?? new Date().toISOString()) - Date.parse(task.started_at)) /
          1000;
        return html`<li class="grid min-w-0 gap-3 py-3 md:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]">
          <div class="min-w-0">
            <div class="flex flex-wrap items-center gap-2">
              ${active ? html`<span class="loading loading-spinner loading-xs" aria-label="Running"></span>` : nothing}
              <h4 class="font-medium">${task.title}</h4>
              <span class=${failed ? 'badge badge-error badge-soft badge-sm' : active ? 'badge badge-info badge-soft badge-sm' : 'badge badge-ghost badge-sm'}>${humanize(task.status)}</span>
            </div>
            <p class="mt-1 break-words text-sm text-base-content/70">${task.resource}</p>
            <p class="mt-1 break-words text-sm">${task.phase}</p>
            ${task.next_at ? html`<p class="mt-1 text-sm">Next eligible: ${formatDate(task.next_at)}</p>` : nothing}
            ${task.error ? html`<details class="mt-2 text-sm"><summary class="cursor-pointer">Error details</summary><p class="mt-1 break-words">${task.error}</p></details>` : nothing}
            ${task.operation_id ? html`<button type="button" class="link link-hover mt-2 text-sm" @click=${() => this.dispatchEvent(new CustomEvent('operation-open', { detail: task.operation_id, bubbles: true, composed: true }))}>Open related operation</button>` : task.page && task.page !== '/operations' ? html`<a class="link link-hover mt-2 inline-block text-sm" href=${task.page}>${task.page === '/sync' ? 'View sync' : task.page === '/servers' ? 'View server maintenance' : task.page === '/mappings' ? 'View folder mappings' : 'View peers and shares'}</a>` : nothing}
          </div>
          <div class="min-w-0 space-y-1 text-sm">
            ${measured ? html`<progress class="progress progress-primary w-full" aria-label=${`${task.title}: ${task.unit}`} value=${task.done} max=${task.total}></progress>` : nothing}
            ${task.done > 0 || measured ? html`<p>${count(task.done)}${measured ? ` / ${count(task.total ?? 0)}` : ''} ${bytes ? task.unit.replace('bytes', '').trim() : task.unit}</p>` : active ? html`<p class="text-base-content/70">Progress total not yet known</p>` : nothing}
            ${!task.id.startsWith('state:') ? html`<p class="text-xs text-base-content/70">${task.finished_at ? 'Run duration' : 'Elapsed'}: ${formatDuration(elapsed)}</p>` : nothing}
            <p class="break-words text-xs text-base-content/70">${task.id.startsWith('state:') ? 'Last checked' : 'Last update'}: ${formatDate(task.updated_at)}</p>
            ${active && Date.now() - Date.parse(task.updated_at) > 30_000 ? html`<p class="text-xs text-warning">No progress update since ${formatDate(task.updated_at)}</p>` : nothing}
          </div>
        </li>`;
      })}
    </ul>`;
  }

  protected override render() {
    const snapshot = this.snapshot;
    const disconnected = events.connection.value !== 'open';
    const stale = this.receivedAt > 0 && Date.now() - this.receivedAt > 15_000;
    const groups = [
      [
        'Needs attention',
        snapshot?.current.filter((a) => ['failed', 'blocked', 'interrupted'].includes(a.status)) ??
          [],
      ],
      ['Running', snapshot?.current.filter((a) => a.status === 'running') ?? []],
      ['Waiting', snapshot?.current.filter((a) => a.status === 'waiting') ?? []],
    ] as const;
    return html`<section class="min-w-0 rounded-box border border-base-300 bg-base-100 p-4" aria-labelledby="background-activity-title">
      <div class="flex flex-wrap items-center justify-between gap-2">
        <h2 id="background-activity-title" class="text-lg font-semibold">Background activity</h2>
        <button type="button" class="btn btn-ghost btn-sm" @click=${() => this.reload()}>Refresh activity</button>
      </div>
      <p class="mt-1 text-sm text-base-content/70">What ISM is doing, and what is preventing work from advancing.</p>
      ${this.loadError ? html`<p role="alert" class="alert alert-error alert-soft mt-3">Activity could not be refreshed: ${this.loadError}. ${snapshot ? 'Previously received activity is shown below.' : 'Current activity is unknown.'}</p>` : nothing}
      ${snapshot && (disconnected || stale) ? html`<p role="status" class="alert alert-warning alert-soft mt-3">${stale ? 'Activity snapshot is stale.' : 'Live updates are disconnected; automatic refresh continues every 5 seconds.'} Last checked: ${formatDate(snapshot.observed_at)}</p>` : nothing}
      ${snapshot?.warnings.map((warning) => html`<p role="alert" class="alert alert-warning alert-soft mt-3">${warning}</p>`)}
      ${this.loading ? html`<p class="mt-4"><span class="loading loading-spinner loading-sm" aria-label="Loading background activity"></span></p>` : nothing}
      ${groups.filter(([, rows]) => rows.length > 0).map(([label, rows]) => html`<section class="mt-5"><h3 class="text-sm font-semibold">${label} <span class="text-base-content/70">(${rows.length})</span></h3>${this.rows(rows)}</section>`)}
      ${snapshot && !this.loadError && !stale && snapshot.current.length === 0 ? html`<p class="py-4 text-sm text-base-content/70">No background work or sync blockers reported at the last check. Managed file operations are listed below.</p>` : nothing}
      <details class="mt-4 border-t border-base-300 pt-3">
        <summary class="cursor-pointer text-sm font-medium">Recent background runs${snapshot ? ` (${snapshot.total})` : ''}</summary>
        <p class="mt-2 text-xs text-base-content/70">Completed runs are retained for seven days, up to 1,000 runs. Unresolved issues stay above.</p>
        ${this.rows(snapshot?.history ?? [])}
        ${snapshot && snapshot.history.length === 0 ? html`<p class="py-3 text-sm text-base-content/70">No recent background runs.</p>` : nothing}
        <ism-pagination .total=${snapshot?.total ?? 0} .limit=${this.limit} .offset=${this.offset}
          @page-change=${(event: CustomEvent<PageChange>) => {
            this.limit = event.detail.limit;
            this.offset = event.detail.offset;
            void this.reload();
          }}></ism-pagination>
      </details>
    </section>`;
  }
}
customElements.define('ism-background-activity', IsmBackgroundActivity);
declare global {
  interface HTMLElementTagNameMap {
    'ism-background-activity': IsmBackgroundActivity;
  }
}
