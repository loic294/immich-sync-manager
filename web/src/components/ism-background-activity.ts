import { html, nothing } from 'lit';
import { admin } from '@/api/admin';
import { events } from '@/api/events';
import type { BackgroundActivity } from '@/api/generated/BackgroundActivity';
import type { BackgroundActivityPage } from '@/api/generated/BackgroundActivityPage';
import { IsmPage } from '@/base/ism-page';
import type { PageChange } from '@/components/ism-pagination';
import { formatBytes, formatDate, formatDuration, humanize } from '@/util/format';
import { tabKeydown } from '@/util/tabs';
import '@/components/ism-icon';
import '@/components/ism-pagination';

type Filter = 'attention' | 'running' | 'waiting' | 'all';
const ATTENTION = ['failed', 'blocked', 'interrupted'];
const FILTERS: readonly [Filter, string][] = [
  ['attention', 'Needs attention'],
  ['running', 'Running'],
  ['waiting', 'Waiting'],
  ['all', 'All'],
];
const PAGES: Record<string, string> = {
  '/sync': 'Open sync',
  '/servers': 'Open server maintenance',
  '/mappings': 'Open folder mappings',
  '/peers': 'Open peers and shares',
};
// Polling only backs up live events, so it can be slow; the snapshot is stale after a few misses.
const POLL_MS = 10_000;
const STALE_MS = 35_000;

function matches(filter: Filter, task: BackgroundActivity) {
  if (filter === 'all') return true;
  if (filter === 'attention') return ATTENTION.includes(task.status);
  return task.status === filter;
}

function ago(value: string, now: number) {
  const seconds = (now - Date.parse(value)) / 1000;
  if (Number.isNaN(seconds)) return '—';
  return seconds < 5 ? 'just now' : `${formatDuration(seconds)} ago`;
}

export class IsmBackgroundActivity extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    snapshot: { state: true },
    filter: { state: true },
    limit: { state: true },
    offset: { state: true },
  };
  declare snapshot: BackgroundActivityPage | undefined;
  declare filter: Filter | undefined;
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
  protected override liveReloadInterval = 2000;
  private timer: number | undefined;
  private receivedAt = 0;

  constructor() {
    super();
    this.filter = undefined;
    this.limit = 25;
    this.offset = 0;
  }

  override connectedCallback() {
    super.connectedCallback();
    this.timer = window.setInterval(() => {
      this.requestUpdate();
      void this.reload();
    }, POLL_MS);
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

  /** The chosen tab, else the most urgent one that has something in it. */
  private activeFilter(current: BackgroundActivity[]): Filter {
    if (this.filter) return this.filter;
    return (
      FILTERS.map(([f]) => f).find((f) => f !== 'all' && current.some((t) => matches(f, t))) ??
      'all'
    );
  }

  private measure(task: BackgroundActivity) {
    const bytes = task.unit.startsWith('bytes');
    const fmt = (n: number) => (bytes ? formatBytes(n) : n.toLocaleString());
    const unit = bytes ? task.unit.replace('bytes', '').trim() : task.unit;
    const measured = task.total !== null && task.total > 0;
    return { bytes, fmt, unit, measured };
  }

  private icon(task: BackgroundActivity) {
    if (task.status === 'running')
      return html`<span class="loading loading-spinner loading-sm text-info" aria-hidden="true"></span>`;
    if (ATTENTION.includes(task.status))
      return html`<ism-icon name="circle-alert" class="text-error"></ism-icon>`;
    if (task.status === 'completed')
      return html`<ism-icon name="circle-check" class="text-success"></ism-icon>`;
    return html`<ism-icon name="clock" class="text-base-content/60"></ism-icon>`;
  }

  /** Right-hand summary: how much work, and how far along, in one glance. */
  private aside(task: BackgroundActivity, now: number) {
    const { bytes, fmt, unit, measured } = this.measure(task);
    const items =
      task.count > 1 ? `${task.count.toLocaleString()} ${bytes ? 'files' : 'items'}` : '';
    let amount = '';
    if (task.status === 'running' && measured)
      amount = `${fmt(task.done)} / ${fmt(task.total ?? 0)} ${unit}`;
    else if (bytes && measured) amount = fmt(task.total ?? 0);
    else if (task.done > 0) amount = `${fmt(task.done)} ${unit}`;
    const pct = measured ? Math.min(100, Math.round((task.done / (task.total ?? 1)) * 100)) : 0;
    return html`<div class="flex min-w-0 flex-col items-end gap-1 text-right text-sm tabular-nums">
      ${items ? html`<span class="font-medium">${items}</span>` : nothing}
      ${amount ? html`<span class="text-base-content/70">${amount}</span>` : nothing}
      ${task.status === 'running' && measured ? html`<progress class="progress progress-info h-1.5 w-28" value=${pct} max="100" aria-label=${`${task.title}: ${pct}%`}></progress>` : nothing}
      <span class="text-xs text-base-content/60">${task.finished_at ? `Finished ${ago(task.finished_at, now)}` : `Updated ${ago(task.updated_at, now)}`}</span>
    </div>`;
  }

  private details(task: BackgroundActivity, now: number) {
    const { measured, fmt, unit } = this.measure(task);
    const stalled = task.status === 'running' && now - Date.parse(task.updated_at) > 60_000;
    const elapsed =
      (Date.parse(task.finished_at ?? new Date(now).toISOString()) - Date.parse(task.started_at)) /
      1000;
    const reasonTotal = task.reasons.reduce((n, r) => n + r.count, 0);
    return html`<div class="grid gap-4 px-3 pb-4 pl-10 text-sm md:grid-cols-[minmax(0,3fr)_minmax(0,2fr)]">
      <div class="min-w-0 space-y-3">
        ${stalled ? html`<p class="text-warning">No progress reported for ${formatDuration((now - Date.parse(task.updated_at)) / 1000)}.</p>` : nothing}
        ${task.next_at ? html`<p>Next eligible: <span class="font-medium">${formatDate(task.next_at)}</span></p>` : nothing}
        ${task.error ? html`<p class="break-words text-error">${task.error}</p>` : nothing}
        ${
          task.reasons.length > 0
            ? html`<div>
              <h5 class="mb-1 font-medium">${task.count > reasonTotal ? `Reasons (first ${reasonTotal.toLocaleString()} checked)` : 'Reasons'}</h5>
              <ul class="space-y-1">${task.reasons.map(
                (r) => html`<li class="flex items-baseline justify-between gap-3">
                  <span class="min-w-0 break-words">${r.reason}</span>
                  <span class="shrink-0 tabular-nums text-base-content/70">${r.count.toLocaleString()}</span>
                </li>`,
              )}</ul>
            </div>`
            : nothing
        }
        ${
          task.examples.length > 0
            ? html`<div>
              <h5 class="mb-1 font-medium">${task.count > task.examples.length ? `Examples (${task.examples.length} of ${task.count.toLocaleString()})` : 'Items'}</h5>
              <ul class="space-y-1">${task.examples.map(
                (e) => html`<li class="min-w-0">
                  <span class="break-all font-mono text-xs">${e.resource}</span>
                  ${e.detail ? html`<span class="block break-words text-xs text-base-content/70">${e.detail}</span>` : nothing}
                </li>`,
              )}</ul>
            </div>`
            : nothing
        }
      </div>
      <dl class="grid grid-cols-[auto_minmax(0,1fr)] content-start gap-x-3 gap-y-1 text-base-content/80">
        <dt class="text-base-content/60">Status</dt><dd>${humanize(task.status)}</dd>
        ${measured ? html`<dt class="text-base-content/60">Progress</dt><dd class="tabular-nums">${fmt(task.done)} / ${fmt(task.total ?? 0)} ${unit}</dd>` : nothing}
        <dt class="text-base-content/60">${task.id.startsWith('state:') ? 'Oldest item' : 'Started'}</dt><dd>${formatDate(task.started_at)}</dd>
        ${!task.id.startsWith('state:') ? html`<dt class="text-base-content/60">${task.finished_at ? 'Duration' : 'Elapsed'}</dt><dd>${formatDuration(elapsed)}</dd>` : nothing}
        <dt class="text-base-content/60">Last update</dt><dd>${formatDate(task.updated_at)}</dd>
        <dd class="col-span-2 mt-2 flex flex-wrap gap-2">
          ${task.operation_id ? html`<button type="button" class="btn btn-sm" @click=${() => this.dispatchEvent(new CustomEvent('operation-open', { detail: task.operation_id, bubbles: true, composed: true }))}>Open related operation</button>` : nothing}
          ${task.page && PAGES[task.page] ? html`<a class="btn btn-sm" href=${task.page}>${PAGES[task.page]}</a>` : nothing}
        </dd>
      </dl>
    </div>`;
  }

  private rows(rows: BackgroundActivity[], now: number) {
    return html`<ul class="divide-y divide-base-300">
      ${rows.map(
        (task) => html`<li>
          <details class="group">
            <summary class="grid cursor-pointer list-none grid-cols-[1.25rem_minmax(0,1fr)_auto] items-center gap-x-3 rounded-field px-3 py-3 hover:bg-base-200 focus-visible:outline-2 focus-visible:outline-primary [&::-webkit-details-marker]:hidden">
              <span class="flex justify-center">${this.icon(task)}</span>
              <span class="min-w-0">
                <span class="flex min-w-0 items-baseline gap-2">
                  <span class="shrink-0 font-medium">${task.title}</span>
                  <span class="truncate text-sm text-base-content/70" title=${task.resource}>${task.resource}</span>
                </span>
                <span class="block truncate text-sm ${ATTENTION.includes(task.status) ? 'text-error' : 'text-base-content/80'}" title=${task.phase}>${task.phase}</span>
              </span>
              ${this.aside(task, now)}
            </summary>
            ${this.details(task, now)}
          </details>
        </li>`,
      )}
    </ul>`;
  }

  private skeleton() {
    return html`<div class="space-y-3 py-3" aria-label="Loading background activity" role="status">
      ${[0, 1, 2].map(() => html`<div class="flex items-center gap-3 px-3"><div class="skeleton size-5 rounded-full"></div><div class="flex-1 space-y-2"><div class="skeleton h-4 w-1/3"></div><div class="skeleton h-3 w-2/3"></div></div><div class="skeleton h-4 w-20"></div></div>`)}
    </div>`;
  }

  private freshness(now: number) {
    const snapshot = this.snapshot;
    const stale = this.receivedAt > 0 && now - this.receivedAt > STALE_MS;
    if (this.loadError)
      return html`<p role="alert" class="flex flex-wrap items-center gap-2 text-sm text-error">
        <ism-icon name="triangle-alert"></ism-icon>
        Couldn't refresh: ${this.loadError}. ${snapshot ? `Showing what was known ${ago(snapshot.observed_at, now)}.` : 'Current activity is unknown.'}
      </p>`;
    if (!snapshot) return nothing;
    const live = events.connection.value === 'open' && !stale;
    return html`<p role="status" class="flex items-center gap-2 text-sm ${live ? 'text-base-content/60' : 'text-warning'}">
      <span class="status ${live ? 'status-success' : 'status-warning'}" aria-hidden="true"></span>
      ${live ? 'Live' : stale ? 'Not refreshed recently' : 'Live updates disconnected; checking every 10 sec'} · updated ${ago(snapshot.observed_at, now)}
    </p>`;
  }

  protected override render() {
    const now = Date.now();
    const current = this.snapshot?.current ?? [];
    const filter = this.activeFilter(current);
    const order = (t: BackgroundActivity) =>
      ATTENTION.includes(t.status) ? 0 : t.status === 'running' ? 1 : 2;
    const visible = current
      .filter((t) => matches(filter, t))
      .sort(
        (a, b) =>
          order(a) - order(b) || b.count - a.count || a.started_at.localeCompare(b.started_at),
      );
    const counts = Object.fromEntries(
      FILTERS.map(([f]) => [f, current.filter((t) => matches(f, t)).length]),
    ) as Record<Filter, number>;
    return html`<section class="min-w-0 rounded-box border border-base-300 bg-base-100" aria-labelledby="background-activity-title">
      <header class="flex flex-wrap items-start justify-between gap-3 px-4 pt-4">
        <div class="min-w-0">
          <h2 id="background-activity-title" class="text-lg font-semibold">Background activity</h2>
          <div class="mt-1">${this.freshness(now)}</div>
        </div>
        <button type="button" class="btn btn-ghost btn-sm" aria-label="Refresh activity" @click=${() => this.reload()}>
          <ism-icon name="refresh-cw"></ism-icon> Refresh
        </button>
      </header>
      ${this.snapshot?.warnings.map((w) => html`<p role="alert" class="mx-4 mt-3 text-sm text-warning">${w}</p>`)}
      <div role="tablist" aria-label="Background activity status" class="tabs tabs-border mt-2 flex-nowrap overflow-x-auto px-2" @keydown=${tabKeydown}>
        ${FILTERS.map(
          ([
            value,
            label,
          ]) => html`<button type="button" role="tab" id=${`activity-tab-${value}`} aria-controls="activity-panel"
            aria-selected=${filter === value} tabindex=${filter === value ? 0 : -1}
            class=${filter === value ? 'tab tab-active gap-2 whitespace-nowrap' : 'tab gap-2 whitespace-nowrap'}
            @click=${() => {
              this.filter = value;
            }}>
            ${label}
            <span class=${value === 'attention' && counts.attention > 0 ? 'badge badge-error badge-sm' : 'badge badge-ghost badge-sm'}>${counts[value]}</span>
          </button>`,
        )}
      </div>
      <div id="activity-panel" role="tabpanel" aria-labelledby=${`activity-tab-${filter}`} class="px-1">
        ${
          this.loading && !this.snapshot
            ? this.skeleton()
            : visible.length > 0
              ? this.rows(visible, now)
              : html`<p class="px-3 py-6 text-sm text-base-content/70">${
                  this.snapshot
                    ? filter === 'attention'
                      ? 'Nothing needs attention.'
                      : filter === 'running'
                        ? 'Nothing is running right now.'
                        : filter === 'waiting'
                          ? 'Nothing is waiting.'
                          : 'ISM is idle: no background work or sync blockers.'
                    : 'Current activity is unknown.'
                }</p>`
        }
      </div>
      <details class="border-t border-base-300">
        <summary class="cursor-pointer px-4 py-3 text-sm font-medium">Recent runs${this.snapshot ? html` <span class="text-base-content/60">(${this.snapshot.total.toLocaleString()})</span>` : nothing}</summary>
        <p class="px-4 text-xs text-base-content/60">Kept for seven days, up to 1,000 runs.</p>
        <div class="px-1">${this.rows(this.snapshot?.history ?? [], now)}</div>
        ${this.snapshot && this.snapshot.history.length === 0 ? html`<p class="px-4 py-3 text-sm text-base-content/70">No recent runs.</p>` : nothing}
        <div class="px-4 pb-3">
          <ism-pagination .total=${this.snapshot?.total ?? 0} .limit=${this.limit} .offset=${this.offset}
            @page-change=${(event: CustomEvent<PageChange>) => {
              this.limit = event.detail.limit;
              this.offset = event.detail.offset;
              void this.reload();
            }}></ism-pagination>
        </div>
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
