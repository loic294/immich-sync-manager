import { html, nothing } from 'lit';
import { createRef, ref } from 'lit/directives/ref.js';
import { admin } from '@/api/admin';
import { isApiError } from '@/api/errors';
import type { OverrideView } from '@/api/generated/OverrideView';
import type { PeerView } from '@/api/generated/PeerView';
import type { ScheduleView } from '@/api/generated/ScheduleView';
import type { ShareView } from '@/api/generated/ShareView';
import type { SyncActivity } from '@/api/generated/SyncActivity';
import type { SyncProgress } from '@/api/generated/SyncProgress';
import type { SyncStatus } from '@/api/generated/SyncStatus';
import type { Window } from '@/api/generated/Window';
import { IsmPage } from '@/base/ism-page';
import type { PageChange } from '@/components/ism-pagination';
import type { IsmSyncDetail } from '@/components/ism-sync-detail';
import {
  formatBitRate,
  formatBytes,
  formatDate,
  formatDuration,
  humanize,
  percent,
} from '@/util/format';
import { statusBadge } from '@/util/status';
import { tabKeydown } from '@/util/tabs';
import '@/components/ism-pagination';
import '@/components/ism-sync-thumbnail';
import '@/components/ism-sync-detail';
import '@/components/ism-recovery-list';

const DAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'] as const;

function timeZones(): string[] {
  const intl = Intl as unknown as { supportedValuesOf?: (key: string) => string[] };
  return intl.supportedValuesOf?.('timeZone') ?? ['UTC'];
}

/**
 * File transfers between peers: what runs now, the queue, the hours when new file content may be
 * transferred, and temporary overrides. Folder and asset changes are never held back by the hours.
 */
export class IsmSyncPage extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    status: { state: true },
    schedule: { state: true },
    scheduleInvalid: { state: true },
    overrides: { state: true },
    shares: { state: true },
    peers: { state: true },
    rows: { state: true },
    total: { state: true },
    view: { state: true },
    limit: { state: true },
    offset: { state: true },
    progress: { state: true },
    warnings: { state: true },
  };

  declare status: SyncStatus | undefined;
  declare schedule: ScheduleView | undefined;
  declare scheduleInvalid: boolean;
  declare overrides: OverrideView[];
  declare shares: ShareView[];
  declare peers: PeerView[];
  declare rows: SyncActivity[];
  declare total: number;
  declare view: string;
  declare limit: number;
  declare offset: number;
  declare progress: SyncProgress | undefined;
  declare warnings: string[];
  private refreshTimer: number | undefined;
  protected override reloadOn = [
    'sync_changed',
    'operation_updated',
    'operation_committed',
  ] as const;
  private overrideDialog = createRef<HTMLDialogElement>();
  private settingsDialog = createRef<HTMLDialogElement>();
  private detail = createRef<IsmSyncDetail>();
  private scheduleDirty = false;

  constructor() {
    super();
    this.status = undefined;
    this.schedule = undefined;
    this.scheduleInvalid = false;
    this.overrides = [];
    this.shares = [];
    this.peers = [];
    this.rows = [];
    this.total = 0;
    this.view = 'active';
    this.limit = 250;
    this.offset = 0;
    this.progress = undefined;
    this.warnings = [];
  }

  override connectedCallback() {
    super.connectedCallback();
    this.refreshTimer = window.setInterval(() => {
      if (!document.hidden) void this.reload();
    }, 5000);
  }

  override disconnectedCallback() {
    window.clearInterval(this.refreshTimer);
    this.refreshTimer = undefined;
    super.disconnectedCallback();
  }

  protected override async load() {
    const { view, limit, offset } = this;
    const [status, schedule, overrides, shares, peers, page] = await Promise.all([
      admin.sync().catch((error: unknown) => {
        if (isApiError(error) && error.code === 'invalid_schedule') return undefined;
        throw error;
      }),
      admin.schedule().catch((error: unknown) => {
        if (isApiError(error) && error.code === 'invalid_schedule') return undefined;
        throw error;
      }),
      admin.overrides(),
      admin.shares(),
      admin.peers(),
      admin.syncPage(view === 'attention' ? 'active' : view, limit, offset),
    ]);
    this.status = status;
    this.scheduleInvalid = !schedule;
    if (!this.scheduleDirty) {
      this.schedule = schedule ?? {
        timezone: 'UTC',
        windows: [{ days: [], start: '00:00', end: '00:00' }],
      };
    }
    this.overrides = overrides;
    this.shares = shares;
    this.peers = peers;
    this.progress = page.progress;
    this.warnings = page.warnings ?? [];
    if (view === this.view && limit === this.limit && offset === this.offset) {
      this.rows = page.rows;
      this.total = page.total;
      if (offset > 0 && offset >= page.total) {
        this.offset = Math.max(0, Math.ceil(page.total / limit) - 1) * limit;
        void this.reload();
      }
    }
  }

  private renderProgress() {
    const p = this.progress;
    if (!p) return nothing;
    const complete = p.files_total > 0 && p.files_completed === p.files_total;
    const value =
      p.bytes_total > 0
        ? percent(p.bytes_done, p.bytes_total)
        : percent(p.files_completed, p.files_total);
    const eta = complete
      ? 'Complete'
      : p.eta_seconds !== null
        ? `${formatDuration(p.eta_seconds)} remaining`
        : p.publishing > 0 && p.bytes_done === p.bytes_total
          ? 'Finishing publication'
          : p.files_total === 0
            ? 'No transfers queued'
            : p.failed > 0 && p.running === 0 && p.queued === 0
              ? 'Needs attention'
              : 'Waiting for transfer speed';
    return html`<section class="rounded-box border border-base-300 bg-base-100 p-4" aria-labelledby="sync-progress-title">
      <div class="flex flex-wrap items-baseline justify-between gap-x-6 gap-y-1">
        <h2 id="sync-progress-title" class="text-base font-semibold">Current sync</h2>
        <span class="text-sm tabular-nums">${p.files_completed.toLocaleString()} of ${p.files_total.toLocaleString()} files transferred</span>
      </div>
      <div class="my-3 flex items-center gap-3">
        <progress class="progress progress-primary flex-1" aria-label="Total sync progress" value=${value} max="100"></progress>
        <span class="min-w-10 text-right text-sm tabular-nums">${value}%</span>
      </div>
      <div class="flex flex-wrap items-baseline justify-between gap-x-6 gap-y-2 text-sm tabular-nums">
        <span>${formatBytes(p.bytes_done)} of ${formatBytes(p.bytes_total)}</span>
        <span>${formatBitRate(p.bytes_per_second)} <span class="text-base-content/70">(last minute)</span></span>
        <span>${eta}</span>
      </div>
    </section>
    ${this.warnings.map((warning) => html`<p role="alert" class="alert alert-warning alert-soft">${warning} Speed and remaining time may be unavailable.</p>`)}`;
  }

  private editSchedule(change: (schedule: ScheduleView) => ScheduleView) {
    if (!this.schedule) return;
    this.scheduleDirty = true;
    this.schedule = change(this.schedule);
  }

  private editWindow(index: number, change: Partial<Window>) {
    this.editSchedule((s) => ({
      ...s,
      windows: s.windows.map((w, i) => (i === index ? { ...w, ...change } : w)),
    }));
  }

  private toggleDay(index: number, day: number) {
    const window = this.schedule?.windows[index];
    if (!window) return;
    const days = window.days.includes(day)
      ? window.days.filter((d) => d !== day)
      : [...window.days, day].sort((a, b) => a - b);
    this.editWindow(index, { days });
  }

  private async saveSchedule() {
    const schedule = this.schedule;
    if (!schedule) return;
    await this.act(
      'schedule',
      async () => {
        this.schedule = await admin.saveSchedule(schedule);
        this.scheduleDirty = false;
      },
      'Allowed hours saved.',
    );
  }

  private async onOverrideSubmit(event: SubmitEvent) {
    event.preventDefault();
    const formElement = event.target as HTMLFormElement;
    const form = new FormData(formElement);
    const ok = await this.act(
      'override',
      () =>
        admin.createOverride({
          share_id: String(form.get('share_id') ?? '') || null,
          peer_id: String(form.get('peer_id') ?? '') || null,
          minutes: Number(form.get('minutes') ?? 60),
          reason: String(form.get('reason') ?? '').trim(),
        }),
      'Transfers allowed now.',
    );
    if (ok) {
      formElement.reset();
      this.overrideDialog.value?.close();
    }
  }

  private renderTransfers() {
    return html`<section class="min-w-0 rounded-box border border-base-300 bg-base-100">
      <div role="tablist" aria-label="Sync view" class="tabs tabs-border" @keydown=${tabKeydown}>
        ${['active', 'attention', 'completed'].map(
          (view) => html`<button type="button" role="tab"
          id=${`sync-tab-${view}`} aria-controls="sync-panel" aria-selected=${this.view === view}
          tabindex=${this.view === view ? 0 : -1}
          class=${this.view === view ? 'tab tab-active' : 'tab'}
          @click=${() => {
            this.view = view;
            this.offset = 0;
            void this.reload();
          }}>
          ${view === 'active' ? 'Active & queued' : view === 'attention' ? 'Needs attention' : 'Completed syncs'}</button>`,
        )}
      </div>
      <div id="sync-panel" role="tabpanel" aria-labelledby=${`sync-tab-${this.view}`}>
        ${
          this.view === 'attention'
            ? html`<ism-recovery-list></ism-recovery-list>`
            : html`
        ${this.view === 'active' ? html`<p class="px-4 pt-3 text-xs text-base-content/70">Incoming and outgoing transfers, with current runs first, then files in queue order. Files waiting for a peer, retry delay or allowed hours may be skipped until eligible.</p>` : nothing}
        <div class="overflow-x-auto">
          <table class="table table-sm">
            <thead><tr><th><span class="sr-only">Preview</span></th><th>File</th><th>Sync direction</th><th>Size</th><th>Status & progress</th>${this.view === 'completed' ? html`<th>Finished</th>` : nothing}</tr></thead>
            <tbody>${
              this.rows.length
                ? this.rows.map(
                    (t) => html`<tr class="hover:bg-base-200">
              <td class="w-16"><ism-sync-thumbnail .transfer=${t}></ism-sync-thumbnail></td>
              <td class="max-w-72 break-all">
                <button type="button" class="link link-hover text-left font-medium" @click=${() => this.detail.value?.show(t)}>${t.relative_path.split('/').at(-1)}</button>
                <div class="text-xs text-base-content/70">${t.share_name} / ${t.relative_path}</div>
                ${t.error ? html`<details class="text-xs text-base-content/70"><summary class="cursor-pointer">Technical details</summary><p class="mt-1">${t.error}</p></details>` : nothing}
              </td>
              <td class="text-xs"><div>${t.source}</div><div>→ ${t.destination}</div></td>
              <td class="whitespace-nowrap">${formatBytes(t.size)}</td>
              <td class="min-w-32"><span class=${statusBadge(t.status)}>${humanize(t.status)}</span>
                ${t.status === 'running' ? html`<div><progress class="progress progress-primary w-24" aria-label="Transfer progress" value=${percent(t.bytes_done, t.size)} max="100"></progress> <span class="text-xs">${percent(t.bytes_done, t.size)}%</span></div>` : nothing}
              </td>
              ${this.view === 'completed' ? html`<td class="whitespace-nowrap">${formatDate(t.finished_at)}</td>` : nothing}
            </tr>`,
                  )
                : html`<tr><td colspan=${this.view === 'completed' ? 6 : 5} class="p-6 text-center text-base-content/70">${this.view === 'completed' ? 'No completed syncs recorded yet.' : 'No active or queued files. New peer changes will appear here.'}</td></tr>`
            }</tbody>
          </table>
        </div>
        <ism-pagination .total=${this.total} .limit=${this.limit} .offset=${this.offset}
          @page-change=${(e: CustomEvent<PageChange>) => {
            this.limit = e.detail.limit;
            this.offset = e.detail.offset;
            void this.reload();
          }}></ism-pagination>
        `
        }
      </div>
    </section>`;
  }

  private renderSchedule() {
    const s = this.schedule;
    if (!s) return nothing;
    return html`
      <section>
        <div class="flex flex-col gap-3">
          <h2 class="card-title text-base">Allowed hours for file transfers</h2>
          <p class="text-sm text-base-content/70">
            New and changed file content is copied to peers only inside these windows. With no window,
            transfers are always allowed. A window whose end is before its start runs past midnight.
          </p>
          <label class="fieldset max-w-xs">
            <span class="fieldset-legend">Time zone</span>
            <input
              class="input input-sm w-full"
              list="ism-time-zones"
              .value=${s.timezone}
              @change=${(e: Event) => this.editSchedule((x) => ({ ...x, timezone: (e.target as HTMLInputElement).value }))}
            />
            <datalist id="ism-time-zones">${timeZones().map((z) => html`<option value=${z}></option>`)}</datalist>
          </label>
          ${s.windows.map(
            (w, i) => html`
              <div class="flex flex-wrap items-center gap-2 rounded-box border border-base-300 p-2">
                <div class="join">
                  ${DAYS.map(
                    (day, d) => html`<input
                      type="checkbox"
                      class="join-item btn btn-sm"
                      aria-label=${day}
                      .checked=${w.days.includes(d)}
                      @change=${() => this.toggleDay(i, d)}
                    />`,
                  )}
                </div>
                <input type="time" class="input input-sm w-auto" aria-label="Start" .value=${w.start}
                  @change=${(e: Event) => this.editWindow(i, { start: (e.target as HTMLInputElement).value })} />
                <span>to</span>
                <input type="time" class="input input-sm w-auto" aria-label="End" .value=${w.end}
                  @change=${(e: Event) => this.editWindow(i, { end: (e.target as HTMLInputElement).value })} />
                <button type="button" class="btn btn-ghost btn-sm btn-square" aria-label="Remove window"
                  @click=${() => this.editSchedule((x) => ({ ...x, windows: x.windows.filter((_, j) => j !== i) }))}>
                  <ism-icon name="x" size="sm"></ism-icon>
                </button>
              </div>
            `,
          )}
          <div class="flex gap-2">
            <button type="button" class="btn btn-sm"
              @click=${() => this.editSchedule((x) => ({ ...x, windows: [...x.windows, { days: [0, 1, 2, 3, 4, 5, 6], start: '01:00', end: '06:00' }] }))}>
              <ism-icon name="plus" size="sm"></ism-icon>Add window
            </button>
            <button type="button" class="btn btn-primary btn-sm" ?disabled=${this.busy !== undefined} @click=${this.saveSchedule}>
              ${this.spin('schedule')}Save hours
            </button>
          </div>
        </div>
      </section>
    `;
  }

  private renderOverrides() {
    const shareName = (id: string | null) =>
      id ? (this.shares.find((s) => s.id === id)?.name ?? id) : 'All shares';
    const peerName = (id: string | null) =>
      id ? (this.peers.find((p) => p.id === id)?.name ?? id) : 'all peers';
    return html`
      <section>
        <div class="flex flex-col gap-3">
          <h2 class="card-title text-base">Overrides</h2>
          ${
            this.overrides.length === 0
              ? html`<p class="text-sm text-base-content/60">No active override.</p>`
              : html`<ul class="list">
                  ${this.overrides.map(
                    (o) => html`<li class="list-row items-center">
                      <ism-icon name="calendar-clock"></ism-icon>
                      <div>
                        <div>${shareName(o.share_id)} with ${peerName(o.peer_id)} until ${formatDate(o.until)}</div>
                        <div class="text-xs text-base-content/60">${o.reason}</div>
                      </div>
                      <button type="button" class="btn btn-ghost btn-sm" @click=${() => this.act(`revoke:${o.id}`, () => admin.revokeOverride(o.id), 'Override ended.')}>
                        ${this.spin(`revoke:${o.id}`)}End
                      </button>
                    </li>`,
                  )}
                </ul>`
          }
        </div>
      </section>
    `;
  }

  private renderOverrideDialog() {
    return html`
      <dialog class="modal" ${ref(this.overrideDialog)}>
        <div class="modal-box">
          <h3 class="text-lg font-semibold">Allow transfers now</h3>
          <form class="flex flex-col gap-1" @submit=${this.onOverrideSubmit}>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Share</legend>
              <select name="share_id" class="select w-full">
                <option value="">All shares</option>
                ${this.shares.map((s) => html`<option value=${s.id}>${s.name}</option>`)}
              </select>
            </fieldset>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Peer</legend>
              <select name="peer_id" class="select w-full">
                <option value="">All peers</option>
                ${this.peers.map((p) => html`<option value=${p.id}>${p.name}</option>`)}
              </select>
            </fieldset>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">For</legend>
              <select name="minutes" class="select w-full">
                <option value="30">30 minutes</option>
                <option value="60" selected>1 hour</option>
                <option value="240">4 hours</option>
                <option value="720">12 hours</option>
                <option value="1440">24 hours</option>
              </select>
            </fieldset>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Reason</legend>
              <input name="reason" class="input w-full" maxlength="200" placeholder="Import after a trip" />
            </fieldset>
            <div class="modal-action">
              <button type="button" class="btn btn-ghost" @click=${() => this.overrideDialog.value?.close()}>Cancel</button>
              <button class="btn btn-primary" ?disabled=${this.busy !== undefined}>${this.spin('override')}Allow</button>
            </div>
          </form>
        </div>
        <form method="dialog" class="modal-backdrop"><button>close</button></form>
      </dialog>
    `;
  }

  protected override render() {
    return html`
      ${this.frame(
        () => {
          const s = this.status;
          if (!s && this.scheduleInvalid) {
            return html`
              <div role="alert" class="alert alert-error alert-soft">
                The saved schedule is invalid. File transfers are paused unless an override is active.
                Choose replacement hours and save them to resume scheduling.
              </div>
              ${this.renderProgress()}
              ${this.renderTransfers()}
            `;
          }
          if (!s) return html``;
          return html`
            ${this.renderProgress()}
            <div role="status" class=${s.allowed_now || s.override_active ? 'alert alert-success alert-soft' : 'alert alert-info alert-soft'}>
              <ism-icon name=${s.allowed_now || s.override_active ? 'play' : 'pause'}></ism-icon>
              <span>
                ${
                  s.override_active
                    ? `Transfers allowed by an override until ${formatDate(s.override_active.until)}.`
                    : s.allowed_now
                      ? 'Incoming transfers are allowed now.'
                      : `Incoming transfers wait until ${formatDate(s.next_allowed_at)}.`
                }
                Outgoing transfers follow the receiving instance's hours.
              </span>
            </div>
            ${this.renderTransfers()}
          `;
        },
        html`<button type="button" class="btn btn-sm" @click=${() => this.settingsDialog.value?.showModal()}>
          <ism-icon name="settings" size="sm"></ism-icon>Hours & overrides
        </button>`,
      )}
      <dialog class="modal" aria-labelledby="sync-settings-title" ${ref(this.settingsDialog)}>
        <div class="modal-box flex max-w-3xl flex-col gap-5">
          <div class="flex items-center justify-between gap-3">
            <h2 id="sync-settings-title" class="text-lg font-semibold">Sync settings</h2>
            <form method="dialog"><button class="btn btn-ghost btn-sm btn-square" aria-label="Close sync settings"><ism-icon name="x"></ism-icon></button></form>
          </div>
          ${this.renderSchedule()}
          <div class="divider my-0"></div>
          ${this.renderOverrides()}
          <button type="button" class="btn btn-primary btn-sm self-start" @click=${() => this.overrideDialog.value?.showModal()}>
            <ism-icon name="play" size="sm"></ism-icon>Allow transfers now
          </button>
        </div>
        <form method="dialog" class="modal-backdrop"><button>Close</button></form>
      </dialog>
      ${this.renderOverrideDialog()}
      <ism-sync-detail ${ref(this.detail)}></ism-sync-detail>
    `;
  }
}

customElements.define('ism-sync-page', IsmSyncPage);

declare global {
  interface HTMLElementTagNameMap {
    'ism-sync-page': IsmSyncPage;
  }
}
