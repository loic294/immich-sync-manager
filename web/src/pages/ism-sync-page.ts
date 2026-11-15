import { html, nothing } from 'lit';
import { createRef, ref } from 'lit/directives/ref.js';
import { admin } from '@/api/admin';
import { isApiError } from '@/api/errors';
import type { OverrideView } from '@/api/generated/OverrideView';
import type { PeerView } from '@/api/generated/PeerView';
import type { ScheduleView } from '@/api/generated/ScheduleView';
import type { ShareView } from '@/api/generated/ShareView';
import type { SyncStatus } from '@/api/generated/SyncStatus';
import type { TransferView } from '@/api/generated/TransferView';
import type { Window } from '@/api/generated/Window';
import { IsmPage } from '@/base/ism-page';
import { formatBytes, formatDate, formatLocation, humanize, percent } from '@/util/format';
import { statusBadge } from '@/util/status';

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
  };

  declare status: SyncStatus | undefined;
  declare schedule: ScheduleView | undefined;
  declare scheduleInvalid: boolean;
  declare overrides: OverrideView[];
  declare shares: ShareView[];
  declare peers: PeerView[];
  protected override reloadOn = ['sync_changed'] as const;
  private overrideDialog = createRef<HTMLDialogElement>();
  private scheduleDirty = false;

  constructor() {
    super();
    this.status = undefined;
    this.schedule = undefined;
    this.scheduleInvalid = false;
    this.overrides = [];
    this.shares = [];
    this.peers = [];
  }

  protected override async load() {
    const [status, schedule, overrides, shares, peers] = await Promise.all([
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

  private renderTransfer(t: TransferView, running: boolean) {
    return html`<tr>
      <td class="break-all">
        <div>${formatLocation(t.location)}</div>
        <div class="text-xs text-base-content/60">${t.share_name} · ${t.peer_name}</div>
        ${t.error ? html`<div class="text-xs text-error">${t.error}</div>` : nothing}
      </td>
      <td class="whitespace-nowrap">${formatBytes(t.size)}</td>
      <td class="min-w-32">
        ${
          running
            ? html`<progress class="progress progress-primary w-24" value=${percent(t.bytes_done, t.size)} max="100"></progress>
                <span class="text-xs">${percent(t.bytes_done, t.size)}%</span>`
            : html`<span class=${statusBadge(t.status)}>${humanize(t.status)}</span>
                ${t.attempts > 0 ? html`<span class="text-xs">${t.attempts} attempt(s)</span>` : nothing}`
        }
      </td>
      <td class="whitespace-nowrap">${formatDate(t.queued_at)}</td>
      <td class="whitespace-nowrap text-right">
        ${
          t.status === 'failed' || t.status === 'cancelled'
            ? html`<button type="button" class="btn btn-ghost btn-sm" @click=${() => this.act(`retry:${t.id}`, () => admin.retryTransfer(t.id))}>
                ${this.spin(`retry:${t.id}`)}<ism-icon name="rotate-ccw" size="sm"></ism-icon>Retry
              </button>`
            : nothing
        }
        ${
          t.status !== 'cancelled'
            ? html`<button type="button" class="btn btn-ghost btn-sm" @click=${() => this.act(`cancel:${t.id}`, () => admin.cancelTransfer(t.id))}>
                ${this.spin(`cancel:${t.id}`)}Cancel
              </button>`
            : nothing
        }
      </td>
    </tr>`;
  }

  private renderTransfers(title: string, rows: TransferView[], running: boolean, empty: string) {
    return html`
      <section class="card card-border bg-base-100">
        <div class="card-body p-4">
          <h2 class="card-title text-base">${title}</h2>
          <div class="max-h-96 overflow-auto">
            <table class="table table-sm table-pin-rows">
              <thead><tr><th>File</th><th>Size</th><th>${running ? 'Progress' : 'Status'}</th><th>Queued</th><th></th></tr></thead>
              <tbody>
                ${
                  rows.length === 0
                    ? html`<tr><td colspan="5" class="text-base-content/60">${empty}</td></tr>`
                    : rows.map((t) => this.renderTransfer(t, running))
                }
              </tbody>
            </table>
          </div>
        </div>
      </section>
    `;
  }

  private renderSchedule() {
    const s = this.schedule;
    if (!s) return nothing;
    return html`
      <section class="card card-border bg-base-100">
        <div class="card-body gap-3 p-4">
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
      <section class="card card-border bg-base-100">
        <div class="card-body p-4">
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
              ${this.renderSchedule()}
            `;
          }
          if (!s) return html``;
          return html`
            <div role="status" class=${s.allowed_now || s.override_active ? 'alert alert-success alert-soft' : 'alert alert-info alert-soft'}>
              <ism-icon name=${s.allowed_now || s.override_active ? 'play' : 'pause'}></ism-icon>
              <span>
                ${
                  s.override_active
                    ? `Transfers allowed by an override until ${formatDate(s.override_active.until)}.`
                    : s.allowed_now
                      ? 'File transfers are allowed now.'
                      : `Outside the allowed hours. New file content waits until ${formatDate(s.next_allowed_at)}.`
                }
                ${s.queued_total} queued, ${s.failed_total} failed.
              </span>
            </div>
            ${this.renderTransfers('Transferring now', s.running, true, 'Nothing is transferring.')}
            ${this.renderTransfers(
              s.queued_total + s.failed_total > s.queued.length
                ? `Queue (first ${s.queued.length})`
                : 'Queue',
              s.queued,
              false,
              'The queue is empty.',
            )}
            ${this.renderSchedule()} ${this.renderOverrides()}
          `;
        },
        html`<button type="button" class="btn btn-primary btn-sm" @click=${() => this.overrideDialog.value?.showModal()}>
          <ism-icon name="play" size="sm"></ism-icon>Allow transfers now
        </button>`,
      )}
      ${this.renderOverrideDialog()}
    `;
  }
}

customElements.define('ism-sync-page', IsmSyncPage);

declare global {
  interface HTMLElementTagNameMap {
    'ism-sync-page': IsmSyncPage;
  }
}
