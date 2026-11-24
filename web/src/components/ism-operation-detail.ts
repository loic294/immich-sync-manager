import { html, nothing } from 'lit';
import { createRef, ref } from 'lit/directives/ref.js';
import { admin } from '@/api/admin';
import type { OperationDetail } from '@/api/generated/OperationDetail';
import { IsmPage } from '@/base/ism-page';
import { confirmAction } from '@/components/confirm';
import type { PageChange } from '@/components/ism-pagination';
import { formatDate, formatLocation, humanize } from '@/util/format';
import { statusBadge } from '@/util/status';
import '@/components/ism-pagination';

export class IsmOperationDetail extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    detail: { state: true },
    limit: { state: true },
    offset: { state: true },
  };
  declare detail: OperationDetail | undefined;
  declare limit: number;
  declare offset: number;
  private operationId: string | undefined;
  private dialog = createRef<HTMLDialogElement>();
  constructor() {
    super();
    this.limit = 250;
    this.offset = 0;
  }

  async show(id: string) {
    this.operationId = id;
    this.offset = 0;
    this.detail = undefined;
    this.loadError = undefined;
    this.loading = true;
    await this.updateComplete;
    this.dialog.value?.showModal();
    await this.reload();
  }
  protected override async load() {
    const id = this.operationId;
    if (!id) return;
    const detail = await this.data.operationDetail(id);
    if (this.operationId === id) this.detail = detail;
  }
  private async fix() {
    const id = this.operationId;
    if (!id) return;
    await this.act(
      'fix',
      () =>
        admin.fixIssues({
          targets: [{ kind: 'operation', id, peer_id: null }],
          all: false,
          resolution: null,
        }),
      'Recovery queued.',
    );
  }
  private async cancel() {
    const id = this.operationId;
    if (!id) return;
    if (
      await confirmAction({
        title: 'Cancel operation',
        message: 'Items that have not started are skipped. A running item finishes safely first.',
        confirmLabel: 'Cancel operation',
        danger: true,
      })
    )
      await this.act('cancel', () => admin.cancelOperation(id), 'Cancellation requested.');
  }
  protected override render() {
    const detail = this.detail;
    const op = detail?.operation;
    const activity = detail?.activity;
    const canFix =
      detail?.recovery?.can_fix ??
      ['failed', 'partial', 'recovery_required'].includes(op?.status ?? '');
    return html`<dialog class="modal" aria-labelledby="operation-detail-title" ${ref(this.dialog)}
      @close=${() => {
        this.operationId = undefined;
        void this.reload();
      }}>
      <div class="modal-box max-w-5xl">
        <div class="flex items-start justify-between gap-3">
          <h2 id="operation-detail-title" class="text-lg font-semibold">${activity?.title ?? 'Operation details'}</h2>
          <form method="dialog"><button class="btn btn-ghost btn-sm btn-square" aria-label="Close operation details"><ism-icon name="x"></ism-icon></button></form>
        </div>
        ${this.loadError ? html`<div role="alert" class="alert alert-error alert-soft">${this.loadError}<button class="btn btn-sm" @click=${() => this.reload()}>Retry</button></div>` : nothing}
        ${this.loading ? html`<div class="skeleton my-4 h-32 w-full" aria-label="Loading operation"></div>` : nothing}
        ${
          op && activity
            ? html`
          <p class="my-2 break-all text-sm">${op.label ?? op.operation_id}</p>
          <p class="text-sm">${activity.source} → ${activity.destination}</p>
          <div class="my-3 flex flex-wrap items-center gap-2">
            <span class=${statusBadge(op.status)}>${humanize(op.status)}</span>
            <span>${op.progress.done}/${op.progress.total} actions finished</span>
            ${activity.handled_at ? html`<span class="badge badge-ghost">Handled ${formatDate(activity.handled_at)}</span>` : nothing}
          </div>
          <progress aria-label="Overall progress" class="progress progress-primary w-full" value=${op.progress.done} max=${Math.max(1, op.progress.total)}></progress>
          <p class="my-2 text-xs text-base-content/70">Started ${formatDate(op.created_at)} · Finished ${formatDate(op.finished_at)}</p>
          ${op.status === 'waiting_maintenance' ? html`<p role="status" class="alert alert-info alert-soft">Waiting for the Immich maintenance barrier before changing files.</p>` : nothing}
          ${detail.recovery ? html`<p role="status" class="my-3 text-sm">${detail.recovery.state === 'not_retryable' ? 'Not retryable' : detail.recovery.state === 'needs_decision' ? 'Choose a version in Sync → Needs attention' : humanize(detail.recovery.state)}</p>` : nothing}
          <div class="my-3 max-h-[50vh] overflow-y-auto">
            ${op.items.slice(this.offset, this.offset + this.limit).map((item) => {
              const task = detail.tasks[item.index];
              return html`<section class="border-b border-base-300 py-3">
                <h3 class="flex flex-wrap items-center gap-2 font-medium">
                  ${item.index + 1}. ${humanize(item.kind)}
                  <span class=${statusBadge(item.status)}>${humanize(item.status)}</span>
                </h3>
                ${task?.files.map(
                  (file) => html`<div class="my-1 break-all text-xs">
                  <span class="badge badge-ghost badge-xs">${humanize(file.role)}</span>
                  ${file.from ? formatLocation(file.from) : 'New file'} → ${file.to ? formatLocation(file.to) : 'Removed'}
                  ${file.copy ? '(copy)' : ''}
                </div>`,
                )}
                ${
                  task?.steps.length
                    ? html`<ol class="my-2 space-y-1">
                  ${task.steps.map(
                    (step) => html`<li class="flex items-start gap-2 text-sm">
                    ${
                      step.status === 'running'
                        ? html`<span class="loading loading-spinner loading-xs mt-1" aria-label="Running"></span>`
                        : html`<ism-icon class=${step.status === 'done' ? 'text-success' : 'text-base-content/70'} name=${step.status === 'done' ? 'circle-check' : step.status === 'failed' ? 'circle-x' : 'clock'} size="sm"></ism-icon>`
                    }
                    <span class="min-w-0 break-all">${step.label} <span class="text-base-content/70">· ${humanize(step.status)}</span></span>
                  </li>`,
                  )}
                </ol>`
                    : html`<p class="text-sm text-base-content/70">${item.status === 'running' ? 'Planning and verifying this action…' : item.status === 'pending' ? 'Waiting for earlier actions.' : 'No step history recorded for this action.'}</p>`
                }
                ${item.error ? html`<details class="mt-2 text-xs"><summary class="cursor-pointer">Technical details</summary><p class="mt-1 break-all">${item.error.code}: ${item.error.message}</p></details>` : nothing}
                ${item.result ? html`<details class="mt-2 text-xs"><summary class="cursor-pointer">Result</summary><pre class="overflow-x-auto whitespace-pre-wrap break-all">${JSON.stringify(item.result, null, 2)}</pre></details>` : nothing}
              </section>`;
            })}
          </div>
          <ism-pagination .total=${op.items.length} .limit=${this.limit} .offset=${this.offset}
            @page-change=${(e: CustomEvent<PageChange>) => {
              this.limit = e.detail.limit;
              this.offset = e.detail.offset;
            }}></ism-pagination>
          <div class="modal-action flex-wrap">
            ${['queued', 'running', 'waiting_maintenance'].includes(op.status) ? html`<button class="btn btn-sm" ?disabled=${!!this.busy} @click=${this.cancel}>${this.spin('cancel')}Cancel operation</button>` : nothing}
            ${
              canFix
                ? html`<button class="btn btn-primary btn-sm" ?disabled=${!!this.busy} @click=${this.fix}>${this.spin('fix')}Fix</button>`
                : nothing
            }
            ${
              detail.recovery?.can_dismiss && !activity.handled_at
                ? html`<button class="btn btn-primary btn-sm" ?disabled=${!!this.busy}
                @click=${() => this.act('handled', () => admin.markOperationHandled(op.operation_id), 'Operation marked handled.')}>${this.spin('handled')}Mark handled</button>`
                : nothing
            }
          </div>
        `
            : nothing
        }
      </div>
      <form method="dialog" class="modal-backdrop"><button>Close</button></form>
    </dialog>`;
  }
}
customElements.define('ism-operation-detail', IsmOperationDetail);
declare global {
  interface HTMLElementTagNameMap {
    'ism-operation-detail': IsmOperationDetail;
  }
}
