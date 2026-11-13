import { html, nothing } from 'lit';
import { createRef, ref } from 'lit/directives/ref.js';
import { admin } from '@/api/admin';
import type { Operation } from '@/api/generated/Operation';
import type { OperationStatus } from '@/api/generated/OperationStatus';
import { IsmPage } from '@/base/ism-page';
import { confirmAction } from '@/components/confirm';
import { formatDate, humanize, percent } from '@/util/format';
import { statusBadge } from '@/util/status';

const FILTERS: readonly { value: string; label: string }[] = [
  { value: '', label: 'All' },
  { value: 'running', label: 'Running' },
  { value: 'queued', label: 'Queued' },
  { value: 'waiting_maintenance', label: 'Waiting for maintenance' },
  { value: 'partial', label: 'Partial' },
  { value: 'failed', label: 'Failed' },
  { value: 'recovery_required', label: 'Recovery required' },
  { value: 'completed', label: 'Completed' },
  { value: 'cancelled', label: 'Cancelled' },
];

const CANCELLABLE: readonly OperationStatus[] = ['queued', 'waiting_maintenance', 'running'];

/** Batches submitted by clients and by this UI, with per-item results. */
export class IsmOperationsPage extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    operations: { state: true },
    filter: { state: true },
    detail: { state: true },
  };

  declare operations: Operation[];
  declare filter: string;
  declare detail: Operation | undefined;
  protected override reloadOn = ['operation_updated', 'operation_committed'] as const;
  private dialog = createRef<HTMLDialogElement>();

  constructor() {
    super();
    this.operations = [];
    this.filter = '';
    this.detail = undefined;
  }

  protected override async load() {
    this.operations = await admin.operations(this.filter || undefined);
    const open = this.detail;
    if (open && this.dialog.value?.open) this.detail = await admin.operation(open.operation_id);
  }

  private async show(op: Operation) {
    this.detail = op;
    this.dialog.value?.showModal();
    try {
      this.detail = await admin.operation(op.operation_id);
    } catch {
      // Keep the list copy.
    }
  }

  private async cancel(op: Operation) {
    const ok = await confirmAction({
      title: 'Cancel operation',
      message: 'Items that have not started are skipped. An item already running finishes first.',
      confirmLabel: 'Cancel operation',
      danger: true,
    });
    if (ok)
      await this.act(
        `cancel:${op.operation_id}`,
        () => admin.cancelOperation(op.operation_id),
        'Cancellation requested.',
      );
  }

  private async resolve(op: Operation) {
    const ok = await confirmAction({
      title: 'Mark recovery as done',
      message:
        'Confirm that the files and Immich assets named in the failed items are where they should be. Library scanning and the watcher are restored afterwards.',
      confirmLabel: 'Mark as done',
    });
    if (ok)
      await this.act(
        `resolve:${op.operation_id}`,
        () => admin.resolveOperation(op.operation_id),
        'Recovery marked as done.',
      );
  }

  private actions(op: Operation) {
    return html`
      ${
        CANCELLABLE.includes(op.status)
          ? html`<button type="button" class="btn btn-ghost btn-sm" @click=${() => this.cancel(op)}>
              ${this.spin(`cancel:${op.operation_id}`)}Cancel
            </button>`
          : nothing
      }
      ${
        op.status === 'recovery_required' || op.status === 'partial'
          ? html`<button type="button" class="btn btn-warning btn-sm" @click=${() => this.resolve(op)}>
              ${this.spin(`resolve:${op.operation_id}`)}Resolve
            </button>`
          : nothing
      }
    `;
  }

  private renderDetail() {
    const op = this.detail;
    return html`
      <dialog class="modal" ${ref(this.dialog)} @close=${() => (this.detail = undefined)}>
        <div class="modal-box max-w-4xl">
          ${
            op
              ? html`
                <div class="flex flex-wrap items-center gap-2">
                  <h3 class="text-lg font-semibold">${op.label ?? 'Operation'}</h3>
                  <span class=${statusBadge(op.status)}>${humanize(op.status)}</span>
                </div>
                <p class="text-sm text-base-content/70">
                  ${op.source} · started ${formatDate(op.created_at)} · finished ${formatDate(op.finished_at)}
                </p>
                <progress class="progress progress-primary my-3 w-full" value=${op.progress.done} max=${Math.max(op.progress.total, 1)}></progress>
                <div class="max-h-[60vh] overflow-auto">
                  <table class="table table-sm">
                    <thead><tr><th>#</th><th>Action</th><th>Status</th><th>Result</th></tr></thead>
                    <tbody>
                      ${op.items.map(
                        (item) => html`<tr>
                          <td>${item.index + 1}</td>
                          <td>${humanize(item.kind)}</td>
                          <td><span class=${statusBadge(item.status)}>${humanize(item.status)}</span></td>
                          <td class="break-all text-xs">
                            ${item.error ? html`<span class="text-error"><code>${item.error.code}</code> ${item.error.message}</span>` : nothing}
                            ${item.result !== null && item.result !== undefined ? html`<code>${JSON.stringify(item.result)}</code>` : nothing}
                          </td>
                        </tr>`,
                      )}
                    </tbody>
                  </table>
                </div>
                <div class="modal-action">
                  ${this.actions(op)}
                  <form method="dialog"><button class="btn">Close</button></form>
                </div>
              `
              : nothing
          }
        </div>
        <form method="dialog" class="modal-backdrop"><button>close</button></form>
      </dialog>
    `;
  }

  protected override render() {
    return html`
      ${this.frame(
        () => html`
          <h2 class="sr-only">Operations</h2>
          <div class="overflow-x-auto rounded-box border border-base-300 bg-base-100">
            <table class="table table-sm">
              <thead><tr><th>Operation</th><th>Source</th><th>Status</th><th>Progress</th><th>Started</th><th></th></tr></thead>
              <tbody>
                ${
                  this.operations.length === 0
                    ? html`<tr><td colspan="6" class="text-center text-base-content/60">No operations.</td></tr>`
                    : this.operations.map(
                        (op) => html`<tr class="hover:bg-base-200">
                          <td>
                            <button type="button" class="link link-hover text-left font-medium" @click=${() => this.show(op)}>
                              ${op.label ?? `${op.items.length} action(s)`}
                            </button>
                          </td>
                          <td>${op.source}</td>
                          <td><span class=${statusBadge(op.status)}>${humanize(op.status)}</span></td>
                          <td class="min-w-32">
                            <progress class="progress progress-primary w-24" value=${percent(op.progress.done, op.progress.total)} max="100"></progress>
                            <span class="text-xs">${op.progress.done}/${op.progress.total}</span>
                          </td>
                          <td class="whitespace-nowrap">${formatDate(op.created_at)}</td>
                          <td class="whitespace-nowrap text-right">${this.actions(op)}</td>
                        </tr>`,
                      )
                }
              </tbody>
            </table>
          </div>
        `,
        html`<select
          class="select select-sm w-auto"
          aria-label="Status"
          @change=${(e: Event) => {
            this.filter = (e.target as HTMLSelectElement).value;
            void this.reload();
          }}
        >
          ${FILTERS.map((f) => html`<option value=${f.value} ?selected=${f.value === this.filter}>${f.label}</option>`)}
        </select>`,
      )}
      ${this.renderDetail()}
    `;
  }
}

customElements.define('ism-operations-page', IsmOperationsPage);

declare global {
  interface HTMLElementTagNameMap {
    'ism-operations-page': IsmOperationsPage;
  }
}
