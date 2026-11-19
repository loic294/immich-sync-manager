import { html, nothing } from 'lit';
import { createRef, ref } from 'lit/directives/ref.js';
import { admin } from '@/api/admin';
import type { SyncActivity } from '@/api/generated/SyncActivity';
import type { SyncFileDetails } from '@/api/generated/SyncFileDetails';
import { IsmPage } from '@/base/ism-page';
import type { IsmOperationDetail } from '@/components/ism-operation-detail';
import { formatBytes, formatDate, humanize, percent } from '@/util/format';
import { statusBadge } from '@/util/status';
import '@/components/ism-operation-detail';

export class IsmSyncDetail extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    transfer: { state: true },
    files: { state: true },
    fileError: { state: true },
  };
  declare transfer: SyncActivity | undefined;
  declare files: SyncFileDetails | undefined;
  declare fileError: string | undefined;
  protected override reloadOn = [
    'sync_changed',
    'operation_updated',
    'operation_committed',
  ] as const;
  private dialog = createRef<HTMLDialogElement>();
  private operation = createRef<IsmOperationDetail>();
  private selectedId: string | undefined;
  async show(transfer: SyncActivity) {
    this.selectedId = transfer.id;
    this.transfer = transfer;
    this.files = undefined;
    this.fileError = undefined;
    this.loadError = undefined;
    await this.updateComplete;
    this.dialog.value?.showModal();
    void this.reload();
    try {
      const files = await admin.syncFiles(
        transfer.share_name,
        transfer.relative_path,
        transfer.peer_id,
      );
      if (this.selectedId === transfer.id) this.files = files;
    } catch (error) {
      if (this.selectedId === transfer.id)
        this.fileError = error instanceof Error ? error.message : String(error);
    }
  }
  protected override async load() {
    const id = this.selectedId;
    if (!id) return;
    const page = await admin.syncTransfer(id);
    if (id !== this.selectedId) return;
    const transfer = page.rows[0];
    if (!transfer)
      throw new Error(
        'This transfer is no longer available. It may have been superseded by a newer version.',
      );
    this.transfer = transfer;
  }
  protected override render() {
    const t = this.transfer;
    return html`<dialog class="modal" aria-labelledby="sync-detail-title" ${ref(this.dialog)}
      @close=${() => {
        this.selectedId = undefined;
      }}>
      <div class="modal-box max-w-4xl">
        <div class="flex items-start justify-between gap-3">
          <h2 id="sync-detail-title" class="break-all text-lg font-semibold">${t?.relative_path.split('/').at(-1) ?? 'Sync details'}</h2>
          <form method="dialog"><button class="btn btn-ghost btn-sm btn-square" aria-label="Close sync details"><ism-icon name="x"></ism-icon></button></form>
        </div>
        ${this.loadError ? html`<p role="alert" class="alert alert-error alert-soft">${this.loadError}</p>` : nothing}
        ${
          t
            ? html`
          <p class="my-3">${t.source} → ${t.destination}</p>
          <div class="my-2 flex flex-wrap items-center gap-2"><span class=${statusBadge(t.status)}>${humanize(t.status)}</span>
            <span class="text-sm">${formatBytes(t.bytes_done)} / ${formatBytes(t.size)}</span></div>
          <progress class="progress progress-primary w-full" aria-label="Transfer progress" value=${percent(t.bytes_done, t.size)} max="100"></progress>
          <dl class="my-4 grid grid-cols-1 gap-2 text-sm sm:grid-cols-[auto_1fr]">
            <dt class="font-medium">Source folder</dt><dd class="break-all">${this.files?.folder ?? `Share ${t.share_name}/${t.relative_path.split('/').slice(0, -1).join('/')}`}</dd>
            <dt class="font-medium">Destination folder</dt><dd class="break-all">${t.destination_path.split('/').slice(0, -1).join('/')}</dd>
            <dt class="font-medium">Destination file</dt><dd class="break-all">${t.destination_path}</dd>
            <dt class="font-medium">Queued</dt><dd>${formatDate(t.queued_at)}</dd>
            <dt class="font-medium">Finished</dt><dd>${formatDate(t.finished_at)}</dd>
          </dl>
          ${t.error ? html`<p role="alert" class="alert alert-error alert-soft">${t.error}</p>` : nothing}
          <h3 class="font-semibold">Original & related sidecars</h3>
          ${this.fileError ? html`<p role="alert" class="my-2 text-sm text-warning">Cannot verify the source folder and related files: ${this.fileError}</p>` : nothing}
          ${
            this.files
              ? html`<ul class="my-2 space-y-2">${this.files.files.map(
                  (file) => html`<li class="break-all text-sm">
            <span class="badge badge-ghost badge-sm">${humanize(file.role)}</span> ${file.path}
          </li>`,
                )}</ul><p class="text-xs text-base-content/70">Related files are shown for context. Each queued file is transferred separately.</p>`
              : !this.fileError
                ? html`<div class="skeleton my-3 h-16 w-full" aria-label="Loading related files"></div>`
                : nothing
          }
          <div class="modal-action flex-wrap">
            ${t.operation_id ? html`<button class="btn btn-sm" @click=${() => this.operation.value?.show(t.operation_id ?? '')}>View publishing operation</button>` : nothing}
            ${
              !t.operation_id && ['failed', 'cancelled'].includes(t.status)
                ? html`<button class="btn btn-primary btn-sm" ?disabled=${!!this.busy}
              @click=${() => this.act('retry', () => admin.retryTransfer(t.id), 'Transfer queued again.')}>${this.spin('retry')}Retry</button>`
                : nothing
            }
            ${
              !t.operation_id && ['queued', 'running', 'failed'].includes(t.status)
                ? html`<button class="btn btn-sm" ?disabled=${!!this.busy}
              @click=${() => this.act('cancel', () => admin.cancelTransfer(t.id), 'Transfer cancelled.')}>${this.spin('cancel')}Cancel transfer</button>`
                : nothing
            }
          </div>
        `
            : nothing
        }
      </div>
      <form method="dialog" class="modal-backdrop"><button>Close</button></form>
    </dialog><ism-operation-detail ${ref(this.operation)}></ism-operation-detail>`;
  }
}
customElements.define('ism-sync-detail', IsmSyncDetail);
declare global {
  interface HTMLElementTagNameMap {
    'ism-sync-detail': IsmSyncDetail;
  }
}
