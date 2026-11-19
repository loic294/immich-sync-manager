import { html } from 'lit';
import type { OperationActivity } from '@/api/generated/OperationActivity';
import { IsmElement } from '@/base/ism-element';
import { formatDate, humanize, percent } from '@/util/format';
import { statusBadge } from '@/util/status';

export class IsmOperationTable extends IsmElement {
  static override properties = { rows: { attribute: false } };
  declare rows: OperationActivity[];
  constructor() {
    super();
    this.rows = [];
  }
  protected override render() {
    return html`<div class="overflow-x-auto">
      <table class="table table-sm">
        <thead><tr><th>Operation</th><th>Source</th><th>New destination</th><th>Status</th><th>Progress</th><th>Started</th></tr></thead>
        <tbody>${
          this.rows.length
            ? this.rows.map(
                (op) => html`<tr class="hover:bg-base-200">
          <td><button type="button" class="link link-hover text-left font-medium"
            aria-label=${`Open ${op.title} operation ${op.label ?? op.operation_id}`}
            @click=${() =>
              this.dispatchEvent(
                new CustomEvent('operation-open', {
                  detail: op.operation_id,
                  bubbles: true,
                  composed: true,
                }),
              )}>${op.title}</button></td>
          <td class="break-words">${op.source}</td>
          <td class="break-words">${op.destination}</td>
          <td><span class=${statusBadge(op.status)}>${humanize(op.status)}</span>
            ${op.handled_at ? html`<span class="badge badge-ghost badge-sm">Handled</span>` : ''}</td>
          <td class="min-w-32"><progress class="progress progress-primary w-24"
            aria-label="Operation progress" value=${percent(op.progress.done, op.progress.total)} max="100"></progress>
            <span class="whitespace-nowrap text-xs">${op.progress.done}/${op.progress.total}</span></td>
          <td class="whitespace-nowrap">${formatDate(op.created_at)}</td>
        </tr>`,
              )
            : html`<tr><td colspan="6" class="p-6 text-center text-base-content/70">No operations in this view.</td></tr>`
        }</tbody>
      </table>
    </div>`;
  }
}
customElements.define('ism-operation-table', IsmOperationTable);
declare global {
  interface HTMLElementTagNameMap {
    'ism-operation-table': IsmOperationTable;
  }
}
