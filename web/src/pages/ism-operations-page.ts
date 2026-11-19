import { html } from 'lit';
import { createRef, ref } from 'lit/directives/ref.js';
import { admin } from '@/api/admin';
import type { OperationActivity } from '@/api/generated/OperationActivity';
import { IsmPage } from '@/base/ism-page';
import type { IsmOperationDetail } from '@/components/ism-operation-detail';
import type { PageChange } from '@/components/ism-pagination';
import { tabKeydown } from '@/util/tabs';
import '@/components/ism-operation-table';
import '@/components/ism-operation-detail';
import '@/components/ism-pagination';

const FILTERS = [
  ['', 'All'],
  ['attention', 'Needs attention'],
  ['running', 'Running'],
  ['queued', 'Queued'],
  ['waiting_maintenance', 'Waiting for maintenance'],
  ['completed', 'Completed'],
  ['partial', 'Partial'],
  ['failed', 'Failed'],
  ['recovery_required', 'Recovery required'],
  ['cancelled', 'Cancelled'],
  ['handled', 'Handled'],
] as const;

export class IsmOperationsPage extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    operations: { state: true },
    filter: { state: true },
    total: { state: true },
    limit: { state: true },
    offset: { state: true },
  };
  declare operations: OperationActivity[];
  declare filter: string;
  declare total: number;
  declare limit: number;
  declare offset: number;
  protected override reloadOn = ['operation_updated', 'operation_committed'] as const;
  private detail = createRef<IsmOperationDetail>();
  constructor() {
    super();
    this.operations = [];
    this.filter = '';
    this.total = 0;
    this.limit = 250;
    this.offset = 0;
  }
  protected override async load() {
    const { filter, limit, offset } = this;
    const page = await admin.operationPage(filter, limit, offset);
    if (filter !== this.filter || limit !== this.limit || offset !== this.offset) return;
    this.operations = page.rows;
    this.total = page.total;
    if (offset > 0 && offset >= page.total) {
      this.offset = Math.max(0, Math.ceil(page.total / limit) - 1) * limit;
      void this.reload();
    }
  }
  protected override render() {
    return html`${this.frame(
      () => html`
      <h2 class="sr-only">Operations</h2>
      <section class="min-w-0 rounded-box border border-base-300 bg-base-100">
        <div role="tablist" aria-label="Operation status" class="tabs tabs-border flex-nowrap overflow-x-auto" @keydown=${tabKeydown}>
          ${FILTERS.map(
            ([value, label]) => html`<button type="button" role="tab"
            id=${`operations-tab-${value || 'all'}`} aria-controls="operations-panel"
            aria-selected=${this.filter === value} tabindex=${this.filter === value ? 0 : -1}
            class=${this.filter === value ? 'tab tab-active whitespace-nowrap' : 'tab whitespace-nowrap'}
            @click=${() => {
              this.filter = value;
              this.offset = 0;
              void this.reload();
            }}>${label}</button>`,
          )}
        </div>
        <div role="tabpanel" id="operations-panel" aria-labelledby=${`operations-tab-${this.filter || 'all'}`}>
          <ism-operation-table .rows=${this.operations}
            @operation-open=${(e: CustomEvent<string>) => this.detail.value?.show(e.detail)}></ism-operation-table>
          <ism-pagination .total=${this.total} .limit=${this.limit} .offset=${this.offset}
            @page-change=${(e: CustomEvent<PageChange>) => {
              this.limit = e.detail.limit;
              this.offset = e.detail.offset;
              void this.reload();
            }}></ism-pagination>
        </div>
      </section>
    `,
    )}<ism-operation-detail ${ref(this.detail)}></ism-operation-detail>`;
  }
}
customElements.define('ism-operations-page', IsmOperationsPage);
declare global {
  interface HTMLElementTagNameMap {
    'ism-operations-page': IsmOperationsPage;
  }
}
