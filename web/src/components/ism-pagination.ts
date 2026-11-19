import { html } from 'lit';
import { IsmElement } from '@/base/ism-element';

export interface PageChange {
  limit: number;
  offset: number;
}
export const PAGE_SIZES = [25, 50, 100, 250, 500, 1000, 2500, 5000] as const;

export class IsmPagination extends IsmElement {
  static override properties = {
    total: { type: Number },
    limit: { type: Number },
    offset: { type: Number },
  };
  declare total: number;
  declare limit: number;
  declare offset: number;
  constructor() {
    super();
    this.total = 0;
    this.limit = 250;
    this.offset = 0;
  }
  protected override updated() {
    const select = this.querySelector('select');
    if (select) select.value = String(this.limit);
  }
  private change(offset: number, limit = this.limit) {
    this.dispatchEvent(
      new CustomEvent<PageChange>('page-change', {
        detail: { limit, offset },
        bubbles: true,
        composed: true,
      }),
    );
  }
  protected override render() {
    const last = Math.max(0, Math.ceil(this.total / this.limit) - 1) * this.limit;
    return html`<nav aria-label="Pagination" class="flex flex-wrap items-center justify-between gap-3 p-3 text-sm">
      <label class="flex items-center gap-2">Rows per page
        <select class="select select-sm w-auto" aria-label="Rows per page"
          @change=${(e: Event) => this.change(0, Number((e.target as HTMLSelectElement).value))}>
          ${PAGE_SIZES.map((n) => html`<option value=${n} ?selected=${n === this.limit}>${n.toLocaleString()}</option>`)}
        </select>
      </label>
      <span role="status">${this.total ? this.offset + 1 : 0}–${Math.min(this.offset + this.limit, this.total)} of ${this.total.toLocaleString()}</span>
      <div class="join">
        <button type="button" class="btn btn-sm join-item" aria-label="First page" ?disabled=${this.offset === 0} @click=${() => this.change(0)}>First</button>
        <button type="button" class="btn btn-sm join-item" aria-label="Previous page" ?disabled=${this.offset === 0} @click=${() => this.change(Math.max(0, this.offset - this.limit))}>Previous</button>
        <button type="button" class="btn btn-sm join-item" aria-label="Next page" ?disabled=${this.offset >= last} @click=${() => this.change(this.offset + this.limit)}>Next</button>
        <button type="button" class="btn btn-sm join-item" aria-label="Last page" ?disabled=${this.offset >= last} @click=${() => this.change(last)}>Last</button>
      </div>
    </nav>`;
  }
}
customElements.define('ism-pagination', IsmPagination);
declare global {
  interface HTMLElementTagNameMap {
    'ism-pagination': IsmPagination;
  }
}
