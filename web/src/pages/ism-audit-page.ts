import { html } from 'lit';
import { admin } from '@/api/admin';
import type { AuditEntry } from '@/api/generated/AuditEntry';
import { IsmPage } from '@/base/ism-page';
import { formatDate } from '@/util/format';

/** Who changed what, newest first, 200 at a time. */
export class IsmAuditPage extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    entries: { state: true },
    more: { state: true },
  };

  declare entries: AuditEntry[];
  declare more: boolean;

  constructor() {
    super();
    this.entries = [];
    this.more = false;
  }

  protected override async load() {
    const entries = await admin.audit();
    this.entries = entries;
    this.more = entries.length >= 200;
  }

  private async loadMore() {
    const last = this.entries.at(-1);
    if (!last) return;
    await this.act('more', async () => {
      const next = await admin.audit(last.id);
      this.entries = [...this.entries, ...next];
      this.more = next.length >= 200;
    });
  }

  protected override render() {
    return this.frame(
      () => html`
        <h2 class="sr-only">Audit entries</h2>
        <div class="overflow-x-auto rounded-box border border-base-300 bg-base-100">
          <table class="table table-sm">
            <thead><tr><th>When</th><th>Actor</th><th>Action</th><th>Detail</th></tr></thead>
            <tbody>
              ${
                this.entries.length === 0
                  ? html`<tr><td colspan="4" class="text-center text-base-content/60">No entries yet.</td></tr>`
                  : this.entries.map(
                      (e) => html`<tr>
                        <td class="whitespace-nowrap">${formatDate(e.at)}</td>
                        <td>${e.actor}</td>
                        <td><code>${e.action}</code></td>
                        <td class="break-all">${e.detail}</td>
                      </tr>`,
                    )
              }
            </tbody>
          </table>
        </div>
        ${
          this.more
            ? html`<button type="button" class="btn btn-sm self-center" @click=${this.loadMore}>${this.spin('more')}Load more</button>`
            : ''
        }
      `,
    );
  }
}

customElements.define('ism-audit-page', IsmAuditPage);

declare global {
  interface HTMLElementTagNameMap {
    'ism-audit-page': IsmAuditPage;
  }
}
