import { html } from 'lit';
import { admin } from '@/api/admin';
import type { MappingView } from '@/api/generated/MappingView';
import type { TrashEntry } from '@/api/generated/TrashEntry';
import { IsmPage } from '@/base/ism-page';
import { confirmAction } from '@/components/confirm';
import { formatBytes, formatDate, formatLocation } from '@/util/format';

/** Files and folders moved to the recoverable trash. Restore puts them back; purge is final. */
export class IsmTrashPage extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    entries: { state: true },
    mappings: { state: true },
    rootId: { state: true },
  };

  declare entries: TrashEntry[];
  declare mappings: MappingView[];
  declare rootId: string;

  constructor() {
    super();
    this.entries = [];
    this.mappings = [];
    this.rootId = '';
  }

  protected override async load() {
    [this.entries, this.mappings] = await Promise.all([
      this.data.trash(this.rootId || undefined),
      this.data.mappings(),
    ]);
  }

  private async restore(entry: TrashEntry) {
    await this.act(
      `restore:${entry.trash_id}`,
      () => admin.submit('Restore from trash', [{ kind: 'restore', trash_id: entry.trash_id }]),
      'Restore started. See Operations for progress.',
    );
  }

  private async purge(entry: TrashEntry) {
    const ok = await confirmAction({
      title: 'Delete permanently',
      message: `${formatLocation(entry.location)} is deleted from disk${entry.asset_ids.length > 0 ? ' and from Immich' : ''}. This cannot be undone.`,
      confirmLabel: 'Delete permanently',
      danger: true,
    });
    if (!ok) return;
    await this.act(
      `purge:${entry.trash_id}`,
      () => admin.submit('Purge from trash', [{ kind: 'purge', trash_id: entry.trash_id }]),
      'Purge started. See Operations for progress.',
    );
  }

  private onRoot(event: Event) {
    this.rootId = (event.target as HTMLSelectElement).value;
    void this.reload();
  }

  protected override render() {
    return this.frame(
      () => html`
        <h2 class="sr-only">Trashed items</h2>
        <div class="overflow-x-auto rounded-box border border-base-300 bg-base-100">
          <table class="table table-sm">
            <thead><tr><th>Item</th><th>Size</th><th>Assets</th><th>Trashed</th><th></th></tr></thead>
            <tbody>
              ${
                this.entries.length === 0
                  ? html`<tr><td colspan="5" class="text-center text-base-content/60">The trash is empty.</td></tr>`
                  : this.entries.map(
                      (e) => html`<tr>
                        <td class="break-all">
                          <ism-icon name=${e.is_folder ? 'folder' : 'file'} size="sm"></ism-icon>
                          ${formatLocation(e.location)}
                        </td>
                        <td class="whitespace-nowrap">${formatBytes(e.size)}</td>
                        <td>${e.asset_ids.length}</td>
                        <td class="whitespace-nowrap">${formatDate(e.trashed_at)}</td>
                        <td class="whitespace-nowrap text-right">
                          <button type="button" class="btn btn-ghost btn-sm" ?disabled=${this.busy !== undefined} @click=${() => this.restore(e)}>
                            ${this.spin(`restore:${e.trash_id}`)}<ism-icon name="undo-2" size="sm"></ism-icon>Restore
                          </button>
                          <button type="button" class="btn btn-ghost btn-sm text-error" ?disabled=${this.busy !== undefined} @click=${() => this.purge(e)}>
                            ${this.spin(`purge:${e.trash_id}`)}<ism-icon name="trash-2" size="sm"></ism-icon>Delete
                          </button>
                        </td>
                      </tr>`,
                    )
              }
            </tbody>
          </table>
        </div>
      `,
      html`<select class="select select-sm w-auto" aria-label="Folder" @change=${this.onRoot}>
        <option value="">All folders</option>
        ${this.mappings.map((m) => html`<option value=${m.root_id} ?selected=${m.root_id === this.rootId}>${m.name}</option>`)}
      </select>`,
    );
  }
}

customElements.define('ism-trash-page', IsmTrashPage);

declare global {
  interface HTMLElementTagNameMap {
    'ism-trash-page': IsmTrashPage;
  }
}
