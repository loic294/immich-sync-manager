import { html, nothing } from 'lit';
import { createRef, ref } from 'lit/directives/ref.js';
import { admin } from '@/api/admin';
import type { ImmichServerView } from '@/api/generated/ImmichServerView';
import type { LibraryView } from '@/api/generated/LibraryView';
import type { MappingInput } from '@/api/generated/MappingInput';
import type { MappingView } from '@/api/generated/MappingView';
import type { MountView } from '@/api/generated/MountView';
import { IsmPage } from '@/base/ism-page';
import { confirmAction } from '@/components/confirm';

const ROOT_ID = /^[a-z0-9_-]{1,64}$/;

/**
 * Folder mappings tie a folder mounted into this container to a stable root ID used by clients,
 * to the Immich external library and import path that sees the same files, and to the prefix
 * Open Photo Manager uses for them.
 */
export class IsmMappingsPage extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    mappings: { state: true },
    mounts: { state: true },
    servers: { state: true },
    libraries: { state: true },
    editing: { state: true },
    serverId: { state: true },
    libraryId: { state: true },
    containerPath: { state: true },
  };

  declare mappings: MappingView[];
  declare mounts: MountView[];
  declare servers: ImmichServerView[];
  declare libraries: LibraryView[];
  declare editing: MappingView | null;
  declare serverId: string;
  declare libraryId: string;
  declare containerPath: string;
  private dialog = createRef<HTMLDialogElement>();

  constructor() {
    super();
    this.mappings = [];
    this.mounts = [];
    this.servers = [];
    this.libraries = [];
    this.editing = null;
    this.serverId = '';
    this.libraryId = '';
    this.containerPath = '';
  }

  protected override async load() {
    [this.mappings, this.mounts, this.servers, this.libraries] = await Promise.all([
      admin.mappings(),
      admin.mounts(),
      admin.servers(),
      admin.libraries(),
    ]);
  }

  private open(mapping: MappingView | null, mountPath = '') {
    this.editing = mapping;
    this.serverId = mapping?.server_id ?? '';
    this.libraryId = mapping?.library_id ?? '';
    this.containerPath = mapping?.container_path ?? mountPath;
    this.dialog.value?.querySelector('form')?.reset();
    this.dialog.value?.showModal();
  }

  private async onSubmit(event: SubmitEvent) {
    event.preventDefault();
    const form = new FormData(event.target as HTMLFormElement);
    const text = (key: string) => String(form.get(key) ?? '').trim();
    const optional = (key: string) => text(key) || null;
    const body: MappingInput = {
      root_id: this.editing?.root_id ?? text('root_id'),
      name: text('name'),
      container_path: text('container_path'),
      server_id: optional('server_id'),
      library_id: optional('library_id'),
      immich_import_path: optional('immich_import_path'),
      opm_prefix: optional('opm_prefix'),
    };
    const editing = this.editing;
    const ok = await this.act(
      'save',
      () => (editing ? admin.updateMapping(editing.root_id, body) : admin.createMapping(body)),
      editing ? 'Mapping updated.' : 'Mapping added.',
    );
    if (ok) this.dialog.value?.close();
  }

  private async removeMapping(mapping: MappingView) {
    const ok = await confirmAction({
      title: 'Remove mapping',
      message: `Clients lose access to “${mapping.name}” (${mapping.root_id}). Files are not touched.`,
      confirmLabel: 'Remove',
      danger: true,
    });
    if (ok)
      await this.act(
        `delete:${mapping.root_id}`,
        () => admin.deleteMapping(mapping.root_id),
        'Mapping removed.',
      );
  }

  private renderDialog() {
    const m = this.editing;
    const libraries = this.libraries.filter((l) => l.server_id === this.serverId);
    const library = libraries.find((l) => l.id === this.libraryId);
    return html`
      <dialog class="modal" ${ref(this.dialog)}>
        <div class="modal-box max-w-xl">
          <h3 class="text-lg font-semibold">${m ? `Edit ${m.name}` : 'Add folder mapping'}</h3>
          <form class="flex flex-col gap-1" @submit=${this.onSubmit}>
            <div class="grid grid-cols-1 gap-x-3 sm:grid-cols-2">
              <fieldset class="fieldset">
                <legend class="fieldset-legend">Root ID</legend>
                <input
                  name="root_id"
                  class="input w-full font-mono"
                  required
                  pattern=${ROOT_ID.source}
                  .value=${m?.root_id ?? ''}
                  ?disabled=${m !== null}
                  placeholder="photos"
                />
                <p class="label">Lowercase letters, digits, dashes and underscores. Cannot change later.</p>
              </fieldset>
              <fieldset class="fieldset">
                <legend class="fieldset-legend">Name</legend>
                <input name="name" class="input w-full" required maxlength="64" .value=${m?.name ?? ''} placeholder="Photos" />
              </fieldset>
            </div>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Folder in this container</legend>
              <input
                name="container_path"
                class="input w-full font-mono"
                required
                list="ism-mounts"
                .value=${this.containerPath}
                @input=${(e: Event) => (this.containerPath = (e.target as HTMLInputElement).value)}
                placeholder="/data/photos"
              />
              <datalist id="ism-mounts">${this.mounts.map((mount) => html`<option value=${mount.path}></option>`)}</datalist>
              <p class="label">
                The folder must contain a file named <code>.ism-root</code> whose content is the root ID, so a
                missing mount is never mistaken for an empty folder.
              </p>
            </fieldset>
            <div class="grid grid-cols-1 gap-x-3 sm:grid-cols-2">
              <fieldset class="fieldset">
                <legend class="fieldset-legend">Immich server</legend>
                <select
                  name="server_id"
                  class="select w-full"
                  @change=${(e: Event) => {
                    this.serverId = (e.target as HTMLSelectElement).value;
                    this.libraryId = '';
                  }}
                >
                  <option value="" ?selected=${this.serverId === ''}>None (files only)</option>
                  ${this.servers.map((s) => html`<option value=${s.id} ?selected=${s.id === this.serverId}>${s.name}</option>`)}
                </select>
              </fieldset>
              <fieldset class="fieldset">
                <legend class="fieldset-legend">External library</legend>
                <select
                  name="library_id"
                  class="select w-full"
                  ?disabled=${this.serverId === ''}
                  @change=${(e: Event) => (this.libraryId = (e.target as HTMLSelectElement).value)}
                >
                  <option value="" ?selected=${this.libraryId === ''}>None</option>
                  ${libraries.map((l) => html`<option value=${l.id} ?selected=${l.id === this.libraryId}>${l.name}</option>`)}
                </select>
              </fieldset>
            </div>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Path inside Immich</legend>
              <input
                name="immich_import_path"
                class="input w-full font-mono"
                list="ism-import-paths"
                .value=${m?.immich_import_path ?? ''}
                ?disabled=${this.libraryId === ''}
                placeholder="/mnt/photos"
              />
              <datalist id="ism-import-paths">${library?.import_paths.map((p) => html`<option value=${p}></option>`) ?? nothing}</datalist>
              <p class="label">How the Immich server sees this same folder (one of the library's import paths, or a folder inside one).</p>
            </fieldset>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Open Photo Manager path (optional)</legend>
              <input name="opm_prefix" class="input w-full font-mono" .value=${m?.opm_prefix ?? ''} placeholder="photos" />
              <p class="label">The volume or folder Open Photo Manager uses for these files.</p>
            </fieldset>
            <div class="modal-action">
              <button type="button" class="btn btn-ghost" @click=${() => this.dialog.value?.close()}>Cancel</button>
              <button class="btn btn-primary" ?disabled=${this.busy !== undefined}>${this.spin('save')}Save</button>
            </div>
          </form>
        </div>
        <form method="dialog" class="modal-backdrop"><button>close</button></form>
      </dialog>
    `;
  }

  protected override render() {
    const serverName = (id: string | null) => this.servers.find((s) => s.id === id)?.name ?? '—';
    const libraryName = (id: string | null) => this.libraries.find((l) => l.id === id)?.name ?? '—';
    return html`
      ${this.frame(
        () => html`
          <section class="card card-border bg-base-100">
            <div class="card-body p-4">
              <h2 class="card-title text-base">Mappings</h2>
              <div class="overflow-x-auto">
                <table class="table table-sm">
                  <thead><tr><th>Root</th><th>Container folder</th><th>Immich</th><th>Open Photo Manager</th><th>Status</th><th></th></tr></thead>
                  <tbody>
                    ${
                      this.mappings.length === 0
                        ? html`<tr><td colspan="6" class="text-base-content/60">No mapping yet. Pick a mounted folder below.</td></tr>`
                        : this.mappings.map(
                            (m) => html`<tr>
                              <td><div class="font-medium">${m.name}</div><code class="text-xs">${m.root_id}</code></td>
                              <td class="break-all font-mono text-xs">${m.container_path}</td>
                              <td class="text-sm">
                                ${m.server_id ? html`${serverName(m.server_id)} · ${libraryName(m.library_id)}<div class="break-all font-mono text-xs">${m.immich_import_path ?? ''}</div>` : '—'}
                              </td>
                              <td class="font-mono text-xs">${m.opm_prefix ?? '—'}</td>
                              <td>
                                ${
                                  m.available
                                    ? html`<span class="badge badge-success badge-soft">Available</span>`
                                    : html`<span class="badge badge-error badge-soft">Unavailable</span>
                                        <div class="text-xs text-error">${m.availability_detail ?? ''}</div>`
                                }
                              </td>
                              <td class="whitespace-nowrap text-right">
                                <button type="button" class="btn btn-ghost btn-sm btn-square" aria-label="Edit" @click=${() => this.open(m)}>
                                  <ism-icon name="pencil" size="sm"></ism-icon>
                                </button>
                                <button type="button" class="btn btn-ghost btn-sm btn-square text-error" aria-label="Remove" @click=${() => this.removeMapping(m)}>
                                  <ism-icon name="trash-2" size="sm"></ism-icon>
                                </button>
                              </td>
                            </tr>`,
                          )
                    }
                  </tbody>
                </table>
              </div>
            </div>
          </section>
          <section class="card card-border bg-base-100">
            <div class="card-body p-4">
              <h2 class="card-title text-base">Mounted folders</h2>
              <p class="text-sm text-base-content/70">Folders mounted into this container under the mounts directory (/mnt by default).</p>
              <div class="overflow-x-auto">
                <table class="table table-sm">
                  <thead><tr><th>Folder</th><th>Access</th><th>Mapping</th><th></th></tr></thead>
                  <tbody>
                    ${
                      this.mounts.length === 0
                        ? html`<tr><td colspan="4" class="text-base-content/60">No folder found. Mount your library folders as Docker volumes.</td></tr>`
                        : this.mounts.map(
                            (mount) => html`<tr>
                              <td class="break-all font-mono text-xs">${mount.path}</td>
                              <td>${mount.writable ? 'Read and write' : html`<span class="badge badge-warning badge-soft">Read only</span>`}</td>
                              <td>${mount.mapped_root ? html`<code>${mount.mapped_root}</code>` : '—'}</td>
                              <td class="text-right">
                                ${
                                  mount.mapped_root
                                    ? nothing
                                    : html`<button type="button" class="btn btn-ghost btn-sm" @click=${() => this.open(null, mount.path)}>
                                        <ism-icon name="plus" size="sm"></ism-icon>Map
                                      </button>`
                                }
                              </td>
                            </tr>`,
                          )
                    }
                  </tbody>
                </table>
              </div>
            </div>
          </section>
        `,
        html`<button type="button" class="btn btn-primary btn-sm" @click=${() => this.open(null)}>
          <ism-icon name="plus" size="sm"></ism-icon>Add mapping
        </button>`,
      )}
      ${this.renderDialog()}
    `;
  }
}

customElements.define('ism-mappings-page', IsmMappingsPage);

declare global {
  interface HTMLElementTagNameMap {
    'ism-mappings-page': IsmMappingsPage;
  }
}
