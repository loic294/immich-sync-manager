import { html, nothing } from 'lit';
import { live } from 'lit/directives/live.js';
import { createRef, ref } from 'lit/directives/ref.js';
import { admin } from '@/api/admin';
import type { ImmichServerView } from '@/api/generated/ImmichServerView';
import type { LibraryView } from '@/api/generated/LibraryView';
import type { MappingInput } from '@/api/generated/MappingInput';
import type { MappingView } from '@/api/generated/MappingView';
import type { MountView } from '@/api/generated/MountView';
import type { WatchMode } from '@/api/generated/WatchMode';
import { IsmPage } from '@/base/ism-page';
import { confirmAction } from '@/components/confirm';
import { formatDate, humanize } from '@/util/format';

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
    mountTrail: { state: true },
    servers: { state: true },
    libraries: { state: true },
    editing: { state: true },
    serverId: { state: true },
    libraryId: { state: true },
    containerPath: { state: true },
    watching: { state: true },
  };

  declare mappings: MappingView[];
  declare mounts: MountView[];
  declare mountTrail: string[];
  declare servers: ImmichServerView[];
  declare libraries: LibraryView[];
  declare editing: MappingView | null;
  declare serverId: string;
  declare libraryId: string;
  declare containerPath: string;
  /** The mapping whose watch settings are being edited. */
  declare watching: MappingView | null;
  private dialog = createRef<HTMLDialogElement>();
  private watchDialog = createRef<HTMLDialogElement>();

  constructor() {
    super();
    this.mappings = [];
    this.mounts = [];
    this.mountTrail = [];
    this.servers = [];
    this.libraries = [];
    this.editing = null;
    this.serverId = '';
    this.libraryId = '';
    this.containerPath = '';
    this.watching = null;
  }

  protected override async load() {
    const path = this.mountTrail.at(-1);
    const [mappings, mounts, servers, libraries] = await Promise.all([
      this.data.mappings(),
      this.data.mounts(path),
      this.data.servers(),
      this.data.libraries(),
    ]);
    this.mappings = mappings;
    if (path === this.mountTrail.at(-1)) this.mounts = mounts;
    this.servers = servers;
    this.libraries = libraries;
  }

  private async browse(trail: string[]) {
    await this.act('browse', async () => {
      const mounts = await admin.mounts(trail.at(-1));
      this.mounts = mounts;
      this.mountTrail = trail;
    });
  }

  private open(mapping: MappingView | null, mountPath = '') {
    this.editing = mapping;
    this.serverId = mapping?.server_id ?? '';
    this.libraryId = mapping?.library_id ?? '';
    this.containerPath = mapping?.container_path ?? mountPath;
    this.dialog.value?.querySelector('form')?.reset();
    this.requestUpdate();
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

  private openWatch(mapping: MappingView) {
    this.watching = mapping;
    this.watchDialog.value?.querySelector('form')?.reset();
    this.watchDialog.value?.showModal();
  }

  private async onWatchSubmit(event: SubmitEvent) {
    event.preventDefault();
    const mapping = this.watching;
    if (!mapping) return;
    const form = new FormData(event.target as HTMLFormElement);
    const minutes = Number(form.get('poll_minutes'));
    const ok = await this.act(
      'watch',
      () =>
        admin.setMappingWatch(mapping.root_id, {
          mode: String(form.get('mode')) as WatchMode,
          poll_interval_secs: Math.round(minutes * 60),
        }),
      'Watch settings saved.',
    );
    if (ok) this.watchDialog.value?.close();
  }

  private async resetWatch(mapping: MappingView) {
    const ok = await confirmAction({
      title: 'Accept the current folder contents',
      message: `Detection for “${mapping.name}” paused because the folder looked unreachable or suddenly empty. Continue only if you checked that what is there now is correct: ISM records it as the new starting point without reporting deletions.`,
      confirmLabel: 'Accept and resume',
      danger: true,
    });
    if (ok)
      await this.act(
        `reset:${mapping.root_id}`,
        () => admin.resetMappingWatch(mapping.root_id),
        'Change detection resumed.',
      );
  }

  private renderWatch(m: MappingView) {
    const w = m.watch;
    if (!w.watched) return html`<span class="text-xs text-base-content/60">Not watched</span>`;
    const mode = w.effective_mode ? humanize(w.effective_mode) : 'Starting';
    return html`
      <div class="text-sm">
        ${mode}${w.mode === 'auto' ? ' (auto)' : ''}
        ${w.effective_mode === 'poll' ? html`<span class="text-xs text-base-content/60">every ${Math.round(w.poll_interval_secs / 60)} min</span>` : nothing}
      </div>
      <div class="text-xs text-base-content/60">Last pass ${formatDate(w.last_pass_at)}</div>
      ${w.paused ? html`<span class="badge badge-warning badge-soft">Paused</span>` : nothing}
      ${w.warnings.map((warning) => html`<div class="text-xs text-warning">${warning}</div>`)}
      ${
        w.paused
          ? html`<button type="button" class="btn btn-xs" @click=${() => this.resetWatch(m)}>
              ${this.spin(`reset:${m.root_id}`)}Accept contents
            </button>`
          : nothing
      }
    `;
  }

  private renderWatchDialog() {
    const w = this.watching?.watch;
    return html`
      <dialog class="modal" ${ref(this.watchDialog)}>
        <div class="modal-box">
          <h3 class="text-lg font-semibold">Change detection${this.watching ? ` for ${this.watching.name}` : ''}</h3>
          <form class="flex flex-col gap-1" @submit=${this.onWatchSubmit}>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Mode</legend>
              <select name="mode" class="select w-full">
                <option value="auto" ?selected=${w?.mode === 'auto'}>Automatic (polling on network shares)</option>
                <option value="native" ?selected=${w?.mode === 'native'}>Change events (local disks)</option>
                <option value="poll" ?selected=${w?.mode === 'poll'}>Polling</option>
              </select>
              <p class="label">Network shares (SMB, NFS) do not report changes reliably, so they are polled.</p>
            </fieldset>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Polling interval (minutes)</legend>
              <input
                name="poll_minutes"
                type="number"
                class="input w-full"
                required
                min="1"
                max="1440"
                .value=${String(Math.round((w?.poll_interval_secs ?? 300) / 60))}
              />
            </fieldset>
            <div class="modal-action">
              <button type="button" class="btn btn-ghost" @click=${() => this.watchDialog.value?.close()}>Cancel</button>
              <button class="btn btn-primary" ?disabled=${this.busy !== undefined}>${this.spin('watch')}Save</button>
            </div>
          </form>
        </div>
        <form method="dialog" class="modal-backdrop"><button>close</button></form>
      </dialog>
    `;
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
                  .value=${live(m?.root_id ?? '')}
                  ?disabled=${m !== null}
                  placeholder="photos"
                />
                <p class="label">Lowercase letters, digits, dashes and underscores. Cannot change later.</p>
              </fieldset>
              <fieldset class="fieldset">
                <legend class="fieldset-legend">Name</legend>
                <input name="name" class="input w-full" required maxlength="64" .value=${live(m?.name ?? '')} placeholder="Photos" />
              </fieldset>
            </div>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Folder in this container</legend>
              <input
                name="container_path"
                class="input w-full font-mono"
                required
                list="ism-mounts"
                .value=${live(this.containerPath)}
                @input=${(e: Event) => (this.containerPath = (e.target as HTMLInputElement).value)}
                placeholder="/data/photos"
              />
              <datalist id="ism-mounts">${this.mounts.map((mount) => html`<option value=${mount.path}></option>`)}</datalist>
              <p class="label">
                ISM creates a root marker to verify the mount. Use separate root IDs for Immich and OPM
                when they map the same folder or different levels of it. Overlaps within one source are not allowed.
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
                .value=${live(m?.immich_import_path ?? '')}
                ?disabled=${this.libraryId === ''}
                placeholder="/mnt/photos"
              />
              <datalist id="ism-import-paths">${library?.import_paths.map((p) => html`<option value=${p}></option>`) ?? nothing}</datalist>
              <p class="label">How the Immich server sees this same folder (one of the library's import paths, or a folder inside one).</p>
            </fieldset>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Open Photo Manager path (optional)</legend>
              <input name="opm_prefix" class="input w-full font-mono" .value=${live(m?.opm_prefix ?? '')} placeholder="/photos" />
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
    const mountPath = this.mountTrail.at(-1);
    return html`
      ${this.frame(
        () => html`
          <section class="card card-border bg-base-100">
            <div class="card-body p-4">
              <h2 class="card-title text-base">Mappings</h2>
              <div class="overflow-x-auto">
                <table class="table table-sm">
                  <thead><tr><th>Root</th><th>Container folder</th><th>Immich</th><th>Open Photo Manager</th><th>Status</th><th>Watching</th><th></th></tr></thead>
                  <tbody>
                    ${
                      this.mappings.length === 0
                        ? html`<tr><td colspan="7" class="text-base-content/60">No mapping yet. Pick a mounted folder below.</td></tr>`
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
                              <td>${this.renderWatch(m)}</td>
                              <td class="whitespace-nowrap text-right">
                                ${
                                  m.watch.watched
                                    ? html`<button type="button" class="btn btn-ghost btn-sm btn-square" aria-label="Change detection" @click=${() => this.openWatch(m)}>
                                        <ism-icon name="eye" size="sm"></ism-icon>
                                      </button>`
                                    : nothing
                                }
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
              <p class="text-sm text-base-content/70">Browse folders under the mounts directory (/mnt by default). Each level lists its immediate subfolders.</p>
              <div class="flex flex-wrap items-center gap-2">
                <button
                  type="button"
                  class="btn btn-ghost btn-sm"
                  aria-label="Go to parent folder"
                  ?disabled=${this.mountTrail.length === 0 || this.busy !== undefined}
                  @click=${() => this.browse(this.mountTrail.slice(0, -1))}
                >Up</button>
                <nav aria-label="Folder path" class="min-w-0 flex-1">
                  <ol class="flex flex-wrap items-center gap-1 text-sm">
                    <li><button
                      type="button"
                      class="btn btn-ghost btn-sm"
                      ?disabled=${this.mountTrail.length === 0 || this.busy !== undefined}
                      @click=${() => this.browse([])}
                    >Mounts directory</button></li>
                    ${this.mountTrail.map(
                      (path, index) => html`
                      <li class="flex min-w-0 items-center gap-1">
                        <span aria-hidden="true">/</span>
                        <button
                          type="button"
                          class="btn btn-ghost btn-sm h-auto break-all py-2"
                          title=${path}
                          aria-current=${index === this.mountTrail.length - 1 ? 'location' : nothing}
                          ?disabled=${this.busy !== undefined}
                          @click=${() => this.browse(this.mountTrail.slice(0, index + 1))}
                        >${path.split('/').at(-1)}</button>
                      </li>
                    `,
                    )}
                  </ol>
                </nav>
                ${this.spin('browse')}
                ${
                  mountPath
                    ? html`
                  <button
                    type="button"
                    class="btn btn-sm"
                    aria-label="Map current folder"
                    ?disabled=${this.busy !== undefined}
                    @click=${() => this.open(null, mountPath)}
                  >Map this folder</button>
                `
                    : nothing
                }
              </div>
              ${mountPath ? html`<p class="break-all font-mono text-xs">${mountPath}</p>` : nothing}
              <div class="overflow-x-auto" aria-busy=${this.busy === 'browse'}>
                <table class="table table-sm">
                  <thead><tr><th>Folder</th><th>Access</th><th>Mapping</th><th></th></tr></thead>
                  <tbody>
                    ${
                      this.mounts.length === 0
                        ? html`<tr><td colspan="4" class="text-base-content/60">${mountPath ? 'No subfolders in this folder.' : 'No folder found. Mount your library folders as Docker volumes.'}</td></tr>`
                        : this.mounts.map(
                            (mount) => html`<tr>
                              <td class="break-all font-mono text-xs">${mount.path}</td>
                              <td>${mount.writable ? 'Read and write' : html`<span class="badge badge-warning badge-soft">Read only</span>`}</td>
                              <td>${
                                this.mappings.some((m) => m.container_path === mount.path)
                                  ? this.mappings
                                      .filter((m) => m.container_path === mount.path)
                                      .map((m) => html`<div><code>${m.root_id}</code></div>`)
                                  : '—'
                              }</td>
                              <td class="whitespace-nowrap text-right">
                                <button
                                  type="button"
                                  class="btn btn-ghost btn-sm"
                                  aria-label=${`Browse ${mount.path}`}
                                  ?disabled=${this.busy !== undefined}
                                  @click=${() => this.browse([...this.mountTrail, mount.path])}
                                >Browse</button>
                                <button type="button" class="btn btn-ghost btn-sm" aria-label=${`Map ${mount.path}`} ?disabled=${this.busy !== undefined} @click=${() => this.open(null, mount.path)}>
                                  <ism-icon name="plus" size="sm"></ism-icon>Map
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
        `,
        html`<button type="button" class="btn btn-primary btn-sm" @click=${() => this.open(null)}>
          <ism-icon name="plus" size="sm"></ism-icon>Add mapping
        </button>`,
      )}
      ${this.renderDialog()} ${this.renderWatchDialog()}
    `;
  }
}

customElements.define('ism-mappings-page', IsmMappingsPage);

declare global {
  interface HTMLElementTagNameMap {
    'ism-mappings-page': IsmMappingsPage;
  }
}
