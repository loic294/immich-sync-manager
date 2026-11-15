import { html, nothing } from 'lit';
import { createRef, ref } from 'lit/directives/ref.js';
import { admin } from '@/api/admin';
import type { AccountView } from '@/api/generated/AccountView';
import type { ImmichServerInput } from '@/api/generated/ImmichServerInput';
import type { ImmichServerView } from '@/api/generated/ImmichServerView';
import type { LibraryView } from '@/api/generated/LibraryView';
import { IsmPage } from '@/base/ism-page';
import { confirmAction } from '@/components/confirm';
import { humanize } from '@/util/format';
import { statusBadge } from '@/util/status';

/**
 * Immich servers, the accounts ISM signs in with, and which external libraries those accounts
 * cover. Every library needs its owner's account so changes can be routed to the right user.
 */
export class IsmServersPage extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    servers: { state: true },
    accounts: { state: true },
    libraries: { state: true },
    editing: { state: true },
    accountServer: { state: true },
  };

  declare servers: ImmichServerView[];
  declare accounts: AccountView[];
  declare libraries: LibraryView[];
  /** The server being edited; `null` while adding one. */
  declare editing: ImmichServerView | null;
  declare accountServer: ImmichServerView | undefined;
  protected override reloadOn = ['maintenance_changed'] as const;
  private serverDialog = createRef<HTMLDialogElement>();
  private accountDialog = createRef<HTMLDialogElement>();

  constructor() {
    super();
    this.servers = [];
    this.accounts = [];
    this.libraries = [];
    this.editing = null;
    this.accountServer = undefined;
  }

  protected override async load() {
    [this.servers, this.accounts, this.libraries] = await Promise.all([
      admin.servers(),
      admin.accounts(),
      admin.libraries(),
    ]);
  }

  private openServer(server: ImmichServerView | null) {
    this.editing = server;
    const form = this.serverDialog.value?.querySelector('form');
    form?.reset();
    this.serverDialog.value?.showModal();
  }

  private async onServerSubmit(event: SubmitEvent) {
    event.preventDefault();
    const formElement = event.target as HTMLFormElement;
    const form = new FormData(formElement);
    const databaseUrl = String(form.get('database_url') ?? '').trim();
    const clearDatabase = form.get('clear_database') === 'on';
    const body: ImmichServerInput = {
      name: String(form.get('name') ?? '').trim(),
      url: String(form.get('url') ?? '').trim(),
      database_url: clearDatabase ? '' : databaseUrl || null,
      direct_writes_enabled: form.get('direct_writes_enabled') === 'on',
    };
    const editing = this.editing;
    const ok = await this.act(
      'server',
      () => (editing ? admin.updateServer(editing.id, body) : admin.createServer(body)),
      editing ? 'Server updated.' : 'Server added.',
    );
    if (ok) {
      formElement.reset();
      this.serverDialog.value?.close();
    }
  }

  private async deleteServer(server: ImmichServerView) {
    const ok = await confirmAction({
      title: 'Remove server',
      message: `Remove “${server.name}” with its accounts and library list? Files are not touched.`,
      confirmLabel: 'Remove',
      danger: true,
    });
    if (ok)
      await this.act(`delete:${server.id}`, () => admin.deleteServer(server.id), 'Server removed.');
  }

  private openAccount(server: ImmichServerView) {
    this.accountServer = server;
    this.accountDialog.value?.querySelector('form')?.reset();
    this.accountDialog.value?.showModal();
  }

  private async onAccountSubmit(event: SubmitEvent) {
    event.preventDefault();
    const server = this.accountServer;
    if (!server) return;
    const formElement = event.target as HTMLFormElement;
    const form = new FormData(formElement);
    const ok = await this.act(
      'account',
      () =>
        admin.createAccount({
          server_id: server.id,
          label: String(form.get('label') ?? '').trim(),
          email: String(form.get('email') ?? '').trim(),
          password: String(form.get('password') ?? ''),
        }),
      'Account connected.',
    );
    if (ok) {
      formElement.reset();
      this.accountDialog.value?.close();
    }
  }

  private async deleteAccount(account: AccountView) {
    const ok = await confirmAction({
      title: 'Disconnect account',
      message: `Disconnect “${account.label}”? Libraries it owns can no longer be changed through ISM.`,
      confirmLabel: 'Disconnect',
      danger: true,
    });
    if (ok)
      await this.act(
        `account:${account.id}`,
        () => admin.deleteAccount(account.id),
        'Account disconnected.',
      );
  }

  private async resolveMaintenance(server: ImmichServerView) {
    const ok = await confirmAction({
      title: 'Mark recovery as done',
      message:
        'Confirm that you checked the folders and Immich assets listed in the message and that writes can resume.',
      confirmLabel: 'Mark as done',
    });
    if (ok)
      await this.act(
        `resolve:${server.id}`,
        () => admin.resolveMaintenance(server.id),
        'Recovery marked as done.',
      );
  }

  private renderMaintenance(server: ImmichServerView) {
    const m = server.maintenance;
    return html`
      <div class="flex flex-wrap items-center gap-2 text-sm">
        <span class="font-medium">Library maintenance</span>
        <span class=${statusBadge(m.phase)}>${humanize(m.phase)}</span>
        ${m.active_ops > 0 ? html`<span>${m.active_ops} operation(s) running</span>` : nothing}
        ${m.queue_paused ? html`<span class="badge badge-info badge-soft">Library queue paused</span>` : nothing}
        ${
          m.phase === 'blocked'
            ? html`<button type="button" class="btn btn-xs" @click=${() => this.act(`retry:${server.id}`, () => admin.retryMaintenance(server.id), 'Retrying.')}>
                ${this.spin(`retry:${server.id}`)}Retry
              </button>`
            : nothing
        }
        ${
          m.phase === 'recovery_required'
            ? html`<button type="button" class="btn btn-xs btn-warning" @click=${() => this.resolveMaintenance(server)}>
                ${this.spin(`resolve:${server.id}`)}Mark recovery as done
              </button>`
            : nothing
        }
      </div>
      ${m.message ? html`<p class="text-sm text-base-content/70">${m.message}</p>` : nothing}
      ${
        m.drift
          ? html`<div role="alert" class="alert alert-warning alert-soft">
              <ism-icon name="triangle-alert"></ism-icon>
              <div>
                <div class="font-medium">Writes are blocked: ${m.drift}</div>
                <div class="text-sm">ISM detects file changes itself; Immich's file watcher and scheduled library scan must stay off.</div>
              </div>
              <button type="button" class="btn btn-sm" @click=${() => this.act(`reapply:${server.id}`, () => admin.reapplyImmichSettings(server.id), 'Immich settings re-applied.')}>
                ${this.spin(`reapply:${server.id}`)}Turn them off again
              </button>
            </div>`
          : nothing
      }
    `;
  }

  private renderServer(server: ImmichServerView) {
    const accounts = this.accounts.filter((a) => a.server_id === server.id);
    const libraries = this.libraries.filter((l) => l.server_id === server.id);
    const accountName = (id: string | null) => this.accounts.find((a) => a.id === id)?.label ?? '—';
    return html`
      <section class="card card-border bg-base-100">
        <div class="card-body gap-3 p-4">
          <div class="flex flex-wrap items-start justify-between gap-2">
            <div>
              <h2 class="card-title text-base">${server.name}</h2>
              <div class="text-sm text-base-content/70">${server.url}</div>
              <div class="mt-1 flex flex-wrap gap-1">
                <span class="badge badge-sm ${server.compatible ? 'badge-success' : 'badge-error'} badge-soft">
                  ${server.version ? `v${server.version}` : 'Version unknown'}
                </span>
                <span class="badge badge-sm badge-ghost">
                  ${server.database_configured ? 'Database connected' : 'No database access'}
                </span>
                ${server.direct_writes_enabled ? html`<span class="badge badge-sm badge-warning badge-soft">Direct writes on</span>` : nothing}
              </div>
              ${server.compatibility_detail ? html`<p class="mt-1 text-sm text-error">${server.compatibility_detail}</p>` : nothing}
            </div>
            <div class="flex flex-wrap gap-1">
              <button type="button" class="btn btn-sm" @click=${() => this.act(`check:${server.id}`, () => admin.checkServer(server.id), 'Server checked.')}>
                ${this.spin(`check:${server.id}`)}<ism-icon name="shield-check" size="sm"></ism-icon>Check
              </button>
              <button type="button" class="btn btn-sm" @click=${() => this.act(`libs:${server.id}`, () => admin.refreshLibraries(server.id), 'Libraries refreshed.')}>
                ${this.spin(`libs:${server.id}`)}<ism-icon name="refresh-cw" size="sm"></ism-icon>Refresh libraries
              </button>
              <button type="button" class="btn btn-ghost btn-sm btn-square" aria-label="Edit" @click=${() => this.openServer(server)}>
                <ism-icon name="pencil" size="sm"></ism-icon>
              </button>
              <button type="button" class="btn btn-ghost btn-sm btn-square text-error" aria-label="Remove" @click=${() => this.deleteServer(server)}>
                <ism-icon name="trash-2" size="sm"></ism-icon>
              </button>
            </div>
          </div>
          ${this.renderMaintenance(server)}

          <div class="flex items-center justify-between">
            <h3 class="font-semibold">Accounts</h3>
            <button type="button" class="btn btn-ghost btn-sm" @click=${() => this.openAccount(server)}>
              <ism-icon name="plus" size="sm"></ism-icon>Add account
            </button>
          </div>
          <div class="overflow-x-auto">
            <table class="table table-sm">
              <thead><tr><th>Label</th><th>Email</th><th>Role</th><th>Status</th><th></th></tr></thead>
              <tbody>
                ${
                  accounts.length === 0
                    ? html`<tr><td colspan="5" class="text-base-content/60">No account yet. Add an administrator first.</td></tr>`
                    : accounts.map(
                        (a) => html`<tr>
                          <td class="font-medium">${a.label}${server.admin_account_id === a.id ? html` <span class="badge badge-sm badge-primary badge-soft">Admin for ISM</span>` : nothing}</td>
                          <td>${a.email}</td>
                          <td>${a.is_admin ? 'Administrator' : 'User'}</td>
                          <td>
                            <span class=${statusBadge(a.status)}>${humanize(a.status)}</span>
                            ${a.last_error ? html`<div class="text-xs text-error">${a.last_error}</div>` : nothing}
                          </td>
                          <td class="text-right">
                            <button type="button" class="btn btn-ghost btn-sm text-error" @click=${() => this.deleteAccount(a)}>
                              ${this.spin(`account:${a.id}`)}Disconnect
                            </button>
                          </td>
                        </tr>`,
                      )
                }
              </tbody>
            </table>
          </div>

          <h3 class="font-semibold">External libraries</h3>
          <div class="overflow-x-auto">
            <table class="table table-sm">
              <thead><tr><th>Library</th><th>Import paths</th><th>Assets</th><th>Owner account</th></tr></thead>
              <tbody>
                ${
                  libraries.length === 0
                    ? html`<tr><td colspan="4" class="text-base-content/60">No external library found. Use Refresh libraries after connecting an administrator.</td></tr>`
                    : libraries.map(
                        (l) => html`<tr>
                          <td class="font-medium">${l.name}</td>
                          <td class="break-all text-xs">${l.import_paths.join(', ')}</td>
                          <td>${l.asset_count ?? '—'}</td>
                          <td>
                            ${
                              l.covered
                                ? html`<span class="badge badge-success badge-soft">${accountName(l.owner_account_id)}</span>`
                                : html`<span class="badge badge-warning badge-soft">Owner not connected</span>`
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
    `;
  }

  private renderServerDialog() {
    const s = this.editing;
    return html`
      <dialog class="modal" ${ref(this.serverDialog)}>
        <div class="modal-box max-w-lg">
          <h3 class="text-lg font-semibold">${s ? 'Edit server' : 'Add Immich server'}</h3>
          <form class="flex flex-col gap-1" @submit=${this.onServerSubmit}>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Name</legend>
              <input name="name" class="input w-full" required maxlength="64" .value=${s?.name ?? ''} placeholder="Home" />
            </fieldset>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">URL</legend>
              <input name="url" type="url" class="input w-full" required .value=${s?.url ?? ''} placeholder="http://immich-server:2283" />
              <p class="label">As reachable from this container. Immich v3.4.1 is required.</p>
            </fieldset>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Database URL (optional)</legend>
              <input
                name="database_url"
                type="password"
                class="input w-full"
                autocomplete="off"
                placeholder=${s?.database_configured ? 'Unchanged' : 'postgres://user:password@database:5432/immich'}
              />
              <p class="label">Needed for moves and renames inside external libraries. Stored encrypted.</p>
              ${
                s?.database_configured
                  ? html`<label class="label"><input type="checkbox" class="checkbox checkbox-sm" name="clear_database" />Remove the stored database URL</label>`
                  : nothing
              }
            </fieldset>
            <label class="label mt-2">
              <input type="checkbox" class="toggle toggle-sm" name="direct_writes_enabled" .checked=${s?.direct_writes_enabled ?? false} />
              Allow direct database writes for moves and renames
            </label>
            <div class="modal-action">
              <button type="button" class="btn btn-ghost" @click=${() => this.serverDialog.value?.close()}>Cancel</button>
              <button class="btn btn-primary" ?disabled=${this.busy !== undefined}>${this.spin('server')}Save</button>
            </div>
          </form>
        </div>
        <form method="dialog" class="modal-backdrop"><button>close</button></form>
      </dialog>
    `;
  }

  private renderAccountDialog() {
    return html`
      <dialog class="modal" ${ref(this.accountDialog)}>
        <div class="modal-box max-w-lg">
          <h3 class="text-lg font-semibold">Connect an account to ${this.accountServer?.name ?? ''}</h3>
          <p class="py-2 text-sm text-base-content/70">
            ISM signs in once and keeps only the resulting API key. Connect the owner of every external
            library you want to manage.
          </p>
          <form class="flex flex-col gap-1" @submit=${this.onAccountSubmit}>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Label</legend>
              <input name="label" class="input w-full" required maxlength="64" placeholder="Family" />
            </fieldset>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Email</legend>
              <input name="email" type="email" class="input w-full" required autocomplete="off" />
            </fieldset>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Password</legend>
              <input name="password" type="password" class="input w-full" required autocomplete="off" />
            </fieldset>
            <div class="modal-action">
              <button type="button" class="btn btn-ghost" @click=${() => this.accountDialog.value?.close()}>Cancel</button>
              <button class="btn btn-primary" ?disabled=${this.busy !== undefined}>${this.spin('account')}Connect</button>
            </div>
          </form>
        </div>
        <form method="dialog" class="modal-backdrop"><button>close</button></form>
      </dialog>
    `;
  }

  protected override render() {
    return html`
      ${this.frame(
        () =>
          this.servers.length === 0
            ? html`<div class="card card-border bg-base-100"><div class="card-body items-center text-center">
                <ism-icon name="server" size="xl" class="text-base-content/40"></ism-icon>
                <h2 class="card-title">No Immich server yet</h2>
                <p class="text-sm text-base-content/70">Add the Immich server this instance manages.</p>
              </div></div>`
            : html`${this.servers.map((s) => this.renderServer(s))}`,
        html`<button type="button" class="btn btn-primary btn-sm" @click=${() => this.openServer(null)}>
          <ism-icon name="plus" size="sm"></ism-icon>Add server
        </button>`,
      )}
      ${this.renderServerDialog()} ${this.renderAccountDialog()}
    `;
  }
}

customElements.define('ism-servers-page', IsmServersPage);

declare global {
  interface HTMLElementTagNameMap {
    'ism-servers-page': IsmServersPage;
  }
}
