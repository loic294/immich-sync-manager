import { html } from 'lit';
import { createRef, ref } from 'lit/directives/ref.js';
import { admin } from '@/api/admin';
import type { ClientAction } from '@/api/generated/ClientAction';
import type { ClientTokenView } from '@/api/generated/ClientTokenView';
import type { MappingView } from '@/api/generated/MappingView';
import { IsmPage } from '@/base/ism-page';
import { confirmAction } from '@/components/confirm';
import { toast } from '@/components/toast';
import { formatDate } from '@/util/format';

const ACTIONS: readonly { value: ClientAction; label: string; hint: string }[] = [
  { value: 'read', label: 'Read', hint: 'List folders and files' },
  { value: 'download', label: 'Download', hint: 'Download originals and sidecars' },
  { value: 'upload', label: 'Upload', hint: 'Upload new files into mapped folders' },
  { value: 'manage', label: 'Manage', hint: 'Move, rename and create folders' },
  { value: 'trash', label: 'Trash', hint: 'Move files to the recoverable trash' },
  { value: 'purge', label: 'Purge', hint: 'Delete trashed files permanently' },
  { value: 'transfer_owner', label: 'Transfer owner', hint: 'Move assets between Immich accounts' },
];

const KIND_DEFAULTS: Readonly<Record<string, readonly ClientAction[]>> = {
  ila: ['read', 'download', 'upload', 'manage', 'trash'],
  opm: ['read', 'manage', 'trash'],
  other: ['read'],
};

/** Tokens for Immich Local App and Open Photo Manager. The secret is shown once. */
export class IsmTokensPage extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    tokens: { state: true },
    mappings: { state: true },
    kind: { state: true },
    secret: { state: true },
  };

  declare tokens: ClientTokenView[];
  declare mappings: MappingView[];
  declare kind: string;
  declare secret: string | undefined;
  private dialog = createRef<HTMLDialogElement>();

  constructor() {
    super();
    this.tokens = [];
    this.mappings = [];
    this.kind = 'ila';
    this.secret = undefined;
  }

  protected override async load() {
    [this.tokens, this.mappings] = await Promise.all([admin.tokens(), admin.mappings()]);
  }

  private openCreate() {
    this.secret = undefined;
    this.kind = 'ila';
    this.dialog.value?.showModal();
  }

  private async onSubmit(event: SubmitEvent) {
    event.preventDefault();
    const formElement = event.target as HTMLFormElement;
    const form = new FormData(formElement);
    const body = {
      name: String(form.get('name') ?? '').trim(),
      kind: String(form.get('kind') ?? 'other'),
      roots: form.getAll('roots').map(String),
      actions: form.getAll('actions').map(String) as ClientAction[],
    };
    await this.act('create', async () => {
      const created = await admin.createToken(body);
      this.secret = created.secret;
      formElement.reset();
    });
  }

  private async revoke(token: ClientTokenView) {
    const ok = await confirmAction({
      title: 'Revoke token',
      message: `Clients using “${token.name}” stop working immediately.`,
      confirmLabel: 'Revoke',
      danger: true,
    });
    if (ok)
      await this.act(`revoke:${token.id}`, () => admin.revokeToken(token.id), 'Token revoked.');
  }

  private async copySecret() {
    if (!this.secret) return;
    await navigator.clipboard.writeText(this.secret);
    toast.success('Copied.');
  }

  private renderDialog() {
    const defaults = KIND_DEFAULTS[this.kind] ?? [];
    return html`
      <dialog class="modal" ${ref(this.dialog)}>
        <div class="modal-box max-w-xl">
          <h3 class="text-lg font-semibold">New client token</h3>
          ${
            this.secret
              ? html`
                <div role="alert" class="alert alert-warning alert-soft my-4">
                  <ism-icon name="triangle-alert"></ism-icon>
                  <span>Copy the secret now. It is not shown again.</span>
                </div>
                <div class="join w-full">
                  <input class="input join-item w-full font-mono" readonly .value=${this.secret} aria-label="Token secret" />
                  <button type="button" class="btn join-item" @click=${this.copySecret}>
                    <ism-icon name="copy" size="sm"></ism-icon>Copy
                  </button>
                </div>
                <div class="modal-action">
                  <form method="dialog"><button class="btn btn-primary">Done</button></form>
                </div>
              `
              : html`
                <form class="flex flex-col gap-2" @submit=${this.onSubmit}>
                  <fieldset class="fieldset">
                    <legend class="fieldset-legend">Name</legend>
                    <input name="name" class="input w-full" required maxlength="64" placeholder="Laptop" />
                  </fieldset>
                  <fieldset class="fieldset">
                    <legend class="fieldset-legend">Client</legend>
                    <select name="kind" class="select w-full" @change=${(e: Event) => (this.kind = (e.target as HTMLSelectElement).value)}>
                      <option value="ila" ?selected=${this.kind === 'ila'}>Immich Local App</option>
                      <option value="opm" ?selected=${this.kind === 'opm'}>Open Photo Manager</option>
                      <option value="other" ?selected=${this.kind === 'other'}>Other</option>
                    </select>
                  </fieldset>
                  <fieldset class="fieldset">
                    <legend class="fieldset-legend">Folders</legend>
                    <p class="label">None selected gives access to every mapped folder.</p>
                    ${this.mappings.map(
                      (m) => html`<label class="label">
                        <input type="checkbox" class="checkbox checkbox-sm" name="roots" value=${m.root_id} />
                        ${m.name} <code class="text-xs">${m.root_id}</code>
                      </label>`,
                    )}
                  </fieldset>
                  <fieldset class="fieldset">
                    <legend class="fieldset-legend">Allowed actions</legend>
                    ${ACTIONS.map(
                      (a) => html`<label class="label">
                        <input
                          type="checkbox"
                          class="checkbox checkbox-sm"
                          name="actions"
                          value=${a.value}
                          .checked=${defaults.includes(a.value)}
                        />
                        <span class="font-medium text-base-content">${a.label}</span> — ${a.hint}
                      </label>`,
                    )}
                  </fieldset>
                  <div class="modal-action">
                    <button type="button" class="btn btn-ghost" @click=${() => this.dialog.value?.close()}>Cancel</button>
                    <button class="btn btn-primary" ?disabled=${this.busy !== undefined}>${this.spin('create')}Create</button>
                  </div>
                </form>
              `
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
          <h2 class="sr-only">Tokens</h2>
          <div class="overflow-x-auto rounded-box border border-base-300 bg-base-100">
            <table class="table table-sm">
              <thead><tr><th>Name</th><th>Client</th><th>Folders</th><th>Actions</th><th>Last used</th><th></th></tr></thead>
              <tbody>
                ${
                  this.tokens.length === 0
                    ? html`<tr><td colspan="6" class="text-center text-base-content/60">No tokens yet.</td></tr>`
                    : this.tokens.map(
                        (t) => html`<tr>
                          <td class="font-medium">${t.name}</td>
                          <td><span class="badge badge-ghost">${t.kind}</span></td>
                          <td>${t.roots.length === 0 ? 'All' : t.roots.join(', ')}</td>
                          <td class="max-w-xs"><div class="flex flex-wrap gap-1">${t.actions.map((a) => html`<span class="badge badge-sm badge-outline">${a}</span>`)}</div></td>
                          <td class="whitespace-nowrap">${formatDate(t.last_used_at)}</td>
                          <td class="text-right">
                            <button type="button" class="btn btn-ghost btn-sm text-error" @click=${() => this.revoke(t)}>
                              ${this.spin(`revoke:${t.id}`)}Revoke
                            </button>
                          </td>
                        </tr>`,
                      )
                }
              </tbody>
            </table>
          </div>
        `,
        html`<button type="button" class="btn btn-primary btn-sm" @click=${this.openCreate}>
          <ism-icon name="plus" size="sm"></ism-icon>New token
        </button>`,
      )}
      ${this.renderDialog()}
    `;
  }
}

customElements.define('ism-tokens-page', IsmTokensPage);

declare global {
  interface HTMLElementTagNameMap {
    'ism-tokens-page': IsmTokensPage;
  }
}
