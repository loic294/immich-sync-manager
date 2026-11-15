import { html, nothing } from 'lit';
import { createRef, ref } from 'lit/directives/ref.js';
import { admin } from '@/api/admin';
import type { MappingView } from '@/api/generated/MappingView';
import type { PeerInvite } from '@/api/generated/PeerInvite';
import type { PeerView } from '@/api/generated/PeerView';
import type { ShareInput } from '@/api/generated/ShareInput';
import type { ShareView } from '@/api/generated/ShareView';
import { IsmPage } from '@/base/ism-page';
import { confirmAction } from '@/components/confirm';
import { toast } from '@/components/toast';
import { formatDate, formatLocation, humanize } from '@/util/format';
import { statusBadge } from '@/util/status';

/**
 * Other ISM instances (paired with a one-time invite, reached over a private network such as
 * Netbird) and the shares: folders that replicate with chosen peers. A share replicates into the
 * share with the same name on the other instance, so each side picks its own local folder.
 */
export class IsmPeersPage extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    peers: { state: true },
    shares: { state: true },
    mappings: { state: true },
    invite: { state: true },
    editing: { state: true },
  };

  declare peers: PeerView[];
  declare shares: ShareView[];
  declare mappings: MappingView[];
  declare invite: PeerInvite | undefined;
  declare editing: ShareView | null;
  protected override reloadOn = ['sync_changed'] as const;
  private inviteDialog = createRef<HTMLDialogElement>();
  private acceptDialog = createRef<HTMLDialogElement>();
  private shareDialog = createRef<HTMLDialogElement>();

  constructor() {
    super();
    this.peers = [];
    this.shares = [];
    this.mappings = [];
    this.invite = undefined;
    this.editing = null;
  }

  protected override async load() {
    [this.peers, this.shares, this.mappings] = await Promise.all([
      admin.peers(),
      admin.shares(),
      admin.mappings(),
    ]);
  }

  private async createInvite() {
    await this.act('invite', async () => {
      this.invite = await admin.invite();
      this.inviteDialog.value?.showModal();
    });
  }

  private async copy(text: string) {
    await navigator.clipboard.writeText(text);
    toast.success('Copied.');
  }

  private async onAccept(event: SubmitEvent) {
    event.preventDefault();
    const formElement = event.target as HTMLFormElement;
    const form = new FormData(formElement);
    const text = (key: string) => String(form.get(key) ?? '').trim();
    const ok = await this.act(
      'accept',
      () =>
        admin.acceptInvite({
          name: text('name'),
          url: text('url'),
          invite: text('invite'),
          callback_url: text('callback_url'),
        }),
      'Peer paired.',
    );
    if (ok) {
      formElement.reset();
      this.acceptDialog.value?.close();
    }
  }

  private async removePeer(peer: PeerView) {
    const ok = await confirmAction({
      title: 'Remove peer',
      message: `Stop replicating with “${peer.name}”? Queued transfers to it are dropped. Files are not touched.`,
      confirmLabel: 'Remove',
      danger: true,
    });
    if (ok) await this.act(`peer:${peer.id}`, () => admin.deletePeer(peer.id), 'Peer removed.');
  }

  private async retryPeer(peer: PeerView) {
    await this.act(`retry:${peer.id}`, () => admin.retryPeer(peer.id), 'Peer retry requested.');
  }

  private openShare(share: ShareView | null) {
    this.editing = share;
    this.shareDialog.value?.querySelector('form')?.reset();
    this.shareDialog.value?.showModal();
  }

  private async onShare(event: SubmitEvent) {
    event.preventDefault();
    const form = new FormData(event.target as HTMLFormElement);
    const body: ShareInput = {
      name: String(form.get('name') ?? '').trim(),
      location: {
        root_id: String(form.get('root_id') ?? ''),
        path: String(form.get('path') ?? '')
          .trim()
          .replace(/^\/+|\/+$/g, ''),
      },
      peer_ids: form.getAll('peer_ids').map(String),
      enabled: form.get('enabled') === 'on',
    };
    const editing = this.editing;
    const ok = await this.act(
      'share',
      () => (editing ? admin.updateShare(editing.id, body) : admin.createShare(body)),
      editing ? 'Share updated.' : 'Share added.',
    );
    if (ok) this.shareDialog.value?.close();
  }

  private async removeShare(share: ShareView) {
    const ok = await confirmAction({
      title: 'Remove share',
      message: `Stop replicating “${share.name}”? Files stay on every instance.`,
      confirmLabel: 'Remove',
      danger: true,
    });
    if (ok)
      await this.act(`share:${share.id}`, () => admin.deleteShare(share.id), 'Share removed.');
  }

  private renderPeers() {
    return html`
      <section class="card card-border bg-base-100">
        <div class="card-body p-4">
          <div class="flex flex-wrap items-center justify-between gap-2">
            <h2 class="card-title text-base">Peers</h2>
            <div class="flex gap-2">
              <button type="button" class="btn btn-sm" @click=${this.createInvite}>
                ${this.spin('invite')}<ism-icon name="share-2" size="sm"></ism-icon>Create invite
              </button>
              <button type="button" class="btn btn-sm" @click=${() => this.acceptDialog.value?.showModal()}>
                <ism-icon name="plug" size="sm"></ism-icon>Accept invite
              </button>
            </div>
          </div>
          <div class="overflow-x-auto">
            <table class="table table-sm">
              <thead><tr><th>Peer</th><th>Status</th><th>Last seen</th><th>Changes applied</th><th></th></tr></thead>
              <tbody>
                ${
                  this.peers.length === 0
                    ? html`<tr><td colspan="5" class="text-base-content/60">
                        No peer yet. Create an invite here and accept it on the other instance.
                      </td></tr>`
                    : this.peers.map(
                        (p) => html`<tr>
                          <td><div class="font-medium">${p.name}</div><div class="text-xs text-base-content/60">${p.url}</div></td>
                          <td>
                            <span class=${statusBadge(p.status)}>${humanize(p.status)}</span>
                            ${p.last_error ? html`<div class="text-xs text-error">${p.last_error}</div>` : nothing}
                          </td>
                          <td class="whitespace-nowrap">${formatDate(p.last_seen_at)}</td>
                          <td>${p.events_applied}</td>
                          <td class="whitespace-nowrap text-right">
                            ${
                              p.last_error
                                ? html`<button type="button" class="btn btn-ghost btn-sm" ?disabled=${this.busy !== undefined} @click=${() => this.retryPeer(p)}>
                                    ${this.spin(`retry:${p.id}`)}Retry
                                  </button>`
                                : nothing
                            }
                            <button type="button" class="btn btn-ghost btn-sm btn-square text-error" aria-label="Remove" @click=${() => this.removePeer(p)}>
                              <ism-icon name="unplug" size="sm"></ism-icon>
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
    `;
  }

  private renderShares() {
    const peerName = (id: string) => this.peers.find((p) => p.id === id)?.name ?? id;
    return html`
      <section class="card card-border bg-base-100">
        <div class="card-body p-4">
          <div class="flex items-center justify-between">
            <h2 class="card-title text-base">Shares</h2>
            <button type="button" class="btn btn-sm" @click=${() => this.openShare(null)}>
              <ism-icon name="plus" size="sm"></ism-icon>Add share
            </button>
          </div>
          <div class="overflow-x-auto">
            <table class="table table-sm">
              <thead><tr><th>Name</th><th>Local folder</th><th>Peers</th><th>Last scan</th><th></th></tr></thead>
              <tbody>
                ${
                  this.shares.length === 0
                    ? html`<tr><td colspan="5" class="text-base-content/60">No share yet.</td></tr>`
                    : this.shares.map(
                        (s) => html`<tr>
                          <td>
                            <span class="font-medium">${s.name}</span>
                            ${s.enabled ? nothing : html`<span class="badge badge-sm badge-ghost">Paused</span>`}
                          </td>
                          <td class="break-all font-mono text-xs">${formatLocation(s.location)}</td>
                          <td>${s.peer_ids.length === 0 ? '—' : s.peer_ids.map(peerName).join(', ')}</td>
                          <td class="whitespace-nowrap">${formatDate(s.last_scan_at)}</td>
                          <td class="whitespace-nowrap text-right">
                            <button type="button" class="btn btn-ghost btn-sm btn-square" aria-label="Edit" @click=${() => this.openShare(s)}>
                              <ism-icon name="pencil" size="sm"></ism-icon>
                            </button>
                            <button type="button" class="btn btn-ghost btn-sm btn-square text-error" aria-label="Remove" @click=${() => this.removeShare(s)}>
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
    `;
  }

  private renderDialogs() {
    const s = this.editing;
    const callback = `${window.location.protocol}//${window.location.host}`;
    return html`
      <dialog class="modal" ${ref(this.inviteDialog)}>
        <div class="modal-box">
          <h3 class="text-lg font-semibold">Invite</h3>
          <p class="py-2 text-sm text-base-content/70">
            Paste this into Peers → Accept invite on the other instance before ${formatDate(this.invite?.expires_at)}.
            It works once.
          </p>
          <textarea class="textarea w-full font-mono text-xs" rows="4" readonly .value=${this.invite?.invite ?? ''}></textarea>
          <div class="modal-action">
            <button type="button" class="btn" @click=${() => this.copy(this.invite?.invite ?? '')}>
              <ism-icon name="copy" size="sm"></ism-icon>Copy
            </button>
            <form method="dialog"><button class="btn btn-primary">Done</button></form>
          </div>
        </div>
        <form method="dialog" class="modal-backdrop"><button>close</button></form>
      </dialog>

      <dialog class="modal" ${ref(this.acceptDialog)}>
        <div class="modal-box">
          <h3 class="text-lg font-semibold">Accept an invite</h3>
          <form class="flex flex-col gap-1" @submit=${this.onAccept}>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Peer name</legend>
              <input name="name" class="input w-full" required maxlength="64" placeholder="Office" />
            </fieldset>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Peer address</legend>
              <input name="url" type="url" class="input w-full" required placeholder="http://192.0.2.2:8080" />
              <p class="label">How this instance reaches the other one, for example its Netbird address.</p>
            </fieldset>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">This instance's address</legend>
              <input name="callback_url" type="url" class="input w-full" required .value=${callback} />
              <p class="label">How the other instance reaches this one.</p>
            </fieldset>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Invite</legend>
              <textarea name="invite" class="textarea w-full font-mono text-xs" rows="3" required></textarea>
            </fieldset>
            <div class="modal-action">
              <button type="button" class="btn btn-ghost" @click=${() => this.acceptDialog.value?.close()}>Cancel</button>
              <button class="btn btn-primary" ?disabled=${this.busy !== undefined}>${this.spin('accept')}Pair</button>
            </div>
          </form>
        </div>
        <form method="dialog" class="modal-backdrop"><button>close</button></form>
      </dialog>

      <dialog class="modal" ${ref(this.shareDialog)}>
        <div class="modal-box max-w-lg">
          <h3 class="text-lg font-semibold">${s ? `Edit ${s.name}` : 'Add share'}</h3>
          <form class="flex flex-col gap-1" @submit=${this.onShare}>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Name</legend>
              <input name="name" class="input w-full" required maxlength="64" .value=${s?.name ?? ''} placeholder="family-photos" />
              <p class="label">Use the same name on the other instance to connect the two folders.</p>
            </fieldset>
            <div class="grid grid-cols-1 gap-x-3 sm:grid-cols-2">
              <fieldset class="fieldset">
                <legend class="fieldset-legend">Mapped folder</legend>
                <select name="root_id" class="select w-full" required>
                  ${this.mappings.map((m) => html`<option value=${m.root_id} ?selected=${m.root_id === s?.location.root_id}>${m.name}</option>`)}
                </select>
              </fieldset>
              <fieldset class="fieldset">
                <legend class="fieldset-legend">Subfolder (optional)</legend>
                <input name="path" class="input w-full font-mono" .value=${s?.location.path ?? ''} placeholder="2024" />
              </fieldset>
            </div>
            <fieldset class="fieldset">
              <legend class="fieldset-legend">Replicate with</legend>
              ${
                this.peers.length === 0
                  ? html`<p class="label">Pair a peer first.</p>`
                  : this.peers.map(
                      (p) => html`<label class="label">
                        <input type="checkbox" class="checkbox checkbox-sm" name="peer_ids" value=${p.id} .checked=${s?.peer_ids.includes(p.id) ?? false} />
                        ${p.name}
                      </label>`,
                    )
              }
            </fieldset>
            <label class="label">
              <input type="checkbox" class="toggle toggle-sm" name="enabled" .checked=${s?.enabled ?? true} />
              Enabled
            </label>
            <div class="modal-action">
              <button type="button" class="btn btn-ghost" @click=${() => this.shareDialog.value?.close()}>Cancel</button>
              <button class="btn btn-primary" ?disabled=${this.busy !== undefined}>${this.spin('share')}Save</button>
            </div>
          </form>
        </div>
        <form method="dialog" class="modal-backdrop"><button>close</button></form>
      </dialog>
    `;
  }

  protected override render() {
    return html`${this.frame(() => html`${this.renderPeers()} ${this.renderShares()}`)} ${this.renderDialogs()}`;
  }
}

customElements.define('ism-peers-page', IsmPeersPage);

declare global {
  interface HTMLElementTagNameMap {
    'ism-peers-page': IsmPeersPage;
  }
}
