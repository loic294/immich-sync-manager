import { html, nothing } from 'lit';
import { admin } from '@/api/admin';
import type { ConflictResolution } from '@/api/generated/ConflictResolution';
import type { ConflictSide } from '@/api/generated/ConflictSide';
import type { ConflictView } from '@/api/generated/ConflictView';
import { IsmPage } from '@/base/ism-page';
import { confirmAction } from '@/components/confirm';
import { formatBytes, formatDate, formatLocation, humanize } from '@/util/format';
import { statusBadge } from '@/util/status';

const EXPLAIN: Readonly<Record<string, string>> = {
  changed_on_both: 'The file changed on both instances since they last agreed.',
  changed_here_deleted_there: 'The file changed on one instance and was deleted on the other.',
  apply_failed: 'A change from the peer could not be applied here.',
};

const RESOLUTIONS: readonly { value: ConflictResolution; label: string; hint: string }[] = [
  { value: 'keep_local', label: 'Keep this instance', hint: 'The peer receives this version.' },
  {
    value: 'keep_remote',
    label: 'Keep the peer',
    hint: "This instance takes the peer's version; the local copy goes to the trash.",
  },
  {
    value: 'keep_both',
    label: 'Keep both',
    hint: "The peer's version is saved next to this one under a new name.",
  },
];

/** Replication conflicts, side by side, with the three ways to settle them. */
export class IsmConflictsPage extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    conflicts: { state: true },
    includeResolved: { state: true },
  };

  declare conflicts: ConflictView[];
  declare includeResolved: boolean;
  protected override reloadOn = ['conflicts_changed'] as const;

  constructor() {
    super();
    this.conflicts = [];
    this.includeResolved = false;
  }

  protected override async load() {
    this.conflicts = await admin.conflicts(this.includeResolved);
  }

  private async resolve(conflict: ConflictView, resolution: ConflictResolution) {
    const choice = RESOLUTIONS.find((r) => r.value === resolution);
    const ok = await confirmAction({
      title: choice?.label ?? 'Resolve',
      message: `${formatLocation(conflict.location)}\n\n${choice?.hint ?? ''}`,
      confirmLabel: choice?.label ?? 'Resolve',
    });
    if (ok)
      await this.act(
        `${conflict.id}:${resolution}`,
        () => admin.resolveConflict(conflict.id, resolution),
        'Resolution applied.',
      );
  }

  private side(title: string, side: ConflictSide) {
    return html`
      <div class="rounded-box border border-base-300 p-3">
        <div class="mb-1 font-semibold">${title} <span class="text-xs font-normal text-base-content/60">${side.instance}</span></div>
        ${
          side.deleted
            ? html`<span class="badge badge-error badge-soft">Deleted</span>`
            : html`<dl class="grid grid-cols-[auto_1fr] gap-x-3 text-sm">
                <dt class="text-base-content/60">Size</dt><dd>${side.size === null ? '—' : formatBytes(side.size)}</dd>
                <dt class="text-base-content/60">Modified</dt><dd>${formatDate(side.modified_at)}</dd>
                <dt class="text-base-content/60">Checksum</dt><dd class="font-mono text-xs break-all">${side.sha256?.slice(0, 16) ?? '—'}</dd>
              </dl>`
        }
      </div>
    `;
  }

  private card(c: ConflictView) {
    const open = c.status === 'open';
    return html`
      <article class="card card-border bg-base-100">
        <div class="card-body gap-3 p-4">
          <div class="flex flex-wrap items-center gap-2">
            <h2 class="card-title break-all text-base">${formatLocation(c.location)}</h2>
            <span class=${statusBadge(c.status)}>${humanize(c.status)}</span>
          </div>
          <p class="text-sm text-base-content/70">
            ${c.share_name} · ${EXPLAIN[c.kind] ?? humanize(c.kind)} · ${formatDate(c.created_at)}
          </p>
          ${c.detail ? html`<p class="text-sm">${c.detail}</p>` : nothing}
          <div class="grid grid-cols-1 gap-3 md:grid-cols-2">
            ${this.side('This instance', c.local)} ${this.side('Peer', c.remote)}
          </div>
          ${
            open
              ? html`<div class="card-actions">
                  ${RESOLUTIONS.map(
                    (r) => html`<div class="tooltip" data-tip=${r.hint}>
                      <button type="button" class="btn btn-sm" ?disabled=${this.busy !== undefined} @click=${() => this.resolve(c, r.value)}>
                        ${this.spin(`${c.id}:${r.value}`)}${r.label}
                      </button>
                    </div>`,
                  )}
                </div>`
              : nothing
          }
        </div>
      </article>
    `;
  }

  protected override render() {
    return this.frame(
      () =>
        this.conflicts.length === 0
          ? html`<div class="card card-border bg-base-100"><div class="card-body items-center text-center">
              <ism-icon name="circle-check" size="xl" class="text-success"></ism-icon>
              <h2 class="card-title">No conflicts</h2>
              <p class="text-sm text-base-content/70">Every share agrees with its peers.</p>
            </div></div>`
          : html`${this.conflicts.map((c) => this.card(c))}`,
      html`<label class="label">
        <input
          type="checkbox"
          class="toggle toggle-sm"
          .checked=${this.includeResolved}
          @change=${(e: Event) => {
            this.includeResolved = (e.target as HTMLInputElement).checked;
            void this.reload();
          }}
        />
        Show resolved
      </label>`,
    );
  }
}

customElements.define('ism-conflicts-page', IsmConflictsPage);

declare global {
  interface HTMLElementTagNameMap {
    'ism-conflicts-page': IsmConflictsPage;
  }
}
