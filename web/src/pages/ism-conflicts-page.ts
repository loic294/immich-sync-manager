import { html, nothing } from 'lit';
import { admin } from '@/api/admin';
import type { ConflictResolution } from '@/api/generated/ConflictResolution';
import type { ConflictSide } from '@/api/generated/ConflictSide';
import type { ConflictView } from '@/api/generated/ConflictView';
import type { RecoveryVersions } from '@/api/generated/RecoveryVersions';
import { IsmPage } from '@/base/ism-page';
import { confirmAction } from '@/components/confirm';
import { toast } from '@/components/toast';
import { formatBytes, formatDate, formatLocation, humanize } from '@/util/format';
import { statusBadge } from '@/util/status';
import '@/components/ism-recovery-versions';

const EXPLAIN: Readonly<Record<string, string>> = {
  changed_on_both: 'The file changed on both instances since they last agreed.',
  changed_here_deleted_there: 'The file changed on one instance and was deleted on the other.',
  apply_failed: 'A change from the peer could not be applied here.',
  move: 'The peer moved a file that changed here, or its destination is taken.',
  folder_move: 'The peer moved a folder that cannot be moved the same way here.',
};

const RESOLUTIONS: readonly { value: ConflictResolution; hint: string }[] = [
  {
    value: 'keep_local',
    hint: 'Synchronize this version and retain replaced content in ISM trash.',
  },
  {
    value: 'keep_remote',
    hint: "This instance takes the peer's version; the local copy goes to the trash.",
  },
  {
    value: 'keep_both',
    hint: 'Preserve both versions and synchronize the decision.',
  },
];

/** Replication conflicts, side by side, with the ways to settle them. */
export class IsmConflictsPage extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    conflicts: { state: true },
    includeResolved: { state: true },
    versions: { state: true },
  };

  declare conflicts: ConflictView[];
  declare includeResolved: boolean;
  declare versions: Map<string, RecoveryVersions>;
  protected override reloadOn = ['conflicts_changed'] as const;

  constructor() {
    super();
    this.conflicts = [];
    this.includeResolved = false;
    this.versions = new Map();
  }

  protected override async load() {
    this.conflicts = await admin.conflicts(this.includeResolved);
  }

  private async resolve(conflict: ConflictView, resolution: ConflictResolution) {
    const previous = this.versions.get(conflict.id);
    if (!(await this.compare(conflict))) return;
    const current = this.versions.get(conflict.id);
    if (!current || current.revision !== previous?.revision) {
      toast.warning('Versions changed. Review the refreshed comparison before choosing again.');
      return;
    }
    const choice = RESOLUTIONS.find((r) => r.value === resolution);
    const label = this.choiceLabel(conflict, resolution);
    const ok = await confirmAction({
      title: label,
      message: `${formatLocation(conflict.location)}\n\n${choice?.hint ?? ''}`,
      confirmLabel: label,
    });
    if (ok)
      await this.act(
        `${conflict.id}:${resolution}`,
        () =>
          admin.decideIssues({
            decisions: [
              {
                target: { kind: 'conflict', id: conflict.id, peer_id: null },
                revision: current.revision,
              },
            ],
            resolution,
          }),
        'Version decision queued.',
      );
  }

  private async compare(conflict: ConflictView) {
    return this.act('versions', async () => {
      const current = await admin.recoveryVersions({
        kind: 'conflict',
        id: conflict.id,
        peer_id: null,
      });
      this.versions = new Map(this.versions).set(conflict.id, current);
    });
  }

  private choiceLabel(conflict: ConflictView, resolution: ConflictResolution) {
    if (resolution === 'keep_both') return 'Keep both';
    return `Keep ${resolution === 'keep_local' ? conflict.local.instance : conflict.remote.instance}`;
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
    const choices = RESOLUTIONS;
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
          ${c.detail ? html`<details class="text-sm text-base-content/70"><summary class="cursor-pointer">Technical details</summary><p class="mt-1">${c.detail}</p></details>` : nothing}
          ${c.moved_from ? html`<p class="text-sm">Moved by the peer from <span class="break-all font-mono text-xs">${formatLocation(c.moved_from)}</span></p>` : nothing}
          ${
            this.versions.has(c.id)
              ? html`<ism-recovery-versions
            .target=${{ kind: 'conflict', id: c.id, peer_id: null }}
            .versions=${this.versions.get(c.id)}></ism-recovery-versions>`
              : !open
                ? html`<div class="grid grid-cols-1 gap-3 md:grid-cols-2">
              ${this.side('This instance', c.local)} ${this.side('Peer', c.remote)}
            </div>`
                : nothing
          }
          ${
            open
              ? html`<div class="card-actions">
                  <button class="btn btn-sm max-sm:min-h-11" ?disabled=${!!this.busy}
                    @click=${() => this.compare(c)}>Compare versions</button>
                  ${choices.map(
                    (r) => html`<div class="tooltip" data-tip=${r.hint}>
                      <button type="button" class="btn btn-sm max-sm:min-h-11" ?disabled=${this.busy !== undefined || !this.versions.has(c.id)} @click=${() => this.resolve(c, r.value)}>
                        ${this.spin(`${c.id}:${r.value}`)}${this.choiceLabel(c, r.value)}
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
              <p class="text-sm text-base-content/70">No conflicts have been recorded.</p>
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
