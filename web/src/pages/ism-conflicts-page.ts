import { html, nothing } from 'lit';
import { admin } from '@/api/admin';
import type { ConflictResolution } from '@/api/generated/ConflictResolution';
import type { ConflictSide } from '@/api/generated/ConflictSide';
import type { ConflictView } from '@/api/generated/ConflictView';
import type { RecoveryTarget } from '@/api/generated/RecoveryTarget';
import { IsmPage } from '@/base/ism-page';
import { confirmAction } from '@/components/confirm';
import { toast } from '@/components/toast';
import { VersionReview } from '@/components/version-review';
import { formatBytes, formatDate, formatLocation, humanize } from '@/util/format';
import { statusBadge } from '@/util/status';

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
    selected: { state: true },
  };

  declare conflicts: ConflictView[];
  declare includeResolved: boolean;
  /** Ids of selected open conflicts. */
  declare selected: Set<string>;
  private readonly review = new VersionReview(this);
  protected override reloadOn = ['conflicts_changed'] as const;

  constructor() {
    super();
    this.conflicts = [];
    this.includeResolved = false;
    this.selected = new Set();
  }

  protected override async load() {
    this.conflicts = await admin.conflicts(this.includeResolved);
    const open = new Set(this.open().map((c) => c.id));
    this.selected = new Set([...this.selected].filter((id) => open.has(id)));
  }

  private open() {
    return this.conflicts.filter((c) => c.status === 'open');
  }

  private chosen() {
    return this.open().filter((c) => this.selected.has(c.id));
  }

  private select(ids: readonly string[], checked: boolean) {
    const selected = new Set(this.selected);
    for (const id of ids) {
      if (checked) selected.add(id);
      else selected.delete(id);
    }
    this.selected = selected;
  }

  private compare(conflicts: readonly ConflictView[]) {
    return this.review.compare(conflicts.map(target));
  }

  /** Decides every reviewed conflict whose versions are still the ones the user compared. */
  private async decide(resolution: ConflictResolution, conflicts: readonly ConflictView[]) {
    const reviewed = conflicts.filter((c) => this.review.has(target(c)));
    if (!reviewed.length) return;
    const unchanged = new Set(
      (await this.review.compare(reviewed.map(target))).unchanged.map((t) => t.id),
    );
    const ready = reviewed.filter((c) => unchanged.has(c.id));
    if (!ready.length) {
      toast.warning('Versions changed. Review the refreshed comparison before choosing again.');
      return;
    }
    const label = this.choiceLabel(ready, resolution);
    const skipped = reviewed.length - ready.length;
    const hint = RESOLUTIONS.find((r) => r.value === resolution)?.hint ?? '';
    const ok = await confirmAction({
      title: ready.length === 1 ? label : `${label} for ${ready.length} conflicts`,
      message: `${ready.length === 1 ? `${formatLocation(ready[0].location)}\n\n` : ''}${hint}${
        skipped ? `\n\n${skipped} changed since review and stay open.` : ''
      }`,
      confirmLabel: label,
    });
    if (!ok) return;
    const done = await this.act(
      `decide:${resolution}`,
      () =>
        admin.decideIssues({
          decisions: ready.map((c) => {
            const versions = this.review.get(target(c));
            if (!versions) throw new Error('Current version evidence is unavailable.');
            return { target: target(c), revision: versions.revision };
          }),
          resolution,
        }),
      ready.length === 1 ? 'Version decision queued.' : `${ready.length} version decisions queued.`,
    );
    if (done)
      this.select(
        ready.map((c) => c.id),
        false,
      );
  }

  private choiceLabel(conflicts: readonly ConflictView[], resolution: ConflictResolution) {
    if (resolution === 'keep_both') return 'Keep both';
    const names = new Set(
      conflicts.map((c) => (resolution === 'keep_local' ? c.local.instance : c.remote.instance)),
    );
    if (names.size === 1) return `Keep ${[...names][0]}`;
    return resolution === 'keep_local' ? 'Keep this instance' : 'Keep peer';
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
            ${
              open
                ? html`<label class="flex h-11 w-11 items-center justify-center sm:h-auto sm:w-auto">
                    <input type="checkbox" class="checkbox checkbox-sm" aria-label=${`Select ${formatLocation(c.location)}`}
                      .checked=${this.selected.has(c.id)}
                      @change=${(e: Event) => this.select([c.id], (e.target as HTMLInputElement).checked)}></label>`
                : nothing
            }
            <h2 class="card-title break-all text-base">${formatLocation(c.location)}</h2>
            <span class=${statusBadge(c.status)}>${humanize(c.status)}</span>
          </div>
          <p class="text-sm text-base-content/70">
            ${c.share_name} · ${EXPLAIN[c.kind] ?? humanize(c.kind)} · ${formatDate(c.created_at)}
          </p>
          ${c.detail ? html`<details class="text-sm text-base-content/70"><summary class="cursor-pointer">Technical details</summary><p class="mt-1">${c.detail}</p></details>` : nothing}
          ${c.moved_from ? html`<p class="text-sm">Moved by the peer from <span class="break-all font-mono text-xs">${formatLocation(c.moved_from)}</span></p>` : nothing}
          ${
            open
              ? this.review.render(target(c))
              : html`<div class="grid grid-cols-1 gap-3 md:grid-cols-2">
              ${this.side('This instance', c.local)} ${this.side('Peer', c.remote)}
            </div>`
          }
          ${
            open
              ? html`<div class="card-actions">
                  <button class="btn btn-sm max-sm:min-h-11" ?disabled=${this.review.isLoading(target(c))}
                    @click=${() => this.compare([c])}>Compare versions</button>
                  ${choices.map(
                    (r) => html`<div class="tooltip" data-tip=${r.hint}>
                      <button type="button" class="btn btn-sm max-sm:min-h-11" ?disabled=${this.busy !== undefined || !this.review.has(target(c))} @click=${() => this.decide(r.value, [c])}>
                        ${this.choiceLabel([c], r.value)}
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

  private batch() {
    const open = this.open();
    if (!open.length) return nothing;
    const chosen = this.chosen();
    const reviewed = chosen.filter((c) => this.review.has(target(c))).length;
    const all = chosen.length === open.length;
    return html`<div class="flex flex-wrap items-center gap-2" aria-label="Batch conflict decisions">
      <label class="inline-flex items-center gap-2 max-sm:min-h-11">
        <input type="checkbox" class="checkbox checkbox-sm" aria-label="Select all open conflicts"
          .checked=${all} .indeterminate=${chosen.length > 0 && !all}
          @change=${(e: Event) =>
            this.select(
              open.map((c) => c.id),
              (e.target as HTMLInputElement).checked,
            )}>
        <span role="status" class="text-sm">${chosen.length} of ${open.length} selected</span>
      </label>
      ${
        chosen.length
          ? html`<button class="btn btn-sm max-sm:min-h-11" @click=${() => this.compare(chosen)}>
          ${this.review.busy ? html`<span class="loading loading-spinner loading-xs"></span>` : nothing}Compare selected versions</button>
        <span class="text-sm text-base-content/70">${reviewed} reviewed</span>
        ${RESOLUTIONS.map(
          (
            r,
          ) => html`<button class="btn btn-sm max-sm:min-h-11" ?disabled=${!!this.busy || !reviewed}
            @click=${() => this.decide(r.value, chosen)}>${this.spin(`decide:${r.value}`)}${this.choiceLabel(chosen, r.value)}</button>`,
        )}`
          : nothing
      }
    </div>`;
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
          : html`${this.batch()}${this.conflicts.map((c) => this.card(c))}`,
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

function target(conflict: ConflictView): RecoveryTarget {
  return { kind: 'conflict', id: conflict.id, peer_id: null };
}

customElements.define('ism-conflicts-page', IsmConflictsPage);

declare global {
  interface HTMLElementTagNameMap {
    'ism-conflicts-page': IsmConflictsPage;
  }
}
