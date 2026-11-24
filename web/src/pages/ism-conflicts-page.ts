import { html, nothing } from 'lit';
import { admin } from '@/api/admin';
import type { ConflictPageRow } from '@/api/generated/ConflictPageRow';
import type { ConflictResolution } from '@/api/generated/ConflictResolution';
import type { RecoveryTarget } from '@/api/generated/RecoveryTarget';
import { IsmPage } from '@/base/ism-page';
import { confirmAction } from '@/components/confirm';
import type { PageChange } from '@/components/ism-pagination';
import '@/components/ism-pagination';
import { toast } from '@/components/toast';
import { targetKey, VersionReview } from '@/components/version-review';
import { formatDate, formatLocation, humanize } from '@/util/format';
import { statusBadge } from '@/util/status';

const EXPLAIN: Readonly<Record<string, string>> = {
  changed_on_both: 'The file changed on both instances since they last agreed.',
  changed_here_deleted_there: 'The file changed on one instance and was deleted on the other.',
  deleted_here_changed_there: 'One instance has the file; the other no longer does.',
  version_history_mismatch: 'The saved history disagrees. Choose which version to keep.',
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
    hint: "The receiving instance takes the source peer's version; replaced content goes to the trash.",
  },
  {
    value: 'keep_both',
    hint: 'Preserve both versions and synchronize the decision.',
  },
];
const MAX_OFF_PAGE_REVIEWS = 50;

/** A paginated conflict queue with an automatically prepared review for the selected item. */
export class IsmConflictsPage extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    conflicts: { state: true },
    includeResolved: { state: true },
    total: { state: true },
    limit: { state: true },
    offset: { state: true },
    selected: { state: true },
    activeId: { state: true },
  };

  declare conflicts: ConflictPageRow[];
  declare includeResolved: boolean;
  declare total: number;
  declare limit: number;
  declare offset: number;
  /** Selected conflicts, including selections on other pages. */
  declare selected: Map<string, ConflictPageRow>;
  declare activeId: string | undefined;
  private readonly review = new VersionReview(this);
  protected override reloadOn = ['conflicts_changed', 'sync_changed'] as const;
  protected override liveReloadInterval = 1500;

  constructor() {
    super();
    this.conflicts = [];
    this.includeResolved = false;
    this.total = 0;
    this.limit = 50;
    this.offset = 0;
    this.selected = new Map();
    this.activeId = undefined;
  }

  protected override onChange(event: Parameters<IsmPage['onChange']>[0]) {
    const conflicts = new Map(
      [...this.selected.values(), ...this.conflicts]
        .filter(canChoose)
        .map((conflict) => [key(conflict), conflict]),
    );
    const targets = this.preparationTargets(this.conflicts);
    const retained = new Set(targets.map(targetKey));
    this.review.retain(targets);
    if (event.type === 'resync' || event.type === 'conflicts_changed') {
      for (const item of conflicts.values()) {
        const recoveryTarget = target(item);
        this.review.forget(recoveryTarget, retained.has(targetKey(recoveryTarget)));
      }
      if (targets.length) void this.review.compare(targets);
    } else if (event.type === 'sync_changed') {
      this.review.retryUnavailable(targets);
    }
    super.onChange(event);
  }

  protected override async load() {
    const { limit, offset } = this;
    const page = await admin.conflicts(this.includeResolved, limit, offset);
    if (limit !== this.limit || offset !== this.offset) return;
    if (offset > 0 && offset >= page.total) {
      this.offset = Math.max(0, Math.ceil(page.total / limit) - 1) * limit;
      void this.reload();
      return;
    }
    const previousConflicts = new Map(this.conflicts.map((conflict) => [key(conflict), conflict]));
    for (const conflict of page.rows) {
      const previous = previousConflicts.get(key(conflict));
      if (
        recovery(conflict)?.state === 'needs_decision' &&
        recovery(conflict)?.code === 'version_conflict' &&
        (previous === undefined ||
          recovery(previous)?.state !== 'needs_decision' ||
          recovery(previous)?.code !== 'version_conflict')
      ) {
        this.review.forget(target(conflict));
      }
    }
    this.conflicts = page.rows;
    this.total = page.total;
    this.review.retain(this.preparationTargets(page.rows));
    if (!page.rows.some((conflict) => key(conflict) === this.activeId)) {
      const active = page.rows.find(canChoose) ?? page.rows[0];
      this.activeId = active ? key(active) : undefined;
    }
    const needsReview = page.rows.filter(canChoose).filter((conflict) => {
      const key = targetKey(target(conflict));
      return (
        !this.review.has(target(conflict)) &&
        !this.review.errors.has(key) &&
        !this.review.isLoading(target(conflict))
      );
    });
    if (needsReview.length) void this.review.compare(needsReview.map(target));
    const selected = new Map(this.selected);
    for (const conflict of page.rows) {
      const id = key(conflict);
      if (!selected.has(id)) continue;
      if (canChoose(conflict)) selected.set(id, conflict);
      else selected.delete(id);
    }
    this.selected = selected;
  }

  private page(change: PageChange) {
    this.limit = change.limit;
    this.offset = change.offset;
    void this.reload();
  }

  private open() {
    return this.conflicts.filter(canChoose);
  }

  private activeConflict() {
    return this.conflicts.find((conflict) => key(conflict) === this.activeId);
  }

  private chosen() {
    return [...this.selected.values()];
  }

  private preparationTargets(visible: readonly ConflictPageRow[]) {
    const current = new Map(visible.filter(canChoose).map((conflict) => [key(conflict), conflict]));
    const other = [...this.selected.values()]
      .filter((conflict) => canChoose(conflict) && !current.has(key(conflict)))
      .slice(0, MAX_OFF_PAGE_REVIEWS);
    return [...current.values(), ...other].map(target);
  }

  private select(ids: readonly string[], checked: boolean) {
    const selected = new Map(this.selected);
    const open = checked ? new Map(this.open().map((conflict) => [key(conflict), conflict])) : null;
    for (const id of ids) {
      if (checked) {
        const conflict = open?.get(id);
        if (conflict) selected.set(id, conflict);
      } else {
        selected.delete(id);
      }
    }
    this.selected = selected;
  }

  /** Queue the reviewed revisions; recovery revalidates them before changing any files. */
  private async decide(resolution: ConflictResolution, conflicts: readonly ConflictPageRow[]) {
    const ready = conflicts.filter((c) => this.review.has(target(c)));
    if (!ready.length) return;
    const decisions = ready.map((c) => {
      const versions = this.review.get(target(c));
      if (!versions) throw new Error('Current version evidence is unavailable.');
      return { target: target(c), revision: versions.revision };
    });
    const label = this.choiceLabel(ready, resolution);
    const hint = RESOLUTIONS.find((r) => r.value === resolution)?.hint ?? '';
    const notReady = conflicts.length - ready.length;
    const ok = await confirmAction({
      title: ready.length === 1 ? label : `${label} for ${ready.length} conflicts`,
      message: `${ready.length === 1 ? `${title(ready[0])}\n\n` : ''}${hint}${
        notReady
          ? `\n\n${notReady} selected conflicts are not ready and will remain selected. Revisit their pages to refresh or retry their evidence.`
          : ''
      }`,
      confirmLabel: label,
    });
    if (!ok) return;
    const admitted: string[] = [];
    const done = await this.act(`decide:${resolution}`, async () => {
      const result = await admin.decideIssues({ decisions, resolution });
      for (const item of result.items) {
        if (['queued', 'fixing', 'fixed'].includes(item.state))
          admitted.push(targetKey(item.target));
      }
      const deferred = result.items.find((item) => !admitted.includes(targetKey(item.target)));
      if (deferred)
        toast.warning(deferred.detail ?? 'Some decisions could not be queued and remain selected.');
    });
    if (done) {
      this.select(admitted, false);
      if (admitted.length)
        toast.success(
          admitted.length === 1
            ? 'Version decision queued.'
            : `${admitted.length} version decisions queued.`,
        );
      if (notReady)
        toast.warning(
          `${notReady} selected conflicts still need evidence. Revisit their pages to refresh or retry.`,
        );
    }
  }

  private choiceLabel(conflicts: readonly ConflictPageRow[], resolution: ConflictResolution) {
    if (resolution === 'keep_both') return 'Keep both';
    const names = new Set(
      conflicts.flatMap((c) => {
        const versions = this.review.get(target(c));
        if (!versions) return [];
        return resolution === 'keep_local'
          ? versions.receiving.map((side) => side.instance)
          : [versions.source.instance];
      }),
    );
    if (names.size === 1) return `Keep ${[...names][0]}`;
    return resolution === 'keep_local' ? 'Keep receiving instances' : 'Keep source peers';
  }

  private queueItem(c: ConflictPageRow) {
    const open = canChoose(c);
    const status = conflictStatus(c);
    return html`
      <li class="border-b border-base-300 last:border-b-0">
        <div class=${`flex items-start gap-2 px-3 py-3 ${key(c) === this.activeId ? 'bg-primary/10' : ''}`}>
          ${
            open
              ? html`<input
                  type="checkbox"
                  class="checkbox checkbox-sm mt-1"
                  aria-label=${`Select ${title(c)}`}
                  .checked=${this.selected.has(key(c))}
                  @change=${(e: Event) =>
                    this.select([key(c)], (e.target as HTMLInputElement).checked)}
                />`
              : nothing
          }
          <button
            type="button"
            class="min-w-0 flex-1 text-left"
            aria-current=${key(c) === this.activeId ? 'true' : nothing}
            @click=${() => {
              this.activeId = key(c);
            }}
          >
            <span class="block break-words font-medium">${title(c)}</span>
            <span class="mt-1 block text-xs text-base-content/70">${c.owner === 'local' ? c.conflict.share_name : (c.conflict.local_instance ?? 'Peer')}</span>
            <span class=${`${statusBadge(status.tone)} mt-2`}>${status.label}</span>
          </button>
        </div>
      </li>
    `;
  }

  private detail(c: ConflictPageRow) {
    const recoveryTarget = target(c);
    const open = canChoose(c);
    const ready = this.review.has(recoveryTarget);
    return html`
      <article class="min-w-0 space-y-4 p-4 sm:p-6">
        <header class="space-y-2 border-b border-base-300 pb-4">
          <div class="flex flex-wrap items-center gap-2">
            <span class=${statusBadge(conflictStatus(c).tone)}>${conflictStatus(c).label}</span>
            <span class="text-sm text-base-content/70">${c.owner === 'local' ? html`${c.conflict.share_name} · ${formatDate(c.conflict.created_at)}` : (c.conflict.local_instance ?? 'Peer')}</span>
          </div>
          <h2 class="break-words text-xl font-semibold">${title(c)}</h2>
          ${c.owner === 'local' ? html`<p class="text-sm text-base-content/70">${EXPLAIN[c.conflict.kind] ?? humanize(c.conflict.kind)}</p>` : nothing}
          ${c.owner === 'local' && c.conflict.moved_from ? html`<p class="text-sm">The peer moved this from <span class="break-all font-medium">${formatLocation(c.conflict.moved_from)}</span>.</p>` : nothing}
        </header>
        ${
          open
            ? html`${this.review.render(recoveryTarget, {
                disabled: !!this.busy || !ready,
                receivingLabel: this.choiceLabel([c], 'keep_local'),
                sourceLabel: this.choiceLabel([c], 'keep_remote'),
                choose: (source: boolean) =>
                  this.decide(source ? 'keep_remote' : 'keep_local', [c]),
              })}
                <section class="flex flex-wrap items-center gap-3 border-t border-base-300 pt-4">
                  <button class="btn btn-primary max-sm:min-h-11" ?disabled=${!!this.busy || !ready}
                    @click=${() => this.decide('keep_both', [c])}>Keep both</button>
                  <p class="text-sm text-base-content/70">This choice will sync to the other instance. Replaced files are kept in ISM trash.</p>
                </section>`
            : html`<div role="status" class="rounded-box bg-base-200 p-4">
                ${
                  c.owner === 'local' && c.conflict.status === 'resolved'
                    ? html`<p class="font-medium">This conflict has been resolved.</p>`
                    : recovery(c)?.state === 'waiting'
                      ? html`<p class="font-medium">Waiting for work to continue.</p>
                          <a class="link link-primary mt-2 inline-flex" href="/sync">See what is waiting on Sync</a>`
                      : html`<p class="font-medium">This conflict needs attention.</p>
                          <a class="link link-primary mt-2 inline-flex" href="/sync">Open Sync for repair options</a>`
                }
              </div>`
        }
      </article>
    `;
  }

  private batch() {
    const open = this.open();
    const chosen = this.chosen();
    if (!open.length && !chosen.length) return nothing;
    const chosenHere = open.filter((conflict) => this.selected.has(key(conflict)));
    const reviewed = chosen.filter((c) => this.review.has(target(c))).length;
    const all = open.length > 0 && chosenHere.length === open.length;
    return html`<div class="flex flex-wrap items-center gap-2" aria-label="Batch conflict decisions">
      <label class="inline-flex items-center gap-2 max-sm:min-h-11">
        <input type="checkbox" class="checkbox checkbox-sm" aria-label="Select all open conflicts"
          ?disabled=${open.length === 0} .checked=${all} .indeterminate=${chosenHere.length > 0 && !all}
          @change=${(e: Event) =>
            this.select(open.map(key), (e.target as HTMLInputElement).checked)}>
        <span role="status" class="text-sm">${chosenHere.length} of ${open.length} selected${chosen.length > chosenHere.length ? html` · ${chosen.length} total selected` : nothing}</span>
      </label>
      ${
        chosen.length
          ? html`<span class="text-sm text-base-content/70">${reviewed} of ${chosen.length} ready</span>
        ${chosen.length > MAX_OFF_PAGE_REVIEWS && reviewed < chosen.length ? html`<span class="text-sm text-base-content/70">Revisit other pages to refresh remaining selections.</span>` : nothing}
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
    const active = this.activeConflict();
    return this.frame(
      () =>
        this.conflicts.length === 0
          ? html`<div class="card card-border bg-base-100"><div class="card-body items-center text-center">
              <ism-icon name="circle-check" size="xl" class="text-success"></ism-icon>
              <h2 class="card-title">No conflicts</h2>
              <p class="text-sm text-base-content/70">No conflicts have been recorded.</p>
            </div></div>${this.batch()}`
          : html`${this.batch()}
              <section class="grid min-h-[32rem] min-w-0 grid-cols-1 overflow-hidden rounded-box border border-base-300 bg-base-100 lg:grid-cols-[minmax(16rem,20rem)_minmax(0,1fr)]">
                <aside class="flex min-h-0 flex-col border-b border-base-300 lg:border-b-0 lg:border-r">
                  <div class="flex items-center justify-between gap-2 border-b border-base-300 px-3 py-3">
                    <h2 class="font-semibold">Review queue</h2>
                    <span class="badge badge-ghost">${this.total}</span>
                  </div>
                  <ul tabindex="0" aria-label="Conflicts on this page" aria-describedby=${this.conflicts.length > 1 ? 'queue-scroll-hint' : nothing} class="max-h-24 min-h-0 flex-1 overflow-y-auto lg:max-h-none">${this.conflicts.map((c) => this.queueItem(c))}</ul>
                  ${this.conflicts.length > 1 ? html`<p id="queue-scroll-hint" class="px-3 py-1 text-xs text-base-content/70 lg:hidden">Scroll to see other conflicts on this page.</p>` : nothing}
                  <div class="border-t border-base-300 p-2">
                    <ism-pagination
                      .total=${this.total}
                      .limit=${this.limit}
                      .offset=${this.offset}
                      @page-change=${(e: CustomEvent<PageChange>) => this.page(e.detail)}
                    ></ism-pagination>
                  </div>
                </aside>
                <div class="min-w-0">
                  ${
                    active
                      ? this.detail(active)
                      : html`<p class="p-6 text-sm text-base-content/70">Select a conflict to review.</p>`
                  }
                </div>
              </section>`,
      html`<div class="flex flex-wrap items-center gap-3">
        <label class="label">
          <input
            type="checkbox"
            class="toggle toggle-sm"
            .checked=${this.includeResolved}
            @change=${(e: Event) => {
              this.includeResolved = (e.target as HTMLInputElement).checked;
              this.offset = 0;
              void this.reload();
            }}
          />
          Show resolved
        </label>
      </div>`,
    );
  }
}

function target(row: ConflictPageRow): RecoveryTarget {
  return row.owner === 'local'
    ? { kind: 'conflict', id: row.conflict.id, peer_id: null }
    : row.conflict.target;
}

function key(row: ConflictPageRow): string {
  return targetKey(target(row));
}

function recovery(row: ConflictPageRow) {
  return row.owner === 'local' ? row.conflict.recovery : row.conflict;
}

function title(row: ConflictPageRow): string {
  return row.owner === 'local' ? formatLocation(row.conflict.location) : row.conflict.title;
}

function canChoose(row: ConflictPageRow): boolean {
  return (
    (row.owner === 'peer' || row.conflict.status === 'open') &&
    (!recovery(row) || recovery(row)?.state === 'needs_decision')
  );
}

function conflictStatus(row: ConflictPageRow): { label: string; tone: string } {
  if (row.owner === 'local' && row.conflict.status === 'resolved')
    return { label: 'Resolved', tone: 'resolved' };
  const issue = recovery(row);
  switch (issue?.state) {
    case 'queued':
      return { label: 'Decision queued', tone: 'queued' };
    case 'fixing':
      return { label: 'Applying decision', tone: 'resolving' };
    case 'waiting':
      return {
        label: ['peer_evidence_unavailable', 'peer_unreachable', 'peer_busy'].includes(
          issue.code ?? '',
        )
          ? 'Waiting for the other instance'
          : 'Waiting to continue',
        tone: 'queued',
      };
    case 'needs_decision':
      return { label: 'Needs review', tone: 'open' };
    case 'fixed':
      return { label: 'Checking completion', tone: 'resolving' };
    case 'not_retryable':
      return { label: 'Needs attention', tone: 'blocked' };
    default:
      return row.owner === 'local'
        ? { label: humanize(row.conflict.status), tone: row.conflict.status }
        : { label: 'Needs attention', tone: 'blocked' };
  }
}

customElements.define('ism-conflicts-page', IsmConflictsPage);

declare global {
  interface HTMLElementTagNameMap {
    'ism-conflicts-page': IsmConflictsPage;
  }
}
