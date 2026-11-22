import { html, nothing } from 'lit';
import { admin } from '@/api/admin';
import type { ConflictResolution } from '@/api/generated/ConflictResolution';
import type { RecoveryBatchResult } from '@/api/generated/RecoveryBatchResult';
import type { RecoveryState } from '@/api/generated/RecoveryState';
import type { RecoveryView } from '@/api/generated/RecoveryView';
import { IsmPage } from '@/base/ism-page';
import { confirmAction } from '@/components/confirm';
import type { PageChange } from '@/components/ism-pagination';
import '@/components/ism-pagination';
import { toast } from '@/components/toast';
import { targetKey as key, VersionReview } from '@/components/version-review';

const LABELS: Record<RecoveryState, string> = {
  queued: 'Fix queued',
  fixing: 'Fixing',
  waiting: 'Waiting',
  needs_decision: 'Choose a version',
  fixed: 'Fixed',
  not_retryable: 'Not retryable',
};

const BADGES: Record<RecoveryState, string> = {
  queued: 'badge badge-info badge-soft',
  fixing: 'badge badge-info badge-soft',
  waiting: 'badge badge-ghost',
  needs_decision: 'badge badge-warning badge-soft',
  fixed: 'badge badge-success badge-soft',
  not_retryable: 'badge badge-error badge-soft',
};

export class IsmRecoveryList extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    rows: { state: true },
    total: { state: true },
    limit: { state: true },
    offset: { state: true },
    selected: { state: true },
    result: { state: true },
  };

  declare rows: RecoveryView[];
  declare total: number;
  declare limit: number;
  declare offset: number;
  declare selected: Map<string, RecoveryView>;
  declare result: RecoveryBatchResult | undefined;
  private readonly review = new VersionReview(this);
  protected override reloadOn = ['sync_changed', 'operation_updated', 'conflicts_changed'] as const;
  protected override liveReloadInterval = 1000;

  constructor() {
    super();
    this.rows = [];
    this.total = 0;
    this.limit = 250;
    this.offset = 0;
    this.selected = new Map();
  }

  protected override async load() {
    const { limit, offset } = this;
    const page = await admin.recoveryPage(limit, offset);
    if (limit !== this.limit || offset !== this.offset) return;
    this.rows = page.rows;
    this.total = page.total;
    const selected = new Map(this.selected);
    for (const row of page.rows) {
      if (selected.has(key(row.target))) selected.set(key(row.target), row);
    }
    this.selected = selected;
  }

  private select(row: RecoveryView, checked: boolean) {
    const selected = new Map(this.selected);
    if (checked) selected.set(key(row.target), row);
    else selected.delete(key(row.target));
    this.selected = selected;
  }

  private async fix(all = false, targets = [...this.selected.values()].map((row) => row.target)) {
    const ok = await this.act(
      'fix',
      async () => {
        this.result = await admin.fixIssues({ targets: all ? [] : targets, all, resolution: null });
      },
      'Recovery queued. Independent sync continues.',
    );
    if (ok) this.selected = new Map();
  }

  private choiceLabel(resolution: ConflictResolution, rows = [...this.selected.values()]) {
    if (resolution === 'keep_both') return 'Keep both';
    const names = [
      ...new Set(
        this.conflictRows(rows).map((row) =>
          resolution === 'keep_local' ? row.local_instance : row.peer_instance,
        ),
      ),
    ].filter(Boolean);
    return names.length === 1
      ? `Keep ${names[0]}`
      : resolution === 'keep_local'
        ? 'Keep receiving instances'
        : 'Keep source peers';
  }

  private async decide(resolution: ConflictResolution, rows = [...this.selected.values()]) {
    const ready = this.conflictRows(rows)
      .map((row) => row.target)
      .filter((target) => this.review.has(target));
    if (!ready.length) return;
    const decisions = ready.map((target) => {
      const versions = this.review.get(target);
      if (!versions) throw new Error('Current version evidence is unavailable.');
      return { target, revision: versions.revision };
    });
    const label = this.choiceLabel(resolution, rows);
    if (
      !(await confirmAction({
        title: `${label} for ${ready.length} ${ready.length === 1 ? 'conflict' : 'conflicts'}`,
        message: 'ISM will preserve replaced content in its trash and verify each managed change.',
        confirmLabel: label,
      }))
    )
      return;
    const ok = await this.act('decision', async () => {
      this.result = await admin.decideIssues({
        decisions,
        resolution,
      });
    });
    if (ok) {
      const selected = new Map(this.selected);
      for (const item of this.result?.items ?? []) {
        if (['queued', 'fixing', 'fixed'].includes(item.state)) selected.delete(key(item.target));
      }
      const admitted = this.selected.size - selected.size;
      this.selected = selected;
      if (admitted) toast.success(`${admitted} decisions queued for verified recovery.`);
    }
  }

  private conflictRows(rows = [...this.selected.values()]) {
    return rows.filter((row) => row.target.kind === 'conflict' && row.state === 'needs_decision');
  }

  private compare(rows: RecoveryView[]) {
    return this.review.compare(rows.map((row) => row.target));
  }

  protected override render() {
    const selectedConflicts = this.conflictRows().length;
    const reviewed = this.conflictRows().filter((row) => this.review.has(row.target)).length;
    const canFixSelected = [...this.selected.values()].some((row) => row.can_fix);
    const visible = new Set(this.rows.map((row) => key(row.target)));
    const offPage = this.conflictRows().filter((row) => !visible.has(key(row.target)));
    return this.frame(
      () => html`
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div><h2 class="text-base font-semibold">Sync issues</h2>
          <p class="text-sm text-base-content/70">Independent sync continues while these issues are recovered.</p></div>
        <div class="flex flex-wrap items-center gap-2">
          <span role="status" class="text-sm">${this.selected.size} selected</span>
          <button class="btn btn-primary btn-sm max-sm:min-h-11" ?disabled=${!!this.busy || !canFixSelected}
            @click=${() => this.fix()}>${this.spin('fix')}Fix selected</button>
          <button class="btn btn-sm max-sm:min-h-11" ?disabled=${!!this.busy || !this.total}
            @click=${() => this.fix(true)}>Fix all ${this.total} issues</button>
        </div>
      </div>
      ${
        selectedConflicts
          ? html`<div class="flex flex-wrap items-center gap-2" aria-label="Batch conflict decisions">
        <span class="text-sm">${selectedConflicts} conflicts:</span>
        <button class="btn btn-sm max-sm:min-h-11" @click=${() => this.compare(this.conflictRows())}>
          ${this.review.busy ? html`<span class="loading loading-spinner loading-xs"></span>` : nothing}Compare selected versions</button>
        <span class="text-sm text-base-content/70">${reviewed} reviewed</span>
        <button class="btn btn-sm max-sm:min-h-11" ?disabled=${!!this.busy || !reviewed} @click=${() => this.decide('keep_local')}>${this.spin('decision')}${this.choiceLabel('keep_local')}</button>
        <button class="btn btn-sm max-sm:min-h-11" ?disabled=${!!this.busy || !reviewed} @click=${() => this.decide('keep_remote')}>${this.choiceLabel('keep_remote')}</button>
        <button class="btn btn-sm max-sm:min-h-11" ?disabled=${!!this.busy || !reviewed} @click=${() => this.decide('keep_both')}>Keep both</button>
      </div>`
          : nothing
      }
      ${
        offPage.length
          ? html`<details>
          <summary class="cursor-pointer font-medium">Review ${offPage.length} selected conflicts from other pages</summary>
          <div class="mt-3 space-y-4" aria-label="Selected conflicts on other pages">
            ${offPage.map(
              (row) => html`<div class="min-w-0">
                <div class="flex flex-wrap items-center justify-between gap-2">
                  <h3 class="break-all font-medium">${row.title}</h3>
                  <button class="btn btn-ghost btn-sm max-sm:min-h-11" @click=${() => this.select(row, false)}>Remove selection</button>
                </div>
                ${this.review.render(row.target)}
              </div>`,
            )}
          </div>
        </details>`
          : nothing
      }
      ${
        this.result
          ? html`<div role="status" class="alert alert-info alert-soft">
        <span>${this.result.items.filter((row) => row.state === 'fixed').length} fixed ·
          ${this.result.items.filter((row) => row.state === 'queued' || row.state === 'fixing').length} queued or fixing ·
          ${this.result.items.filter((row) => row.state === 'waiting').length} waiting ·
          ${this.result.items.filter((row) => row.state === 'needs_decision').length} need a decision ·
          ${this.result.items.filter((row) => row.state === 'not_retryable').length} not retryable</span>
      </div>`
          : nothing
      }
      <div class="overflow-x-auto">
        <table class="table table-sm max-sm:block">
          <thead class="max-sm:block"><tr class="max-sm:block">
            <th class="max-sm:block max-sm:px-0"><label class="inline-flex items-center gap-2 max-sm:min-h-11"><input type="checkbox" class="checkbox checkbox-sm" aria-label="Select this page"
              .checked=${this.rows.length > 0 && this.rows.every((row) => this.selected.has(key(row.target)))}
              .indeterminate=${this.rows.some((row) => this.selected.has(key(row.target))) && !this.rows.every((row) => this.selected.has(key(row.target)))}
              @change=${(event: Event) => {
                const checked = (event.target as HTMLInputElement).checked;
                for (const row of this.rows) this.select(row, checked);
              }}><span class="sm:hidden">Select this page</span></label></th>
            <th class="max-sm:hidden">File or operation</th><th class="max-sm:hidden">Recovery</th><th class="max-sm:hidden">Action</th>
          </tr></thead>
          <tbody class="max-sm:block">${this.rows.map(
            (
              row,
            ) => html`<tr class="max-sm:grid max-sm:grid-cols-[2.75rem_minmax(0,1fr)] max-sm:py-2">
            <td class="max-sm:row-span-3 max-sm:p-0"><label class="flex h-11 w-11 items-center justify-center sm:h-auto sm:w-auto"><input type="checkbox" class="checkbox checkbox-sm"
              aria-label=${`Select ${row.title}`}
              .checked=${this.selected.has(key(row.target))}
              @change=${(event: Event) => this.select(row, (event.target as HTMLInputElement).checked)}></label></td>
            <td class="max-w-96 break-all max-sm:max-w-none max-sm:px-2 max-sm:py-1">
              <span class="font-medium">${row.title}</span>
              ${row.local_instance ? html`<p class="text-sm text-base-content/70">${row.local_instance}${row.peer_instance ? html` / ${row.peer_instance}` : nothing}</p>` : nothing}
              ${this.review.render(row.target)}
              ${
                row.detail || row.code
                  ? html`<details class="mt-1 text-xs text-base-content/70">
                <summary class="cursor-pointer">Technical details</summary>
                <p class="mt-1 break-all">${row.code}${row.detail ? html`: ${row.detail}` : nothing}</p>
              </details>`
                  : nothing
              }
            </td>
            <td class="max-sm:col-start-2 max-sm:px-2 max-sm:py-1"><span class="${BADGES[row.state]} whitespace-nowrap">${LABELS[row.state]}</span></td>
            <td class="max-sm:col-start-2 max-sm:px-2 max-sm:py-1">${
              row.can_fix
                ? html`<button class="btn btn-primary btn-sm max-sm:min-h-11" ?disabled=${!!this.busy}
              @click=${() => this.fix(false, [row.target])}>Fix</button>`
                : row.state === 'needs_decision'
                  ? row.target.kind === 'conflict'
                    ? html`<div class="flex flex-wrap gap-2">
                      <button class="btn btn-sm max-sm:min-h-11" ?disabled=${this.review.isLoading(row.target)}
                        @click=${() => this.compare([row])}>Compare versions</button>
                      <button class="btn btn-sm max-sm:min-h-11" ?disabled=${!!this.busy || !this.review.has(row.target)} @click=${() => this.decide('keep_local', [row])}>${this.choiceLabel('keep_local', [row])}</button>
                      <button class="btn btn-sm max-sm:min-h-11" ?disabled=${!!this.busy || !this.review.has(row.target)} @click=${() => this.decide('keep_remote', [row])}>${this.choiceLabel('keep_remote', [row])}</button>
                      <button class="btn btn-sm max-sm:min-h-11" ?disabled=${!!this.busy || !this.review.has(row.target)} @click=${() => this.decide('keep_both', [row])}>Keep both</button>
                    </div>`
                    : html`<span class="text-sm">Awaiting its version decision</span>`
                  : nothing
            }</td>
          </tr>`,
          )}
          ${this.rows.length ? nothing : html`<tr><td colspan="4" class="p-6 text-center">No outstanding sync issues.</td></tr>`}
          </tbody>
        </table>
      </div>
      <ism-pagination .total=${this.total} .limit=${this.limit} .offset=${this.offset}
        @page-change=${(event: CustomEvent<PageChange>) => {
          this.limit = event.detail.limit;
          this.offset = event.detail.offset;
          void this.reload();
        }}></ism-pagination>
    `,
    );
  }
}

customElements.define('ism-recovery-list', IsmRecoveryList);

declare global {
  interface HTMLElementTagNameMap {
    'ism-recovery-list': IsmRecoveryList;
  }
}
