import { html, nothing } from 'lit';
import { admin, type RecoveryFilters } from '@/api/admin';
import type { ConflictFilter } from '@/api/generated/ConflictFilter';
import type { RecoveryBatchResult } from '@/api/generated/RecoveryBatchResult';
import type { RecoveryState } from '@/api/generated/RecoveryState';
import type { RecoveryView } from '@/api/generated/RecoveryView';
import { IsmPage } from '@/base/ism-page';
import type { PageChange } from '@/components/ism-pagination';
import '@/components/ism-pagination';
import { targetKey as key } from '@/components/version-review';
import { linkTo } from '@/stores/nav';
import { humanize } from '@/util/format';

const LABELS: Record<RecoveryState, string> = {
  queued: 'Fix queued',
  fixing: 'Fixing',
  waiting: 'Waiting',
  needs_decision: 'Choose a version',
  fixed: 'Fixed',
  not_retryable: "Can't fix automatically",
};

const BADGES: Record<RecoveryState, string> = {
  queued: 'badge badge-info badge-soft',
  fixing: 'badge badge-info badge-soft',
  waiting: 'badge badge-ghost',
  needs_decision: 'badge badge-warning badge-soft',
  fixed: 'badge badge-success badge-soft',
  not_retryable: 'badge badge-error badge-soft',
};

const ISSUE_TYPES: Readonly<Record<string, string>> = {
  choose_version: 'Version choice needed',
  version_conflict: 'Files changed after review',
  peer_unreachable: 'Other instance unreachable',
  peer_recovery_unavailable: 'Recovery information unavailable',
  peer_evidence_unavailable: 'Version information unavailable',
  peer_share_changed: 'Shared folder changed',
  root_unavailable: 'Shared storage unavailable',
  maintenance_blocked: 'Server maintenance',
  direct_writes_unavailable: 'Direct writes unavailable',
  peer_busy: 'Other instance busy',
  recovery_plan_unchanged: 'Fix needs new information',
  recovery_worker_failed: 'Fix stopped unexpectedly',
  invalid_saved_recovery: 'Unreadable saved issue',
  recovery_exhausted: 'No complete copy available',
  recovery_evidence_missing: 'Missing file information',
  recovery_evidence_changed: 'File information changed',
  revision_changed: 'File revision changed',
  recovery_changed: 'Recovery changed',
  peer_content_changed: 'Other instance content changed',
  awaiting_receiver: 'Waiting for the other instance',
  awaiting_completion: 'Fix in progress',
};

function issueTypeLabel(value: string): string {
  if (value === 'none') return 'No specific reason';
  const code = value.startsWith('code:') ? value.slice(5) : value;
  return Object.hasOwn(ISSUE_TYPES, code) ? ISSUE_TYPES[code] : humanize(code);
}

function issueGuidance(row: RecoveryView): string {
  if (row.state === 'fixing') return 'ISM is checking and fixing this issue.';
  if (row.state === 'needs_decision') {
    if (row.target.kind !== 'conflict') {
      return 'A related file needs a version choice. Find it on the Conflicts page.';
    }
    return row.code === 'version_conflict'
      ? 'The files changed. Review them again on the Conflicts page.'
      : 'Choose which version to keep on the Conflicts page.';
  }
  switch (row.code) {
    case 'peer_unreachable':
      return "Can't reach the other instance. Check that it is running and connected; ISM will try again.";
    case 'peer_recovery_unavailable':
    case 'peer_evidence_unavailable':
      return "Can't get the information needed from the other instance. Check its connection; ISM will try again.";
    case 'peer_share_changed':
      return 'This folder is no longer shared with the other instance. Check its sharing settings.';
    case 'root_unavailable':
      return 'The shared folder is unavailable. Check that its storage is mounted and its folder mapping is correct.';
    case 'maintenance_blocked':
    case 'direct_writes_unavailable':
      return "Immich isn't ready for file changes. Check server maintenance for the reason.";
    case 'peer_busy':
      return 'The other instance is busy. ISM will try again.';
    case 'recovery_plan_unchanged':
      return 'ISM tried the same fix several times without success. Check Technical details for what needs to change.';
    case 'recovery_worker_failed':
      return 'The fix stopped unexpectedly. Select this issue and choose Fix selected to try again.';
    case 'invalid_saved_recovery':
      return "ISM can't read this saved issue. Check the server logs; it can't be fixed from this page.";
    case 'recovery_exhausted':
      return "ISM couldn't find a complete copy. Restore this file from a backup if you have one.";
    case 'recovery_evidence_missing':
      return "ISM can't verify this file yet. Make sure both instances are online; it will check again.";
    case 'recovery_evidence_changed':
    case 'revision_changed':
    case 'recovery_changed':
    case 'peer_content_changed':
      return 'The file changed while ISM was checking it. ISM will check again.';
    case 'awaiting_receiver':
      return 'The other instance is checking this issue.';
    case 'awaiting_completion':
      return 'The fix is running. ISM is waiting for it to finish.';
    default:
      if (row.state === 'queued') return 'ISM is preparing the fix.';
      if (row.state === 'fixed') return 'This issue has been fixed.';
      if (row.state === 'not_retryable') return "ISM can't fix this issue automatically.";
      return row.code || row.detail
        ? "ISM couldn't finish checking this issue. If it stays here, check Technical details."
        : 'ISM will check this issue again.';
  }
}

export class IsmRecoveryList extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    rows: { state: true },
    total: { state: true },
    limit: { state: true },
    offset: { state: true },
    selected: { state: true },
    result: { state: true },
    filters: { state: true },
    issueTypes: { state: true },
    filtersOpen: { state: true },
    filtering: { state: true },
    share: { state: true },
    folder: { state: true },
    issueType: { state: true },
    status: { state: true },
    kind: { state: true },
  };

  declare rows: RecoveryView[];
  declare total: number;
  declare limit: number;
  declare offset: number;
  declare selected: Map<string, RecoveryView>;
  declare result: RecoveryBatchResult | undefined;
  declare filters: ConflictFilter[];
  declare issueTypes: Array<string | null>;
  declare filtersOpen: boolean;
  declare filtering: boolean;
  declare share: string;
  declare folder: string;
  declare issueType: string;
  declare status: string;
  declare kind: string;

  constructor() {
    super();
    this.rows = [];
    this.total = 0;
    this.limit = 25;
    this.offset = 0;
    this.selected = new Map();
    this.filters = [];
    this.issueTypes = [];
    this.filtersOpen = false;
    this.filtering = false;
    this.share = '';
    this.folder = '';
    this.issueType = '';
    this.status = '';
    this.kind = '';
  }

  private filterQuery(): RecoveryFilters {
    return {
      share: this.share,
      folder: this.folder,
      issue_type: this.issueType,
      status: this.status,
      kind: this.kind,
    };
  }

  private filterCount() {
    return [this.share, this.folder, this.issueType, this.status, this.kind].filter(Boolean).length;
  }

  protected override async load() {
    const { limit, offset } = this;
    const filters = this.filterQuery();
    const page = await this.data.recoveryPage(limit, offset, filters);
    if (
      limit !== this.limit ||
      offset !== this.offset ||
      filters.share !== this.share ||
      filters.folder !== this.folder ||
      filters.issue_type !== this.issueType ||
      filters.status !== this.status ||
      filters.kind !== this.kind
    )
      return;
    this.filters = page.filters ?? [];
    this.issueTypes = page.issue_types ?? [];
    if (offset > 0 && offset >= page.total) {
      this.offset = Math.max(0, Math.ceil(page.total / limit) - 1) * limit;
      void this.reload();
      return;
    }
    this.filtering = false;
    this.rows = page.rows;
    this.total = page.total;
    const selected = new Map(this.selected);
    for (const row of page.rows) {
      if (selected.has(key(row.target))) selected.set(key(row.target), row);
    }
    this.selected = selected;
  }

  private updateFilters(changes: Partial<RecoveryFilters>) {
    this.share = changes.share ?? this.share;
    this.folder = changes.folder ?? this.folder;
    this.issueType = changes.issue_type ?? this.issueType;
    this.status = changes.status ?? this.status;
    this.kind = changes.kind ?? this.kind;
    this.offset = 0;
    this.selected = new Map();
    this.result = undefined;
    this.rows = [];
    this.total = 0;
    this.filtering = true;
    void this.reload();
  }

  private filterControls() {
    if (!this.filtersOpen) return nothing;
    const folders = this.filters.find((filter) => filter.share_name === this.share)?.folders ?? [];
    const issueTypes = this.issueTypes
      .map((code) => (code === null ? 'none' : `code:${code}`))
      .sort((a, b) => issueTypeLabel(a).localeCompare(issueTypeLabel(b)));
    return html`<div id="recovery-filters" class="grid w-full grid-cols-1 items-end gap-3 rounded-box border border-base-300 bg-base-100 p-3 sm:grid-cols-2 lg:grid-cols-3">
      <label class="flex min-w-0 flex-col gap-1 text-sm">
        Share
        <select class="select select-sm w-full" aria-label="Filter by share" ?disabled=${!!this.busy}
          @change=${(event: Event) => this.updateFilters({ share: (event.target as HTMLSelectElement).value, folder: '' })}>
          <option value="" ?selected=${!this.share}>All shares</option>
          ${this.filters.map((filter) => html`<option value=${filter.share_name} ?selected=${filter.share_name === this.share}>${filter.share_name}</option>`)}
          ${this.share && !this.filters.some((filter) => filter.share_name === this.share) ? html`<option value=${this.share} selected>${this.share}</option>` : nothing}
        </select>
      </label>
      <label class="flex min-w-0 flex-col gap-1 text-sm">
        Subfolder
        <select class="select select-sm w-full" aria-label="Filter by subfolder" ?disabled=${!this.share || !!this.busy}
          @change=${(event: Event) => this.updateFilters({ folder: (event.target as HTMLSelectElement).value })}>
          <option value="" ?selected=${!this.folder}>All folders</option>
          ${folders.map((folder) => html`<option value=${folder} ?selected=${folder === this.folder}>${folder}</option>`)}
          ${this.folder && !folders.includes(this.folder) ? html`<option value=${this.folder} selected>${this.folder}</option>` : nothing}
        </select>
      </label>
      <label class="flex min-w-0 flex-col gap-1 text-sm">
        Issue type
        <select class="select select-sm w-full" aria-label="Filter by issue type" ?disabled=${!!this.busy}
          @change=${(event: Event) => this.updateFilters({ issue_type: (event.target as HTMLSelectElement).value })}>
          <option value="" ?selected=${!this.issueType}>All issue types</option>
          ${issueTypes.map((value) => html`<option value=${value} ?selected=${value === this.issueType}>${issueTypeLabel(value)}</option>`)}
          ${this.issueType && !issueTypes.includes(this.issueType) ? html`<option value=${this.issueType} selected>${issueTypeLabel(this.issueType)}</option>` : nothing}
        </select>
      </label>
      <label class="flex min-w-0 flex-col gap-1 text-sm">
        Recovery status
        <select class="select select-sm w-full" aria-label="Filter by recovery status" ?disabled=${!!this.busy}
          @change=${(event: Event) => this.updateFilters({ status: (event.target as HTMLSelectElement).value })}>
          <option value="" ?selected=${!this.status}>All statuses</option>
          ${Object.entries(LABELS).map(([value, label]) => html`<option value=${value} ?selected=${value === this.status}>${label}</option>`)}
        </select>
      </label>
      <label class="flex min-w-0 flex-col gap-1 text-sm">
        Item kind
        <select class="select select-sm w-full" aria-label="Filter by item kind" ?disabled=${!!this.busy}
          @change=${(event: Event) => this.updateFilters({ kind: (event.target as HTMLSelectElement).value })}>
          <option value="" ?selected=${!this.kind}>All item kinds</option>
          <option value="conflict" ?selected=${this.kind === 'conflict'}>Conflict</option>
          <option value="transfer" ?selected=${this.kind === 'transfer'}>File transfer</option>
          <option value="operation" ?selected=${this.kind === 'operation'}>Operation</option>
        </select>
      </label>
      <button type="button" class="btn btn-ghost btn-sm max-sm:min-h-11" ?disabled=${!!this.busy || !this.filterCount()}
        @click=${() => this.updateFilters({ share: '', folder: '', issue_type: '', status: '', kind: '' })}>Clear filters</button>
      <p class="col-span-full text-xs text-base-content/70">Folders include their subfolders. Changing filters clears the selection. Issues without a known location only appear without location filters.</p>
    </div>`;
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
        this.result = await admin.fixIssues(
          { targets: all ? [] : targets, all, resolution: null },
          all && this.filterCount() ? this.filterQuery() : undefined,
        );
      },
      'Fix requested. ISM will check eligible issues.',
    );
    if (ok) this.selected = new Map();
  }

  protected override render() {
    const canFixSelected = [...this.selected.values()].some((row) => row.can_fix);
    const visible = new Set(this.rows.map((row) => key(row.target)));
    const offPage = [...this.selected.values()].filter((row) => !visible.has(key(row.target)));
    return this.frame(
      () => html`
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div><h2 class="text-base font-semibold">Sync issues</h2>
          <p class="text-sm text-base-content/70">Issues are handled per file whenever it is safe to do so.</p></div>
        <div class="flex flex-wrap items-center gap-2">
          <button type="button" class="btn btn-sm max-sm:min-h-11" aria-expanded=${this.filtersOpen}
            aria-controls="recovery-filters" @click=${() => {
              this.filtersOpen = !this.filtersOpen;
            }}><ism-icon name="list-filter" size="sm"></ism-icon>
            Filters${this.filterCount() ? html`<span class="badge badge-primary badge-sm">${this.filterCount()}</span>` : nothing}
          </button>
          <span role="status" class="text-sm">${this.selected.size} selected</span>
          <button class="btn btn-primary btn-sm max-sm:min-h-11" ?disabled=${!!this.busy || this.filtering || !canFixSelected}
            @click=${() => this.fix()}>${this.spin('fix')}Fix selected</button>
          <button class="btn btn-sm max-sm:min-h-11" ?disabled=${!!this.busy || this.filtering || !this.total}
            @click=${() => this.fix(true)}>Fix all ${this.total} ${this.filterCount() ? 'matching ' : ''}issues</button>
        </div>
      </div>
      ${this.filterControls()}
      ${
        offPage.length
          ? html`<details>
          <summary class="cursor-pointer font-medium">Review ${offPage.length} selected issues from other pages</summary>
          <div class="mt-3 space-y-4" aria-label="Selected issues on other pages">
            ${offPage.map(
              (row) => html`<div class="min-w-0">
                <div class="flex flex-wrap items-center justify-between gap-2">
                  <h3 class="break-all font-medium">${row.title}</h3>
                  <button class="btn btn-ghost btn-sm max-sm:min-h-11" @click=${() => this.select(row, false)}>Remove selection</button>
                </div>
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
          ${this.result.items.filter((row) => row.state === 'not_retryable').length} can't be fixed automatically</span>
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
              <p class="mt-1 text-sm">${issueGuidance(row)}</p>
              ${row.local_instance ? html`<p class="text-sm text-base-content/70">${row.local_instance}${row.peer_instance ? html` / ${row.peer_instance}` : nothing}</p>` : nothing}
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
            <td class="max-sm:col-start-2 max-sm:px-2 max-sm:py-1">
              ${row.state === 'needs_decision' ? html`<a class="btn btn-sm max-sm:min-h-11" href=${linkTo('/conflicts')}>Go to Conflicts</a>` : nothing}
              ${
                row.can_fix
                  ? html`<button class="btn btn-primary btn-sm max-sm:min-h-11" ?disabled=${!!this.busy}
              @click=${() => this.fix(false, [row.target])}>Fix</button>`
                  : nothing
              }</td>
          </tr>`,
          )}
          ${this.rows.length ? nothing : html`<tr><td colspan="4" class="p-6 text-center">${this.filtering ? 'Loading issues…' : this.filterCount() ? 'No sync issues match these filters. Clear the filters to see all issues.' : 'No outstanding sync issues.'}</td></tr>`}
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
