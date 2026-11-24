import { html, nothing } from 'lit';
import { admin } from '@/api/admin';
import { events } from '@/api/events';
import type { BackgroundActivity } from '@/api/generated/BackgroundActivity';
import type { BackgroundActivityPage } from '@/api/generated/BackgroundActivityPage';
import { IsmPage } from '@/base/ism-page';
import type { PageChange } from '@/components/ism-pagination';
import { formatBytes, formatDate, formatDuration, humanize } from '@/util/format';
import { tabKeydown } from '@/util/tabs';
import '@/components/ism-icon';
import '@/components/ism-pagination';

type Filter = 'attention' | 'running' | 'waiting' | 'all';
const RESTARTED =
  'ISM restarted before this run reported an outcome; queued work is recovered separately';
const FILTERS: readonly [Filter, string][] = [
  ['attention', 'Needs attention'],
  ['running', 'Running'],
  ['waiting', 'Waiting'],
  ['all', 'All'],
];
const PAGES: Record<string, string> = {
  '/sync': 'Open sync',
  '/conflicts': 'Open conflicts',
  '/servers': 'Open server maintenance',
  '/mappings': 'Open folder mappings',
  '/peers': 'Open peers and shares',
};
// Polling only backs up live events, so it can be slow; the snapshot is stale after a few misses.
const POLL_MS = 10_000;
const STALE_MS = 35_000;

function needsAttention(task: BackgroundActivity) {
  return task.status === 'failed' || task.status === 'blocked' || task.status === 'interrupted';
}

function activityTitle(task: BackgroundActivity) {
  if (task.task_key.startsWith('recovery:conflict:')) return 'Files with different versions';
  const titles: Record<string, string> = {
    'Peer file transfer queue': 'Files queued for sync',
    'Peer event application': 'Changes from another instance',
    'Peer inventory application': 'Checking files from another instance',
    'Outgoing peer synchronization': 'Files syncing to another instance',
    'Peer recovery refresh': 'Checking sync issues',
    'Recovery verification': 'Checking sync issues',
    'Recovery execution': 'Fixing a sync issue',
    'Conflict evidence preparation': 'Comparing file versions',
    'Scoped library synchronization': 'Updating the Immich library',
    'Library synchronization': 'Updating the Immich library',
    'Peer file transfer': 'File transfer',
    'Share inventory scan': 'Checking shared files',
    'Imported upload asset ID': 'Waiting for Immich to find uploaded files',
    'Peer synchronization': 'Syncing with another instance',
    'Pending peer inventory': 'Files waiting to sync',
    'Peer connection': 'Connection to another instance',
    'Peer inventory backfill': 'Checking files from another instance',
    'Peer transfer activity refresh': 'Checking sync progress',
    'Peer recovery request': 'Fixing issues on another instance',
    'Immich maintenance': 'Preparing Immich for changes',
    'Prepare Immich write barrier': 'Preparing Immich for changes',
    'Resume Immich library queue': 'Resuming Immich library updates',
    'Operation preview': 'Checking a file operation',
    'Operation admission': 'Preparing a file operation',
  };
  return titles[task.title] ?? task.title;
}

function explain(message: string): string {
  const prefix = 'Next files in the queue are waiting: ';
  if (message.startsWith(prefix)) return explain(message.slice(prefix.length));
  const checksum = 'Verifying checksum for ';
  if (message.startsWith(checksum))
    return `Checking the contents of ${message.slice(checksum.length)}`;
  const inventory = 'Inventory pagination will continue after ';
  if (message.startsWith(inventory))
    return `Continuing to check files after ${message.slice(inventory.length)}`;
  const slots =
    /^ISM records (\d+) running transfers \(limit (\d+)\); queue readiness is not fully known$/.exec(
      message,
    );
  if (slots)
    return `ISM shows ${slots[1]} of ${slots[2]} transfer slots in use. The status of queued files is not available yet.`;
  if (message.endsWith(' changed on the receiver'))
    return `${message.slice(0, -' changed on the receiver'.length).replaceAll('<item>', 'The file')} changed on the other instance.`;
  if (message.includes('No space left on device'))
    return 'Storage is full. Free space on the affected instance.';
  if (message.startsWith('peer returned unexpected Content-Range '))
    return 'The other instance sent an unexpected part of the file. Open Sync issues for details.';
  if (message.startsWith('peer returned ') && message.endsWith(' requested bytes'))
    return 'The other instance sent an incomplete file. Open Sync issues for details.';
  const messages: Record<string, string> = {
    'peer publication waits for a dependent inbox effect':
      'An earlier change to this file must finish first. Check Sync issues if it needs attention.',
    'peer footprint waits for unresolved managed operations':
      'An earlier change to this file must finish first. Check Sync issues if it needs attention.',
    'peer footprint waits for a failed companion transfer':
      "A file needed for this change couldn't sync. Open Sync issues for details.",
    'peer bookkeeping waits for active managed operations':
      'Waiting for another file change to finish. Check Operations if it stays here.',
    'Peer bookkeeping waits for active managed operations':
      'Waiting for another file operation to finish.',
    'peer effect waits for conflict resolution':
      'Files have different versions. Open Conflicts to compare them.',
    'Waiting for the original file before transferring its companion':
      'The original file must sync first. Check Sync issues if it cannot finish.',
    'Retry backoff after a failed transfer': 'The transfer failed. ISM will try again',
    'At least these files are in retry backoff': 'At least these files are waiting to retry.',
    'Waiting for allowed file-transfer hours': 'Waiting for the scheduled sync hours',
    'Queued; file-specific eligibility has not been checked':
      "ISM can't yet show which queued files are ready to sync.",
    'Mapped folder availability could not be checked; transfer readiness is unknown':
      "ISM couldn't check the mapped folder, so it can't confirm whether these files can sync.",
    'Recorded as running; worker liveness is not yet verified':
      "Saved as in progress, but ISM can't confirm whether the transfer is running.",
    'Waiting for file changes to settle; scan readiness has not been checked':
      "Waiting for file changes to finish. ISM can't yet confirm whether the scan can start.",
    'Share inventory rescan is queued; scan readiness has not been checked':
      'Waiting to check whether the shared files can be scanned.',
    'Received inventory awaits safe dependency verification':
      'Waiting to check the file list from the other instance.',
    'Received changes await dependency verification and application':
      'Waiting to check and apply changes from the other instance.',
    'Waiting for the related managed operation to finish':
      'Waiting for the related file operation to finish.',
    'Waiting for the related file content': 'Waiting for the related file.',
    'Waiting for a conflict comparison worker': 'Waiting to compare file versions.',
    'The last comparison attempt failed; it is queued for retry':
      'The last comparison failed. ISM will try again.',
    'Comparing versions for these conflicts': 'Comparing file versions.',
    'A comparison worker stopped; restart ISM when safe':
      'Comparisons stopped. Restart ISM when it is safe to do so.',
    'The comparison cannot continue automatically':
      "ISM can't compare these versions automatically. Open Conflicts for details.",
    'The saved comparison state or worker could not be verified':
      "ISM can't confirm whether these comparisons are running.",
    'Some conflict comparison work has no verifiable worker or saved state.':
      "ISM can't confirm the status of some file comparisons.",
    'Scoped synchronization failed; retained work waits for a new request':
      "Couldn't update the library. Check folder mappings for details.",
    [RESTARTED]: 'ISM restarted before this finished. Check current work for its latest status.',
    'Verifying evidence and applying the recovery plan': 'Checking and fixing a sync issue.',
    'Queued for recovery verification': 'ISM will check these sync issues.',
    'Waiting for dependent work or new evidence': 'Waiting for other work or updated information.',
    'Waiting for an administrator to choose a version':
      'Compare the versions in Conflicts, then choose which to keep. You can keep both.',
    'Recovery cannot continue automatically':
      "ISM couldn't fix this automatically. Open Sync issues for details.",
    'The receiving ISM reported a failure':
      "The other instance couldn't sync some files. Open Sync issues for details.",
    'Transfer needs recovery': 'The transfer failed. Open Sync issues for details.',
    'Published file is waiting for Immich import and asset lookup':
      'Waiting for Immich to finish importing the uploaded files.',
    'source version is no longer at the queued path; recovery will reconcile its current location':
      'The original file is no longer in the expected location. ISM will check where it went.',
    'filesystem changes may have completed, but peer bookkeeping failed; reconcile replica state and events before resolving recovery':
      'The files may have changed, but ISM could not finish recording the change. Open Sync issues for details.',
    'receiver disk full': 'The receiving instance has run out of storage. Free space there.',
    'downloaded data does not match its checksum':
      'The downloaded file did not match the original. Open Sync issues for details.',
    'checksum mismatch':
      "The file's contents did not match what was expected. Check the file before trying again.",
    'peer file changed during the transfer':
      'The file changed on the other instance during download.',
    'the file has an unresolved conflict':
      'This file has different versions. Compare them in Conflicts.',
    'peer source tree changed; a new choice is required':
      'The folder changed on the other instance. Compare the versions in Conflicts, then choose again.',
    'matching peer file checksums have different sizes':
      'The files appear to match, but their sizes differ. Open Sync issues for details.',
    'peer returned an invalid Content-Range':
      'The other instance sent an unexpected part of the file. Open Sync issues for details.',
    'peer sent more data than the requested range':
      'The other instance sent an unexpected part of the file. Open Sync issues for details.',
    'share location changed while downloading':
      "The shared folder's location changed during download. Check the share's settings.",
    'root location changed while downloading':
      'The destination folder changed during download. Check the folder mapping.',
    'Waiting for the replication-operation lock': 'Waiting for another file change to finish.',
    'Waiting for the replication-operation lock before applying inventory':
      'Waiting to check files from the other instance.',
    'Waiting for the replication-operation lock before applying events':
      'Waiting to apply changes from the other instance.',
    'Checking active operations and uncertain paths': 'Checking which files need to wait.',
    'Enumerating share files': 'Finding files in the shared folder.',
    'Hashing and reconciling settled files': 'Checking files that have finished changing.',
    'Reconciling removed files and publishing inventory': 'Checking for removed files.',
    'Fetching inventory pages and saving durable receipts':
      'Getting the list of files from the other instance.',
    'Applying inventory entries': 'Checking files from the other instance.',
    'Fetching peer events and saving durable receipts': 'Getting changes from the other instance.',
    'Verifying and applying received events': 'Checking and applying received changes.',
    'Connecting to peer and verifying its identity': 'Connecting to the other instance.',
    'Pulling and applying peer events': 'Getting and applying changes from the other instance.',
    'Fetching and applying share inventory backfill': 'Checking files from the other instance.',
    'Fetching current recovery evidence from peer': 'Checking sync issues on the other instance.',
    'Fetching receiver-owned transfer progress': 'Getting sync progress from the other instance.',
    'Forwarding recovery decisions to peer': 'Sending the sync decision to the other instance.',
    'Waiting for safe decision verification': 'Waiting to check a sync decision.',
    'Verifying peer source evidence': 'Checking the file on the other instance.',
    'Verifying local file and transfer prerequisites':
      'Checking whether this file is ready to sync.',
    'Verifying downloaded checksum': 'Checking the downloaded file.',
    'Verifying destination and submitting safe publication': 'Checking where to save the file.',
    'Attempt deferred; file remains queued for its prerequisites':
      'This file is waiting for another change to finish.',
    'Attempt ended; transfer is no longer applicable': 'This transfer is no longer needed.',
    'Waiting for maintenance admission': 'Waiting for Immich to be ready for changes.',
    'Preparing scoped synchronization': 'Preparing to update changed files in Immich.',
    'Synchronizing files through the Immich API': 'Updating changed files in Immich.',
    'Dispatching scoped folder scans to Immich': 'Asking Immich to check changed folders.',
    'Waiting for Immich to finish the library queue and confirm it idle':
      'Waiting for Immich to finish updating the library.',
    'Synchronizing scoped paths or waiting for the Immich library queue':
      'Updating changed files in Immich.',
    'Waiting for the Immich maintenance barrier':
      'Waiting for Immich to finish another file change.',
    'Immich library queue is paused by a write barrier; waiting for release':
      'Waiting for Immich to finish another file change.',
    'Waiting for Immich library queue idle confirmation':
      'Waiting for Immich to finish updating the library.',
    'Synchronizing files and folders through the Immich API':
      'Updating changed files and folders in Immich.',
    'Checking Immich version, database schema and API capabilities':
      'Checking the Immich connection and compatibility.',
    'Fetching libraries and reconciling exclusion patterns':
      'Updating library settings from Immich.',
    'Removing staging for a published or abandoned upload': 'Cleaning up temporary upload files.',
    'Checking expired upload and clearing abandoned staging': 'Cleaning up an expired upload.',
    'Checking the original photo for this sidecar':
      'Checking the original file for a related file.',
    'Waiting for file changes to settle (debounce)': 'Waiting for file changes to finish.',
    'Waiting for the library scheduler': 'Waiting to update Immich.',
    'Verifying settings and pausing the Immich library queue':
      'Checking Immich settings and pausing library updates.',
    'Confirming the paused Immich library queue has no active jobs':
      'Waiting for Immich to finish its current library work.',
    'Resuming the library queue after managed operations': 'Resuming Immich library updates.',
    'The receiving ISM is waiting for safe publication': 'The other instance is saving the files.',
    'Queued on the receiving ISM': 'Waiting on the other instance.',
    'The receiving ISM is downloading': 'The other instance is downloading the files.',
    'The receiving ISM reported a failed operation':
      'A file operation failed on the other instance. Open Sync issues for details.',
    'Receiver status is not current': "The other instance's latest status is unavailable.",
    'Downloading, verifying, or preparing safe publication': 'Downloading and checking the file.',
    'Waiting for the upload lock': 'Waiting for another upload step to finish.',
    'Waiting to submit safe upload publication': 'Waiting to save the uploaded files.',
    'Planning and verifying requested file actions': 'Checking the requested file changes.',
    'Waiting for the operation submission lock': 'Waiting to start the file change.',
    'Validating and admitting the managed operation': 'Checking and starting the file change.',
  };
  return (messages[message] ?? message).replaceAll('<item>', 'the file');
}

function matches(filter: Filter, task: BackgroundActivity) {
  if (filter === 'all') return true;
  if (filter === 'attention') return needsAttention(task);
  return task.status === filter;
}

function ago(value: string, now: number) {
  const seconds = (now - Date.parse(value)) / 1000;
  if (Number.isNaN(seconds)) return '—';
  return seconds < 5 ? 'just now' : `${formatDuration(seconds)} ago`;
}

export class IsmBackgroundActivity extends IsmPage {
  static override properties = {
    ...IsmPage.properties,
    snapshot: { state: true },
    filter: { state: true },
    limit: { state: true },
    offset: { state: true },
  };
  declare snapshot: BackgroundActivityPage | undefined;
  declare filter: Filter | undefined;
  declare limit: number;
  declare offset: number;
  protected override reloadOn = [
    'activity_changed',
    'operation_updated',
    'operation_committed',
    'maintenance_changed',
    'sync_changed',
    'watch_changed',
    'library_scanned',
    'settings_changed',
    'conflicts_changed',
  ] as const;
  protected override liveReloadInterval = 2000;
  private timer: number | undefined;
  private receivedAt = 0;

  constructor() {
    super();
    this.filter = undefined;
    this.limit = 25;
    this.offset = 0;
  }

  override connectedCallback() {
    super.connectedCallback();
    this.timer = window.setInterval(() => {
      this.requestUpdate();
      void this.reload();
    }, POLL_MS);
  }

  override disconnectedCallback() {
    super.disconnectedCallback();
    window.clearInterval(this.timer);
  }

  protected override async load() {
    const { limit, offset } = this;
    const snapshot = await admin.backgroundActivity(limit, offset);
    if (limit !== this.limit || offset !== this.offset) return;
    this.snapshot = snapshot;
    this.receivedAt = Date.now();
    if (offset > 0 && offset >= snapshot.total) {
      this.offset = Math.max(0, Math.ceil(snapshot.total / limit) - 1) * limit;
      void this.reload();
    }
  }

  /** The chosen tab, else the most urgent one that has something in it. */
  private activeFilter(current: BackgroundActivity[]): Filter {
    if (this.filter) return this.filter;
    return (
      FILTERS.map(([f]) => f).find((f) => f !== 'all' && current.some((t) => matches(f, t))) ??
      'all'
    );
  }

  private measure(task: BackgroundActivity) {
    const bytes = task.unit.startsWith('bytes');
    const fmt = (n: number) => (bytes ? formatBytes(n) : n.toLocaleString());
    const units: Record<string, string> = {
      hashed: 'checked',
      transferred: 'downloaded',
      'paths dispatched': 'items sent to Immich',
      'events applied': 'changes applied',
      'events checked': 'changes checked',
      'inventory entries checked': 'files checked',
      'actions planned': 'actions checked',
    };
    const rawUnit = bytes ? task.unit.replace('bytes', '').trim() : task.unit;
    const unit = units[rawUnit] ?? rawUnit;
    const measured = task.total !== null && task.total > 0;
    return { bytes, fmt, unit, measured };
  }

  private icon(task: BackgroundActivity) {
    if (task.status === 'running')
      return html`<span class="loading loading-spinner loading-sm text-info" aria-hidden="true"></span>`;
    if (needsAttention(task))
      return html`<ism-icon name="circle-alert" class="text-error"></ism-icon>`;
    if (task.status === 'completed')
      return html`<ism-icon name="circle-check" class="text-success"></ism-icon>`;
    return html`<ism-icon name="clock" class="text-base-content/60"></ism-icon>`;
  }

  /** Right-hand summary: how much work, and how far along, in one glance. */
  private aside(task: BackgroundActivity, now: number) {
    const { bytes, fmt, unit, measured } = this.measure(task);
    const items =
      task.count > 1
        ? `${task.count.toLocaleString()} ${bytes ? 'files' : task.unit === 'conflicts' ? 'conflicts' : 'items'}`
        : '';
    let amount = '';
    if (task.status === 'running' && measured)
      amount = `${fmt(task.done)} / ${fmt(task.total ?? 0)} ${unit}`;
    else if (bytes && measured) amount = fmt(task.total ?? 0);
    else if (task.done > 0) amount = `${fmt(task.done)} ${unit}`;
    const pct = measured ? Math.min(100, Math.round((task.done / (task.total ?? 1)) * 100)) : 0;
    return html`<div class="flex min-w-0 flex-col items-end gap-1 text-right text-sm tabular-nums">
      ${items ? html`<span class="font-medium">${items}</span>` : nothing}
      ${amount ? html`<span class="text-base-content/70">${amount}</span>` : nothing}
      ${task.status === 'running' && measured ? html`<progress class="progress progress-info h-1.5 w-28" value=${pct} max="100" aria-label=${`${activityTitle(task)}: ${pct}%`}></progress>` : nothing}
      <span class="text-xs text-base-content/60">${task.finished_at ? `Finished ${ago(task.finished_at, now)}` : `Updated ${ago(task.updated_at, now)}`}</span>
    </div>`;
  }

  private details(task: BackgroundActivity, now: number) {
    const { measured, fmt, unit } = this.measure(task);
    const stalled = task.status === 'running' && now - Date.parse(task.updated_at) > 60_000;
    const finished = task.finished_at === null ? now : Date.parse(task.finished_at);
    const elapsed = (finished - Date.parse(task.started_at)) / 1000;
    const reasonTotal = task.reasons.reduce((n, r) => n + r.count, 0);
    const conflictIssue =
      task.task_key.startsWith('recovery:conflict:') ||
      task.phase === 'peer effect waits for conflict resolution';
    const syncIssue = task.page === '/sync' && needsAttention(task);
    const href = conflictIssue ? '/conflicts' : syncIssue ? '/sync?view=attention' : task.page;
    const label = conflictIssue
      ? 'Open conflicts'
      : syncIssue
        ? 'Review sync issues'
        : task.page && PAGES[task.page];
    return html`<div class="grid gap-4 px-3 pb-4 pl-10 text-sm md:grid-cols-[minmax(0,3fr)_minmax(0,2fr)]">
      <div class="min-w-0 space-y-3">
        ${stalled ? html`<p class="text-warning">No progress reported for ${formatDuration((now - Date.parse(task.updated_at)) / 1000)}.</p>` : nothing}
        ${task.next_at ? html`<p>Scheduled for: <span class="font-medium">${formatDate(task.next_at)}</span></p>` : nothing}
        ${task.error ? html`<p class="break-words text-error">${explain(task.error)}</p>` : nothing}
        ${
          task.reasons.length > 0
            ? html`<div>
              <h5 class="mb-1 font-medium">${task.count > reasonTotal ? `Reasons (${reasonTotal.toLocaleString()} of ${task.count.toLocaleString()})` : 'Reasons'}</h5>
              <ul class="space-y-1">${task.reasons.map(
                (r) => html`<li class="flex items-baseline justify-between gap-3">
                  <span class="min-w-0 break-words">${explain(r.reason)}</span>
                  <span class="shrink-0 tabular-nums text-base-content/70">${r.count.toLocaleString()}</span>
                </li>`,
              )}</ul>
            </div>`
            : nothing
        }
        ${
          task.examples.length > 0
            ? html`<div>
              <h5 class="mb-1 font-medium">${task.count > task.examples.length ? `Examples (${task.examples.length} of ${task.count.toLocaleString()})` : 'Items'}</h5>
              <ul class="space-y-1">${task.examples.map(
                (e) => html`<li class="min-w-0">
                  <span class="break-all font-mono text-xs">${e.resource}</span>
                  ${e.detail ? html`<span class="block break-words text-xs text-base-content/70">${explain(e.detail)}</span>` : nothing}
                </li>`,
              )}</ul>
            </div>`
            : nothing
        }
      </div>
      <dl class="grid grid-cols-[auto_minmax(0,1fr)] content-start gap-x-3 gap-y-1 text-base-content/80">
        <dt class="text-base-content/60">Status</dt><dd>${humanize(task.status)}</dd>
        ${measured ? html`<dt class="text-base-content/60">Progress</dt><dd class="tabular-nums">${fmt(task.done)} / ${fmt(task.total ?? 0)} ${unit}</dd>` : nothing}
        <dt class="text-base-content/60">${task.id.startsWith('state:') ? 'Oldest item' : 'Started'}</dt><dd>${formatDate(task.started_at)}</dd>
        ${Number.isFinite(elapsed) && !task.id.startsWith('state:') ? html`<dt class="text-base-content/60">${task.finished_at ? 'Duration' : 'Elapsed'}</dt><dd>${formatDuration(elapsed)}</dd>` : nothing}
        <dt class="text-base-content/60">Last update</dt><dd>${formatDate(task.updated_at)}</dd>
        <dd class="col-span-2 mt-2 flex flex-wrap gap-2">
          ${task.operation_id ? html`<button type="button" class="btn btn-sm" @click=${() => this.dispatchEvent(new CustomEvent('operation-open', { detail: task.operation_id, bubbles: true, composed: true }))}>Open related operation</button>` : nothing}
          ${href && label ? html`<a class="btn btn-sm" href=${href}>${label}</a>` : nothing}
        </dd>
      </dl>
    </div>`;
  }

  private rows(rows: BackgroundActivity[], now: number) {
    return html`<ul class="divide-y divide-base-300">
      ${rows.map(
        (task) => html`<li>
          <details class="group">
            <summary class="grid cursor-pointer list-none grid-cols-[1.25rem_minmax(0,1fr)_auto] items-center gap-x-3 rounded-field px-3 py-3 hover:bg-base-200 focus-visible:outline-2 focus-visible:outline-primary [&::-webkit-details-marker]:hidden">
              <span class="flex justify-center">${this.icon(task)}</span>
              <span class="min-w-0">
                <span class="flex min-w-0 items-baseline gap-2">
                  <span class="min-w-0 font-medium">${activityTitle(task)}</span>
                  ${task.task_key.startsWith('recovery:conflict:') && task.resource === 'conflict' ? nothing : html`<span class="truncate text-sm text-base-content/70" title=${task.resource}>${task.resource}</span>`}
                </span>
                <span class="block text-sm ${needsAttention(task) ? 'text-error' : 'text-base-content/80'}" title=${explain(task.phase)}>${explain(task.phase)}</span>
              </span>
              ${this.aside(task, now)}
            </summary>
            ${this.details(task, now)}
          </details>
        </li>`,
      )}
    </ul>`;
  }

  private skeleton() {
    return html`<div class="space-y-3 py-3" aria-label="Loading background activity" role="status">
      ${[0, 1, 2].map(() => html`<div class="flex items-center gap-3 px-3"><div class="skeleton size-5 rounded-full"></div><div class="flex-1 space-y-2"><div class="skeleton h-4 w-1/3"></div><div class="skeleton h-3 w-2/3"></div></div><div class="skeleton h-4 w-20"></div></div>`)}
    </div>`;
  }

  private freshness(now: number) {
    const snapshot = this.snapshot;
    const stale = this.receivedAt > 0 && now - this.receivedAt > STALE_MS;
    if (this.loadError)
      return html`<p role="alert" class="flex flex-wrap items-center gap-2 text-sm text-error">
        <ism-icon name="triangle-alert"></ism-icon>
        Couldn't refresh: ${this.loadError}. ${snapshot ? `Showing what was known ${ago(snapshot.observed_at, now)}.` : 'Current activity is unknown.'}
      </p>`;
    if (!snapshot) return nothing;
    const live = events.connection.value === 'open' && !stale;
    return html`<p role="status" class="flex items-center gap-2 text-sm ${live ? 'text-base-content/60' : 'text-warning'}">
      <span class="status ${live ? 'status-success' : 'status-warning'}" aria-hidden="true"></span>
      ${live ? 'Live' : stale ? 'Not refreshed recently' : 'Live updates disconnected; checking every 10 sec'} · updated ${ago(snapshot.observed_at, now)}
    </p>`;
  }

  protected override render() {
    const now = Date.now();
    const current = this.snapshot?.current ?? [];
    const filter = this.activeFilter(current);
    const order = (t: BackgroundActivity) =>
      needsAttention(t) ? 0 : t.status === 'running' ? 1 : 2;
    const visible = current
      .filter((t) => matches(filter, t))
      .sort(
        (a, b) =>
          order(a) - order(b) || b.count - a.count || a.started_at.localeCompare(b.started_at),
      );
    const counts = Object.fromEntries(
      FILTERS.map(([f]) => [f, current.filter((t) => matches(f, t)).length]),
    ) as Record<Filter, number>;
    return html`<section class="min-w-0 rounded-box border border-base-300 bg-base-100" aria-labelledby="background-activity-title">
      <header class="flex flex-wrap items-start justify-between gap-3 px-4 pt-4">
        <div class="min-w-0">
          <h2 id="background-activity-title" class="text-lg font-semibold">Background activity</h2>
          <div class="mt-1">${this.freshness(now)}</div>
        </div>
        <button type="button" class="btn btn-ghost btn-sm" aria-label="Refresh activity" @click=${() => this.reload()}>
          <ism-icon name="refresh-cw"></ism-icon> Refresh
        </button>
      </header>
      ${this.snapshot?.warnings.map((w) => html`<p role="alert" class="mx-4 mt-3 text-sm text-warning">${explain(w)}</p>`)}
      <div role="tablist" aria-label="Background activity status" class="tabs tabs-border mt-2 flex-nowrap overflow-x-auto px-2" @keydown=${tabKeydown}>
        ${FILTERS.map(
          ([
            value,
            label,
          ]) => html`<button type="button" role="tab" id=${`activity-tab-${value}`} aria-controls="activity-panel"
            aria-selected=${filter === value} tabindex=${filter === value ? 0 : -1}
            class=${filter === value ? 'tab tab-active gap-2 whitespace-nowrap' : 'tab gap-2 whitespace-nowrap'}
            @click=${() => {
              this.filter = value;
            }}>
            ${label}
            <span class=${value === 'attention' && counts.attention > 0 ? 'badge badge-error badge-sm' : 'badge badge-ghost badge-sm'}>${counts[value]}</span>
          </button>`,
        )}
      </div>
      <div id="activity-panel" role="tabpanel" aria-labelledby=${`activity-tab-${filter}`} class="px-1">
        ${
          this.loading && !this.snapshot
            ? this.skeleton()
            : visible.length > 0
              ? this.rows(visible, now)
              : html`<p class="px-3 py-6 text-sm text-base-content/70">${
                  this.snapshot
                    ? filter === 'attention'
                      ? 'Nothing needs attention.'
                      : filter === 'running'
                        ? 'Nothing is running right now.'
                        : filter === 'waiting'
                          ? 'Nothing is waiting.'
                          : 'ISM is idle: no background work or sync blockers.'
                    : 'Current activity is unknown.'
                }</p>`
        }
      </div>
      <details class="border-t border-base-300">
        <summary class="cursor-pointer px-4 py-3 text-sm font-medium">Recent runs${this.snapshot ? html` <span class="text-base-content/60">(${this.snapshot.total.toLocaleString()})</span>` : nothing}</summary>
        <p class="px-4 text-xs text-base-content/60">Kept for seven days, up to 1,000 runs.</p>
        <div class="px-1">${this.rows(this.snapshot?.history ?? [], now)}</div>
        ${this.snapshot && this.snapshot.history.length === 0 ? html`<p class="px-4 py-3 text-sm text-base-content/70">No recent runs.</p>` : nothing}
        <div class="px-4 pb-3">
          <ism-pagination .total=${this.snapshot?.total ?? 0} .limit=${this.limit} .offset=${this.offset}
            @page-change=${(event: CustomEvent<PageChange>) => {
              this.limit = event.detail.limit;
              this.offset = event.detail.offset;
              void this.reload();
            }}></ism-pagination>
        </div>
      </details>
    </section>`;
  }
}
customElements.define('ism-background-activity', IsmBackgroundActivity);
declare global {
  interface HTMLElementTagNameMap {
    'ism-background-activity': IsmBackgroundActivity;
  }
}
