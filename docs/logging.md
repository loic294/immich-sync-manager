# Logs and debugging

ISM writes timestamped runtime logs to the container's standard output. No extra volume or file
logging service is needed. The example Compose configuration keeps up to three 10 MB log files
per container, rotated by Docker. Logs survive a container restart, but not its replacement or
removal: export them before updating or recreating a container.

## Read or export runtime logs

From the repository root, using the Compose file from the installation guide:

```sh
# Recent activity, or follow new messages (Ctrl+C to stop following).
docker compose -f docker/compose.yml logs --timestamps --since=30m ism
docker compose -f docker/compose.yml logs --timestamps --follow --tail=200 ism

# Save a diagnostic copy before restarting/updating.
docker compose -f docker/compose.yml logs --no-color --timestamps --since=2h ism > ism-debug.log 2>&1
```

For a container created without Compose:

```sh
docker logs --timestamps --since=30m immich-sync-manager
```

On Synology, open **Container Manager -> Container -> immich-sync-manager -> Log**.
Docker Desktop also exposes the container's **Logs** tab.

The default `info` level includes startup/shutdown, maintenance queue pauses, watcher changes and
scan completions, plus warnings and failures from workers, operations and peer transfers. Messages
include operation, library or transfer IDs where available so they can be matched to the UI.
ANSI terminal color sequences are disabled so exported logs remain readable.

For more detail, set this in the service's Compose `environment` section and recreate it:

```yaml
ISM_LOG: info,ism_server=debug
```

Then reproduce the issue and export the logs. Reset it to `info` afterward. Prefer this targeted
filter over global `debug` or `trace`, which also enables third-party library diagnostics.

At `debug`, `collected imported upload asset activity` records the query's elapsed milliseconds,
exact matching item count, and returned row count (at most five). The count includes all published
uploads still awaiting an asset ID; the row limit applies only to examples. This is a fresh database
snapshot, not a cache. Its duration alone does not identify a SQLite lock holder.

Instrumented immediate SQLite transactions log `pool_wait_ms`, `begin_wait_ms`, `body_ms`, and
`commit_ms` separately, with a static `op` name and an `items` count. This covers peer event and
inventory receipts, peer activity and outgoing recovery snapshots, and scan-backlog writes.
Peer counts describe incoming page/snapshot rows, not rows actually written; scan collection
reports deleted rows. Successful transactions log at `debug`, or `warn` when combined pool/BEGIN
wait reaches one second or body plus commit reaches 250 milliseconds. Failed or abandoned writes
warn once with the last entered `stage` (`pool`, `begin`, `body`, or `commit`), including cancellation.
These diagnostics omit SQL, error text, identifiers, paths and payloads. They do not add database
writes or change transaction boundaries or durability. An unfinished body's timing does not
measure completion of SQLx's asynchronous rollback; BEGIN wait indicates contention, not the
identity of the writer holding the lock.

## Persistent history in the UI

Runtime logs complement, rather than replace, ISM's persistent records in the database on `/data`:

- **Audit:** administrative actions, settings changes, login failures and recovery actions.
- **Operations:** live background activity and sync waiting reasons, plus managed file-operation
  progress, outcomes, errors and recovery details. Background activity includes scans, hashing,
  peer event/inventory processing, transfers, maintenance, upload verification, and recovery.
  Bulk waits and failures are grouped per peer and share (or kind) with an item count, the most
  common reasons and a few example files where available; expand a row for those details and use
  **Sync** for the full per-file list.
- **Sync / Conflicts:** queued or failed transfers and conflict details.
- **Overview / Mappings:** maintenance blocks, watcher warnings and pending library scans.

These records survive container replacement as long as the `/data` volume is preserved. Keep
that volume backed up; do not delete it to clear a log or to recover a blocked operation.

Background runs retain their current phase, measured counters, resource, and timestamps.
Unknown totals show counts or a phase, not an invented percentage or ETA. Counters are scoped to
the displayed phase (for example bytes hashed versus files reconciled); progress writes and
notifications are coalesced to a one-second cadence. No progress update is not proof of a hang.
Top-level worker exits, the peer replication, activity-mirror and conflict-comparison workers,
and interrupted runs are recorded. Conflict comparisons are grouped by their queued, retrying,
blocked or observed-running state; a saved running claim alone does not prove a worker is alive.
Queued comparisons are reported as blocked if both comparison workers have been observed stopping.
Individual per-peer polling or activity-refresh exits may still only be logged and retried.
Queue-wide reasons are reported only when supported by group-wide evidence; file-specific
readiness and worker liveness can remain unknown. A timed-out mapped-root check is shown as
unavailable evidence, not proof that the root or transfers are blocked. Operations and structured
attention also include unresolved conflict decisions, server maintenance and compatibility gates,
unavailable mappings even without queued transfers, paused file detection, and failed library
synchronizations. Each points to the controls needed to resolve it; scheduled maintenance waits
continue automatically. A chosen conflict resolution is not reported as another pending decision.
Snapshot completeness reflects the available evidence: unchecked file dependencies, timed-out
folder checks, unreadable maintenance, and missing remote conditions keep the report partial.
Paired instances report receiver transfers and signed, share-authorized folder, maintenance,
conflict, and observed operation-admission conditions for queued transfers to that peer and share.
An active operation alone does not establish blocked transfers. Other remote operation and worker
conditions remain explicitly partial coverage, not a retryable status failure. Missing reports and
validation failures still warn; partial coverage cannot clear a retained condition or dismissal.
Reports omit paths, root/server names, raw
errors and operation IDs. Remote evidence is cached only in memory (at most 64 peers, with each
accepted report limited to 256 KiB and 500 conditions/shares), expires after 40 seconds measured
monotonically, and is refreshed periodically and on peer invalidations. Configuration or membership
changes invalidate cached authority. A server restart, expired report, unavailable peer or older
peer without this endpoint shows unavailable evidence, never a cleared blocker. Attention-only
notifications extend connection liveness without rewriting sync or recovery snapshots and refresh
independently of sync and recovery; newer hints and configuration changes
invalidate in-flight reports before they can become authoritative. A failed notification stream
also invalidates remote attention until it reconnects; unsupported streams use the legacy polling
path instead.
Per-code, owner and optional receiver-peer coverage identifies authoritative sources: a missing item from a
complete source can be cleared even if an unrelated source keeps the overall snapshot partial.
Missing or partial source coverage cannot establish that a prior condition has cleared.
Overlapping condition counts must not be added as distinct files. Library failure counts cover
retained synchronization requests, not files; a historical failure without retained work is not
marked as blocking. Refresh hints cover transfer-window openings and closings, override expiry,
retry eligibility and scheduled synchronization. Unavailable folders are rechecked without
predicting when they will become available.
Retained peer inbox and inventory errors describe the last recorded attempt; generic errors do not
by themselves establish that work is still blocked.

Activity starts, progress, and outcomes are recorded in memory immediately, then persisted by a
retrying background reporter. SQLite contention or recording failures do not delay the observed
work or replace its actual result. Unsaved outcomes stay in the live section until committed;
warnings distinguish in-memory evidence from durable history and report persistence failures.
If ISM stops before a start is saved, that run cannot be recovered. If only its start/progress was
saved, restart marks it interrupted, not successful, even if the work actually finished.
An unreadable saved activity is not overwritten or removed by recovery and is shown as an
unreadable record; it does not prevent ISM from starting or hide other activity. See the server
logs for details. Completed or resolved records remain subject to the normal history retention.

Recent completed background history is kept for seven days, capped at 1,000 runs; older resolved
issues share that bound. Active runs and unresolved failures are not evicted by retention.
Repeated failures update the scoped issue, and a successful subsequent run resolves it after
persistence. Outcomes are ordered by finish time, with recording revision breaking millisecond
ties, regardless of retry/write order. On restart,
unfinished runs are marked interrupted; actual queued work is recovered by the existing workers,
not by the reporting system. Managed file-operation history is unchanged.

The live activity section remains visible when managed-operation history is filtered or paginated.
It refreshes from live notifications and independently every ten seconds. A lost connection,
failed snapshot, or persistence error is shown explicitly, retaining the last received evidence.

## Sharing a report

Include the image's `sha-...` tag or digest, approximate incident time and time zone, the operation
or transfer ID, and the relevant log excerpt. Describe which of ISM, Open Photo Manager or Immich
local app initiated the change. See [recovery](recovery.md) before retrying a blocked operation.

Inspect exports before sharing: logs and audit records can contain file paths, instance names,
usernames and server addresses. ISM redacts URL credentials and password parameters in reported
errors, but that is not a guarantee that every upstream error or diagnostic is safe to publish.
Never attach `/data`, the encryption key, access tokens, passwords or database connection strings
to a public issue.
