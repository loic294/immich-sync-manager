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

## Persistent history in the UI

Runtime logs complement, rather than replace, ISM's persistent records in the database on `/data`:

- **Audit:** administrative actions, settings changes, login failures and recovery actions.
- **Operations:** live background activity and sync waiting reasons, plus managed file-operation
  progress, outcomes, errors and recovery details. Background activity includes scans, hashing,
  peer event/inventory processing, transfers, maintenance, upload verification, and recovery.
  Bulk waits and failures are grouped per peer and share (or kind) with an item count, the most
  common reasons and a few example files; expand a row for those details and use **Sync** for
  the full per-file list.
- **Sync / Conflicts:** queued or failed transfers and conflict details.
- **Overview / Mappings:** maintenance blocks, watcher warnings and pending library scans.

These records survive container replacement as long as the `/data` volume is preserved. Keep
that volume backed up; do not delete it to clear a log or to recover a blocked operation.

Background runs retain their current phase, measured counters, resource, and timestamps.
Unknown totals show counts or a phase, not an invented percentage or ETA. Counters are scoped to
the displayed phase (for example bytes hashed versus files reconciled); progress writes and
notifications are coalesced to a one-second cadence. No progress update is not proof of a hang.
Unexpected worker exits and interrupted runs remain visible rather than appearing healthy.

Activity starts, progress, and outcomes are recorded in memory immediately, then persisted by a
retrying background reporter. SQLite contention or recording failures do not delay the observed
work or replace its actual result. Unsaved outcomes stay in the live section until committed;
warnings distinguish in-memory evidence from durable history and report persistence failures.
If ISM stops before a start is saved, that run cannot be recovered. If only its start/progress was
saved, restart marks it interrupted, not successful, even if the work actually finished.

Recent completed background history is kept for seven days, capped at 1,000 runs; older resolved
issues share that bound. Active runs and unresolved failures are not evicted by retention.
Repeated failures update the scoped issue, and a successful subsequent run resolves it after
persistence. Outcomes are ordered by finish time, with recording revision breaking millisecond
ties, regardless of retry/write order. On restart,
unfinished runs are marked interrupted; actual queued work is recovered by the existing workers,
not by the reporting system. Managed file-operation history is unchanged.

The live activity section remains visible when managed-operation history is filtered or paginated.
It refreshes from live notifications and independently every five seconds. A lost connection,
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
