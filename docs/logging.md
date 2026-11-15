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
- **Operations:** per-item progress, outcomes, errors and recovery details.
- **Sync / Conflicts:** queued or failed transfers and conflict details.
- **Overview / Mappings:** maintenance blocks, watcher warnings and pending library scans.

These records survive container replacement as long as the `/data` volume is preserved. Keep
that volume backed up; do not delete it to clear a log or to recover a blocked operation.

## Sharing a report

Include the image's `sha-...` tag or digest, approximate incident time and time zone, the operation
or transfer ID, and the relevant log excerpt. Describe which of ISM, Open Photo Manager or Immich
local app initiated the change. See [recovery](recovery.md) before retrying a blocked operation.

Inspect exports before sharing: logs and audit records can contain file paths, instance names,
usernames and server addresses. ISM redacts URL credentials and password parameters in reported
errors, but that is not a guarantee that every upstream error or diagnostic is safe to publish.
Never attach `/data`, the encryption key, access tokens, passwords or database connection strings
to a public issue.
