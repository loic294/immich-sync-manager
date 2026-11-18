# Immich compatibility

ISM targets **Immich v3.4.1** only. On every check it reads the server version and the newest database
migration. External-library moves use a per-server `external_library_move_mode`, persisted by ISM and
available under **Servers → Edit**:

| Mode | Behavior |
| --- | --- |
| `api_only` | Prefer the external-library move API and fail on unsupported routes. Database fallback is never attempted. |
| `api_with_db_fallback` | Default for existing and new servers. Prefer the API; only allow compatibility DB writes for route-unavailable `404` (`Cannot POST`), `405`, or `501`. Known-library `404`, `400`, `409`, and all other errors never fall back. |
| `db_only` | Skip the move API and use guarded database writes; requires stored database access and **direct writes** enabled. |

Every managed write runs under the barrier below. The server check also probes the mutation-free
external-library route and exposes `external_library_api_capable` (`true`, `false`, or unverified) to
administrative and client-discovery consumers. The custom Immich v3.4.1 fork exposes this API; stock
v3.4.1 remains supported through `api_with_db_fallback` or `db_only`.

Managed upload publication synchronizes only the `Original` file with `POST /libraries/:id/files/add`
after the filesystem move. Sidecars and companions remain grouped files but are never sent to this
endpoint. Managed trash synchronizes each exact original asset path with
`POST /libraries/:id/files/remove` after the original leaves the library. `db_only` skips both calls.
With `api_with_db_fallback`, route-unavailable errors schedule a debounced targeted parent-folder scan only for add
operations; `remove` is already represented by the existing Immich trash API. A `400`, `409`, known
library `404`, or any other error fails the operation and is never hidden by fallback.
Folder fallbacks require the fork's external-library capability; an unverified server is probed
before dispatch. Unsupported stock servers report an error rather than risking an unscoped scan.
The fork's folder scan discovers files, not existing-asset metadata changes or removals. See
[file watching](watching.md) for API-first outside-change handling and fallback limitations.

## What uses the API

Login and account discovery, libraries and their import paths, asset lookup, downloads of originals,
trash and restore, system config (file watching and periodic scans), job queue status and library
scans. Every request is sent as the account that owns the asset or library.

## What writes to the database directly

The API-capable fork handles the asset path and owner/library change. ISM still uses guarded
`immich/pg.rs` writes only for sidecar rows, for `db_only`, or for the explicit compatibility fallback:

| Change | Statement | Guard |
| --- | --- | --- |
| Move/rename an asset | `asset.originalPath`, `originalFileName`, path checksum | Row lock, old path must match, no other asset may use the new path. |
| Move a sidecar | `asset_file.path` | Old path must match. |
| Transfer to another owner/library | `asset.ownerId`, `libraryId` | Row lock, old owner must match, and no owner-bound rows (albums, faces, …) may block it. |

Each statement runs inside the operation's write barrier and a transaction; a row that no longer
matches the plan rolls back and sends the operation to recovery. Ownership transfers lock the asset
row, require the planned owner and an untrashed asset, and re-check owner-bound rows inside the
transaction (`owner_transfer_blocked` otherwise).

## Immich settings ISM owns

Immich v3.4.1 cannot prove that its file watcher has stopped: the [configuration service](https://github.com/immich-app/immich/blob/v3.4.1/server/src/services/system-config.service.ts)
emits a local update, which the [notification service](https://github.com/immich-app/immich/blob/v3.4.1/server/src/services/notification.service.ts)
forwards to other workers without awaiting an acknowledgement. ISM therefore never toggles the
watcher around changes. Instead, when a server or account is enrolled, ISM turns **off**
`library.watch.enabled` (file watching) and `library.scan.enabled` (scheduled library scans), and
they stay off. ISM detects file changes itself and uses file APIs with scoped scan fallbacks (see
[file watching](watching.md)).

ISM re-reads these settings before every batch of changes and periodically. If either was turned
back on, the server shows a **settings drift** alert and refuses writes. ISM does not silently turn
them off again: the admin clicks **Turn them off again** (Servers or Overview page), which re-applies
the settings and clears the block. Servers enrolled with an older ISM version may still have them on
and show this alert after upgrading.

## The write barrier

Before each managed operation, ISM:

1. reads the system config and refuses (blocking the server) if watching or scheduled scans are on;
2. pauses Immich's `library` queue through the jobs API and waits until the queue reports itself
   paused with **no active job** on three consecutive reads. Jobs queued meanwhile (for example a
   scan an admin starts from Immich's UI) show as *waiting* and are held by the paused queue until
   it resumes; they do not delay the operation;
3. admits the operation. Overlapping operations share one pause with a persisted reference count;
4. resumes the queue when the last operation finishes, after which due library scans may start.

If the queue cannot be paused, confirmed idle or resumed, the server is blocked and the Servers page
shows how to recover. After a restart, a queue ISM had paused is resumed when no operation is
running; interrupted operations still require recovery.

Only the `library` queue imports files or marks them offline, so it is the only queue paused.
Sidecar and metadata-extraction jobs may keep running: they read assets by their database path, and
a managed move changes that path in a checked transaction (row lock, old path must match). A job
racing a move fails to read the old file rather than changing the library; a row that no longer
matches the plan sends the operation to recovery. These jobs are mostly started by scans and uploads,
which wait for the barrier anyway.

Verified against stock v3.4.1 in a disposable test: a paused `library` queue holds newly queued jobs
until resume, a managed move through `pg.rs` keeps the asset ID, album membership and favorite, and
library scans ignore assets in Immich's trash. The end-to-end proof (`docker/immich-proof/`, results in
`docker/immich-proof/RESULTS.md`) also checks that the sidecar path, online state and restore with the
same ID survive, and that nothing is auto-imported while watching and scheduled scans are off. ISM
therefore admits Immich-linked writes; if the barrier cannot be taken, the operation fails closed with
`maintenance_blocked`.

## Limited disposable-database check

The ignored `disposable_immich_path_update_is_guarded` test requires an isolated v3.4.1 database
named `ismtest`, with a synthetic asset imported at `/tmp/ism-proof-photos/a.png` and no asset at
`/tmp/ism-proof-photos/b.png`. Set `ISM_DISPOSABLE_IMMICH_DATABASE_URL` to that database and run
`cargo test -p ism-server disposable_immich_path_update_is_guarded -- --ignored`. The test updates
the asset within a transaction and rolls it back. It checks a path update and its stale-path guard;
it does not test file changes, watcher quiescence, or ownership transfers.

The ignored `disposable_trash_identity_checks_unique_path` test uses a temporary table in a
disposable PostgreSQL database named `ismtest` to check the identity query, including missing,
changed and duplicate paths. It does not establish a maintenance barrier.
