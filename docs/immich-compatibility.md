# Immich compatibility

ISM targets **Immich v3.4.1** only. On every check it reads the server version and the newest database
migration; a mismatch blocks writes for that server (reads and downloads keep working).

## What uses the API

Login and account discovery, libraries and their import paths, asset lookup, downloads of originals,
trash and restore, system config (file watching and periodic scans), job queue status and library
scans. Every request is sent as the account that owns the asset or library.

## What writes to the database directly

Immich has no API to change these for external-library assets, so `immich/pg.rs` does:

| Change | Statement | Guard |
| --- | --- | --- |
| Move/rename an asset | `asset.originalPath`, `originalFileName`, path checksum | Row lock, old path must match, no other asset may use the new path. |
| Move a sidecar | `asset_file.path` | Old path must match. |
| Transfer to another owner/library | `asset.ownerId`, `libraryId` | Row lock, old owner must match, and no owner-bound rows (albums, faces, …) may block it. |

Each write is one transaction and is the last step of an item, after the file moves, so a failure
before it is undone on disk and a committed write never needs undoing.

## The maintenance barrier

Before any change to a library: save the current watcher and scan settings, turn them off, wait for
the queues that read library files to drain, then admit work. After 15 minutes without new work:
scan the affected libraries, wait for the scan, restore only the settings ISM changed. If Immich does
not drain within `ISM_QUIESCE_TIMEOUT_SECS`, nothing is changed and the server is marked **blocked**.
