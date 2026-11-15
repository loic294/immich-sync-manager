# Peer sync

Several instances, each next to its own Immich, can keep the same folders in sync. Use a private
network between them (for example Netbird) and point each peer at the other's private address.

Every change a peer applies goes through the same managed operations as local requests. On a
root inside an Immich library they run under the write barrier and keep asset IDs, albums and
favorites (see [immich-compatibility.md](immich-compatibility.md)). Replaced or deleted files go to
the ISM trash, never straight to deletion.

## Pairing

1. On instance A: **Peers → Create invite**. The invite is valid for 24 hours and works once.
2. On instance B: **Peers → Accept invite**, paste it, and give A's URL (e.g. `http://192.0.2.10:8080`)
   and B's own callback URL as seen from A.
3. Both sides now share a secret; every peer request is signed and must be within 5 minutes of the
   other clock, so keep NTP on.

## Shares

A share is a folder replicated under the same name on each instance: pick the local root and
subfolder, then the peers. Create the share with the same name on the other instance, mapped to its
own folder. Folders may sit at different paths on each machine.

### Backfill when a share is added or re-enabled

The event cursor is per peer, so events from before a share existed or while it was disabled are
not replayed. Instead, creating an enabled share, enabling it, or adding peers to it records a
pending backfill for each of those peers. It also announces a `share_enabled` event, so the other
side backfills from this instance too.

A backfill pages through the peer's inventory of the share
(`GET /api/peer/v1/inventory?share=…&after=…`, 500 entries per page). It handles each entry as if
the peer had just announced it:
- newer files are queued for transfer;
- files the peer deleted since both sides last agreed go to the ISM trash;
- files changed on both sides become conflicts.

Paths with an open conflict and hidden paths are skipped. The backfill position is saved with
each page, so an interrupted backfill resumes where it stopped. A peer's events and backfill wait
while one of its changes is still being applied.

### What is synchronized

- **Management events:** moves, renames, folder creation, folder moves, file trash and folder trash
  done through ISM become peer events. A peer applies each one as a managed operation, so on an
  Immich-linked root the asset keeps its ID and only its path changes. An event is acknowledged
  only after the operation and its bookkeeping succeed. A failed operation is retried after one
  minute; a cancelled operation stays stopped until an administrator retries the peer.
  Administrators must resolve interrupted operations before peer processing resumes.
- **File contents** (new and modified files) are published as managed uploads, queued for transfer
  during **allowed hours** (Sync → Schedule). An override permits a share or peer outside those
  hours for a set time. Uploads and other writes by ISM schedule a rescan of the affected shares,
  so their content is announced without waiting for the full scan.
- If the saved schedule is invalid, transfers stop rather than defaulting to unrestricted hours.
  An active, explicitly scoped override still applies. The Sync page provides a replacement
  schedule with no allowed days; choose the desired hours and save to resume scheduling.
- Changes made outside ISM are picked up from ISM's [file watcher](watching.md): a change under a
  shared folder rescans that share shortly after it settles, and a full rescan still runs every
  `ISM_SCAN_INTERVAL_SECS` as a safety net. Each scan
  hashes settled files, including files with unchanged size and modification time; this can cause
  substantial disk reads on large shares. To avoid recording half-completed managed operations,
  each scan and managed operation runs exclusively of the other within this instance. A long scan
  can therefore delay managed operations and application of incoming events, even though polling
  and scanning run separately. Peer connections are polled independently, so a slow peer does not
  prevent the others from being contacted. Scans and peer bookkeeping also stop while an operation
  needs recovery, until an administrator resolves it. Managed file trash records deletion events
  for the original and moved companions immediately, without waiting for the next scan.
  This coordination does not stop external
  programs modifying files.
  Local operation replica updates and events are committed together; if they fail after files have
  changed, the operation requires recovery rather than reporting success. This transaction does not
  make the filesystem, operation status and peer events atomic across a crash.
- Invalid event page ordering, cursors or identities, invalid file metadata, unsupported event
  types, reused event identities with different content, and equal file versions with different
  contents stop the affected peer's event cursor rather than being silently acknowledged. The Peers
  page shows the processing error while retaining its connected status when the peer is reachable.

## Conflicts

The **Conflicts** page lists each conflict with both versions (size, checksum, time) and, for moves,
the path the peer moved from. ISM records a conflict when:
- `changed_on_both`: both sides changed the file since they last agreed;
- `changed_here_deleted_there`: one side changed it and the other deleted it;
- `move`: the peer moved a file that changed here, or the destination is taken;
- `folder_move`: a folder move cannot be applied the same way here;
- `apply_failed`: an incoming change could not be applied.

Resolutions:

| Choice | Effect |
|---|---|
| **Keep this instance** | Publishes the local version (or deletion) as the decision; the peer applies it. |
| **Keep the peer** | Downloads the peer's version (the local one goes to the ISM trash), or trashes the local copy if the peer deleted it. For a move, fetches the file at the peer's new path and trashes the local source; for a folder move, moves the local folder the same way. |
| **Keep both** | Renames the local file to a `… (conflict …)` copy, then downloads the peer's version under the original name. Both instances end up with both files. For a move, the peer's file is fetched at its new path and the local source stays. Not offered for folder moves. |
| **Skip** | Dismisses the conflict without changing either side. The next change to that path is compared again. |

Keeping the peer or both runs as a managed operation (and transfer). The conflict stays
`resolving` until it finishes, and the outcome is announced to the peer. If the operation fails,
the conflict reopens with the error appended to its detail. A choice that cannot be applied safely
is refused and leaves the conflict open; for example, the destination of a move is taken or the
peer's version is unknown.

If different pending peer versions have concurrent or inconsistent histories, ISM keeps the
existing transfer and leaves the incoming event unacknowledged. The peer's processing error
identifies this condition.
