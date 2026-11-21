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

- Synology `@eaDir` metadata and every name matching the **Ignored names** or **Ignored prefixes**
  of the file settings (see [Shared file settings](#shared-file-settings)) are excluded at every
  path depth, independently of Immich library exclusions. Originals and their sidecars remain
  eligible, as do other dot-prefixed and `@`-prefixed folders; ISM's reserved paths remain
  excluded.
  Previously tracked metadata is ignored, not announced as deleted, and historical metadata events
  and inventory entries are not advertised or applied. Moves with an excluded source or destination
  are not replayed; ordinary files moved out of metadata are discovered by the next share scan.
  A legacy queued metadata transfer fails with an explicit exclusion reason when the worker
  examines it during allowed hours (or an override), without adopting or publishing its content.
  Existing metadata files, replica history and staging data are left untouched; this is not a
  remote cleanup or migration of already accepted managed operations. Upgrade both peers to
  enforce the policy on both sides.
- **Management events:** moves, renames, folder creation, folder moves, file trash and folder trash
  done through ISM become peer events. A peer applies each one as a managed operation, so on an
  Immich-linked root the asset keeps its ID and only its path changes. An event is acknowledged
  only after the operation and its bookkeeping succeed. Failed or cancelled operations stay in
  **Sync → Active & queued** and block later peer processing and rescans of the affected library.
  Administrators inspect the issue and explicitly choose **Re-add to sync queue**; ISM never retries
  a failed file operation automatically. Recovery-required operations must be resolved first.
- **File contents** (new and modified files) are published as managed uploads, queued for transfer
  during **allowed hours** (Sync → Schedule). An override permits a share or peer outside those
  hours for a set time. Uploads and other writes by ISM schedule a rescan of the affected shares,
  so their content is announced without waiting for the full scan.
  Each receiving instance downloads three files concurrently by default, starting the next
  eligible file as soon as a slot becomes free rather than waiting for a batch. Set
  `ISM_PEER_TRANSFER_CONCURRENCY` to 1–32 to tune the global peer download limit; 6–8 is a
  reasonable starting point for high-latency links, provided both source and destination storage
  can sustain the extra I/O. Publication still runs through the serialized managed-operation
  barrier. On the receiver, **Sync → Active & queued** shows incoming files and their progress,
  including files awaiting publication; successful publications move to **Completed syncs**.
  Downloads use bounded 16 MiB byte-range requests and resume from the staged offset, so a large
  video is not one long request through an HTTP proxy. Before serving a range, the source checks
  that the file's tracked checksum, size and modification time still match the announced version;
  it also freshly rehashes small files to detect same-metadata edits before responding. The
  receiver verifies the SHA-256 of the complete staged file before publication.
  Sidecars wait for their matching original to finish publishing, so concurrent downloads cannot
  publish a companion before its original exists. A stem sidecar shared by a JPEG and its RAW or
  video counterpart, or by an original and a timestamped `.edited`/`-edited-` variant, is
  published using a deterministic original as its upload anchor; unrelated same-stem files still
  fail closed as ambiguous. After an upgrade, startup recovery automatically retries transfers
  that previously failed from a 504 or this paired-companion ambiguity.
- If the saved schedule is invalid, transfers stop rather than defaulting to unrestricted hours.
  An active, explicitly scoped override still applies. The Sync page provides a replacement
  schedule with no allowed days; choose the desired hours and save to resume scheduling.
- Changes made outside ISM are picked up from ISM's [file watcher](watching.md): a change under a
  shared folder rescans that share shortly after it settles, and a full rescan still runs every
  `ISM_SCAN_INTERVAL_SECS` as a safety net. Each scan
  hashes settled files, including files with unchanged size and modification time; this can cause
  substantial disk reads on large shares. Full safety-net scans wait while a peer transfer backlog
  or its publication work is active, so restarting or updating ISM does not pause ongoing
  replication behind a complete library rehash. Native watcher-detected outside changes are still
  rescanned promptly. To avoid recording half-completed managed operations,
  each scan and managed operation runs exclusively of the other within this instance. A long scan
  can therefore delay managed operations and application of incoming events, even though polling
  and scanning run separately. Peer connections are polled independently, so a slow peer does not
  prevent the others from being contacted. Scans and peer bookkeeping also stop while an operation
  needs recovery, or while a failed peer operation waits to be re-added, until an administrator
  resolves it. Managed file trash records deletion events for the original and moved companions
  immediately, without waiting for the next scan.
  This coordination does not stop external
  programs modifying files.
  Local operation replica updates and events are committed together; if they fail after files have
  changed, the operation requires recovery rather than reporting success. This transaction does not
  make the filesystem, operation status and peer events atomic across a crash.
- File contents are checked by SHA-256 when the source instance records an announced version, and
  again by the receiving instance after the download. The receiving instance checks the staged
  file immediately before publishing it and checks the final library file after the rename. A
  mismatch stops publication and rolls the managed operation back; none of these checks depend on a
  remote hash result.
- Invalid event page ordering, cursors or identities, invalid file metadata, unsupported event
  types, reused event identities with different content, and equal file versions with different
  contents stop the affected peer's event cursor rather than being silently acknowledged. The Peers
  page shows the processing error while retaining its connected status when the peer is reachable.

## Shared file settings

**Settings → Files and sidecars** holds the sidecar extensions (files that move, rename, download
and sync with their original), the ignored names and prefixes, and the longest name a client may
generate. Open Photo Manager reads the same values from `GET /api/v1/client/settings`.

**Settings → Primary instance** picks the instance whose values are the reference: this instance
or a paired peer. While a primary is chosen:
- only the primary can edit the file settings; every other instance shows its values read-only
  (`409 settings_managed_by_primary` on a change);
- each instance fetches `GET /api/peer/v1/settings` from its peers on every sync poll and adopts
  the primary's values when they are newer than its own copy, so they also reach instances that
  are not paired with the primary directly;
- the choice itself spreads the same way; the latest choice wins.

With no primary, every instance keeps and edits its own values. Older peers without the settings
endpoint are skipped.

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

## Sync activity and progress

Both the source and receiver show the full incoming and outgoing queue in **Sync**, including
completed transfers. The receiver owns the transfer state and shares incremental, signed activity
updates with the source during peer polling. Only activity involving the authenticated peer is
shared; mirrored outgoing activity is never mirrored back. Both peers must be upgraded for
outgoing visibility. An unreachable or older peer leaves the last-known queue visible with a
warning rather than making it look empty or current. Outgoing transfer controls and publishing
operations are managed on the receiving instance. Incoming move, rename, trash and folder
operations appear beside file transfers; failed entries expose **Re-add to sync queue**, and the
source's mirrored row updates after the receiver handles that action.
Activity is polled independently every five seconds, so scans, replication backfills, and the
operation barrier cannot block status updates. Before the first receiver response, the view
reports unavailable peer status instead of silently treating the unknown queue as empty.
Completed thumbnails try the local instance first and fall back to the peer's preview if Immich
has not indexed the local copy yet, rather than immediately replacing the image with a file icon.

The summary above the queue shows completed files versus total files, transferred bytes versus
total bytes, average throughput over the last minute, and estimated remaining transfer time.
The progress bar and estimate use bytes, not file counts. Throughput measures newly received
network bytes (including retries), not adopted identical files or resumed bytes already on disk.
The web UI displays that rate in decimal network bit-rate units (`Kbps`, `Mbps`, `Gbps`);
byte totals continue to use data-size units. Before a minute has elapsed it uses the elapsed run
time. The estimate is remaining bytes divided by that throughput; it excludes allowed-hours waits
and managed publication, and is unavailable when no recent throughput is known or a peer's status
is stale.

Run totals are tracked per peer and direction, combined for the local instance. A new run starts
when new work arrives after that queue has finished; completed history remains available in the
Completed tab. Pending publication or failed work keeps its current run. Totals and mirrored
queues survive a restart, while the rolling rate naturally decays to zero after a minute without
traffic. The page refreshes every five seconds while open, even without new events, so rates and
estimates do not remain stuck on an old value.
