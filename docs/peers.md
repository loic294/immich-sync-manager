# Peer sync

Several instances, each next to its own Immich, can keep the same folders in sync. Use a private
network between them (for example Netbird) and point each peer at the other's private address.

Every change a peer applies goes through the same managed operations as local requests. On a
root inside an Immich library they run under the write barrier and keep asset IDs, albums and
favorites (see [immich-compatibility.md](immich-compatibility.md)). Replaced or deleted files go to
the ISM trash, never straight to deletion.

## Pairing

Use **Operations -> Background activity** to see peer connection checks, event processing,
inventory backfill, file transfer/verification, and recovery work together. Queued transfers are
grouped by peer and share and show confirmed conditions such as an offline peer, allowed hours,
retry backoff, or an unavailable root. File-specific eligibility may remain unknown; a few blocked
files do not prove the entire queue is blocked. Use **Sync** for per-file details and **Peers** for
connection controls. Queued share scans report readiness as unknown until the scan itself reports
its state. Conflict comparison work is grouped on Operations, with failed attempts, retry times,
and observed worker exits linked to **Conflicts**. A peer-specific wait is not a claim that every
share or server has stopped.

1. On instance A: **Peers → Create invite**. The invite is valid for 24 hours and works once.
2. On instance B: **Peers → Accept invite**, paste it, and give A's URL (e.g. `http://192.0.2.10:8080`)
   and B's own callback URL as seen from A.
3. Both sides now share a secret; every peer request is signed and must be within 5 minutes of the
   other clock, so keep NTP on.

### Change notifications

Paired peers can open `GET /api/peer/v1/notifications` with the same signed GET authentication.
This optional SSE endpoint sends `event: changed` with `data: {}` for local sync and recovery
changes, coalescing bursts over 250 ms. It never includes paths, identifiers, transfer content or
remote status mirrored for the UI. Idle connections receive a comment keepalive every 15 seconds.

`event: catch_up` (also `{}`) means the subscriber lagged behind the bounded notification buffer.
These are invalidation hints, not a durable event log: fetch authoritative sync/recovery state
after connecting, reconnecting, or receiving either hint; use `/events` for replication cursors.
Peer removal and committed peer/share configuration changes close existing streams immediately.
Credentials and peer identity are also rechecked before every hint and keepalive; verification
errors close the stream. Credential/configuration writers must call
`AppState::peer_configuration_changed` immediately after committing their change.
Allow long-lived, unbuffered responses through reverse proxies. The peer progress/recovery client
uses this stream instead of five-second refreshes when supported. It reads notifications independently
of authoritative fetches and coalesces refreshes to at most once per second. Routine successful
refreshes do not create background task history; fetch errors are logged and remain visible in Sync.
Keepalives do not fetch state or write heartbeat timestamps to the database. A connection-scoped
in-memory heartbeat keeps unchanged snapshots current only after both progress and recovery fetches
succeed for that connection and its latest hint. New hints, failures, disconnects and configuration
changes invalidate that verification; keepalives alone never authorize failed or previous snapshots.
Closed, silent (45 seconds), or invalid streams report
availability errors and reconnect after five seconds, fetching state again on connection.
Only HTTP 404, 405, or 501 selects legacy five-second polling; legacy peers are probed every minute
for an upgrade. Authentication and server errors never silently select polling. Peer removal or
identity/credential changes cancel the client's worker. Replication, scans, transfer admission and
recovery execution retain their independent schedulers.

## Shares

A share is a folder replicated under the same name on each instance: pick the local root and
subfolder, then the peers. Create the share with the same name on the other instance, mapped to its
own folder. Folders may sit at different paths on each machine.

Local failed share scans stay in the existing attention banners on Overview, Operations and Sync.
They name the affected share and link to **Peers** to check its configured folder. A permission
warning means ISM was denied read access: restore folder traversal and file read permission for
the account running ISM. Other verified folder errors suggest checking the folder and logs;
unknown or system errors suggest checking logs without diagnosing the folder.
ISM retries on watcher changes and its periodic full scan; there is no manual share-scan retry
button. Unreadable paths are not treated as deleted.
A failure survives retries, partial scans and restarts until a complete scan of that same share
succeeds. Disabled shares hide their warning without erasing it; deleting a share removes its
warning. Server logs identify the share and failure category, evidence-save failures, and recovery.
Share-scan warnings are local to the instance running the scan; peers do not report them through
the current cross-instance attention protocol.

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

Hidden paths are skipped. Conflicting or uncertain entries are retained in a durable inbox
while independent entries continue. The inventory position is saved atomically with each received
page, so a restart cannot lose entries deferred behind a folder operation or an unavailable root.
A restarted backfill has a new request generation, so a late page from an earlier request cannot
advance it or insert entries. Changing the share's membership or enabled state restarts its pending
backfills, even if the change is later reversed. This does not undo pages already admitted or make
inventory a complete historical record.

Incoming events likewise have separate **received** and **applied** positions. A page advances
the receipt position only after its identities and payloads are validated and durably stored.
The application position advances only past completed effects; failed or unapplied effects are
not forwarded as successful events. Reception is bounded to 10,000 retained events per peer,
500 events per page and 64 KiB per event. Deferred work is revisited fairly on subsequent polls,
including after a restart; unrelated events from the same peer and share can proceed.

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
  **Sync → Active & queued** and defer overlapping source/destination paths, descendants, companions
  and dependent events. Other folders, shares and servers continue. An unresolved maintenance
  barrier additionally protects the affected Immich server.
  Failed sync work enters recovery automatically; **Needs attention** offers individual and batch
  fixes and in-app version decisions when needed.
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
  Authenticated peer transfers can publish sidecars and configured companions independently of an
  original. Each file retains its role and is verified individually. A later original is imported
  normally. Deleting one announced file does not delete an unannounced companion.
- If the saved schedule is invalid, transfers stop rather than defaulting to unrestricted hours.
  An active, explicitly scoped override bypasses the schedule check for matching transfers, but
  an earlier transfer without an override can prevent the dispatcher from reaching them. The Sync
  page provides a replacement schedule with no allowed days; choose the desired hours and save to
  resume scheduling.
- Changes made outside ISM are picked up from ISM's [file watcher](watching.md): a change under a
  shared folder rescans that share shortly after it settles, and a full rescan still runs every
  `ISM_SCAN_INTERVAL_SECS` as a safety net. Each scan
  hashes settled files, including files with unchanged size and modification time; this can cause
  substantial disk reads on large shares. Scans exclude queued transfers and uncertain operation
  or inbox footprints from hashing and deletion inference, while independent paths remain eligible.
  Unavailable roots never imply deletion. An unreadable subtree is excluded and the incomplete
  scan is reported explicitly. To avoid recording half-completed managed operations, a scan
  never runs while a managed operation is changing files within this instance (or while an
  operation is left active by a restart). Scans do run while an operation waits for its
  maintenance barrier, since it has changed nothing yet; it then waits for any scan in progress
  before its first change, which can lengthen the Immich job pause. Other bookkeeping, including
  application of incoming events, still waits for every active operation. A long scan
  can therefore delay managed operations and application of incoming events, even though polling
  and scanning run separately. Peer connections are polled independently, so a slow peer does not
  prevent the others from being contacted. Each peer has at most one synchronization worker and
  one activity-refresh worker; admitted workers keep progressing while their schedulers query
  the peer list. Worker failures are logged and release the peer's admission slot for retry.
  Stopping a scheduler cancels its admitted workers rather than leaving them detached.
  While a managed operation is running or waiting for maintenance, peer bookkeeping currently
  waits globally, even for unrelated paths. After it stops, recovery and failed peer operations
  protect their uncertain footprints rather than stopping all scans or peer bookkeeping. Managed file trash
  records deletion events for the original and moved companions
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
  types and reused identities with different content reject receipt without advancing its cursor.
  Equal file versions with different contents retain the affected event unapplied while unrelated
  work continues. The Peers page shows processing errors while retaining connected status when
  the peer is reachable. Removing a remote event does not discard an already validated receipt.

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

Conflict previews verify the complete original against the reviewed checksum before display.
JPEG, PNG and WebP originals up to 64 MiB can be resized for preview, subject to a 60-megapixel
and 12,000-pixel-per-side limit. AVIF previews retain their 4 MiB limit. Previewing a peer's
contents follows the configured transfer hours.

Resolutions:

| Choice | Effect |
|---|---|
| **Keep this instance** | Publishes the local version (or deletion) as the decision; the peer applies it. |
| **Keep the peer** | Downloads the peer's version (the local one goes to the ISM trash), or trashes the local copy if the peer deleted it. For file and folder moves, rebuilds the verified peer destination, including nested files and empty folders; displaced local sources and destinations go to ISM trash. |
| **Keep both** | Renames the local file to a `… (conflict …)` copy, then downloads the peer's version under the original name. Both instances end up with both files. For file and folder moves, keeps the local source, preserves an occupied destination under a conflict-copy name, and fetches the peer's destination. All retained copies are announced to the peer. |
| **Skip** | Refused while an executable recovery or version choice remains available. |

Keeping the peer or both runs as a managed operation (and transfer). The conflict stays
`resolving` until it finishes, and the outcome is announced to the peer. Move choices persist
their preparation and publication plan across restarts; a failed step retains its recovery
footprint instead of settling the conflict. Keeping this instance for a move announces its
verified layout and contents, rather than merely dismissing the conflict. Peer-only contents
removed by that choice go to the peer's ISM trash. A choice without authenticated, current
evidence, or involving symlinks or unverifiable paths, is refused without settling it.

Folder trash preparations retain the complete reviewed tree, including empty directories, across
queuing and restarts. Added, removed, or changed contents cannot become authorized merely because
planning starts later. If that tree changes, the failed preparation leaves the contents in place
(or rolls the move back), and reopens the conflict for a fresh review. Legacy preparations without
complete reviewed inventories also require a new choice. A step needing filesystem recovery keeps
its recovery barrier instead of automatically reopening.

Authenticated committed move lineage also follows a peer folder relocated while a choice is
waiting. Unchanged bytes retain the chosen decision; changed competing bytes reopen the choice
instead of silently selecting the new version. If relocation collides with a retained local
source, keeping local or both requires a refreshed in-app choice; already preserved copies
remain included in publication. If the current peer folder overlaps the original source, the
comparison expands to their common ancestor on both instances, including sibling files and empty
folders. Keeping both preserves the receiving instance's complete ancestor as a conflict copy.
Recovery evidence is refused when its lineage overlaps an unresolved or later committed folder
prune; unrelated paths remain available.

If a failed incoming folder move has lost both local paths, ISM can restore its old source from
the peer's complete current inventory, then finish the original managed move. Downloads retain
their verified remote source separately from the receiver target and resume across restarts.
Restoration uploads execute inside the original operation's maintenance window without
registering replacement Immich assets. Only the final verified move completes recovery;
unavailable or changed evidence leaves the original operation outstanding. When source content
changes, ISM retains the earlier generation and requests a fresh in-app decision. If the peer
also moved the folder or changed its directory structure, the decision applies to the complete
current tree. Changes to the receiving tree after review likewise reopen the choice.

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
operations appear beside file transfers. Recovery requests for mirrored issues are forwarded to
the receiver and survive disconnection; the source's mirrored row updates after the receiver acts.
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
