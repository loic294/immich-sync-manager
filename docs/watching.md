# File watching

Immich's own file watcher and scheduled library scans stay **off**; ISM detects file changes in the
mapped folders and synchronizes them with Immich. Every mapping linked to an Immich library or a peer
share is watched.

## Modes

Set per mapping on the Mappings page (eye button):

| Mode | Behavior |
| --- | --- |
| **Auto** (default) | Polling on network and VM-shared filesystems (SMB/CIFS, NFS, 9p, virtiofs as used by Docker Desktop, …, detected from the filesystem type), native events otherwise. |
| **Native** | Operating-system change events. Falls back to polling, with a warning, when events overflow or the watch limit is reached. |
| **Poll** | ISM compares the folder with a saved snapshot (path, size, modification time, inode) every poll interval (default 5 minutes, 1 minute to 24 hours). |

A full polling pass runs after every restart, so changes made while ISM was stopped are found.
Passes are throttled. ISM's staging and trash folders, partial or temporary files and the Immich
library's exclusion patterns are ignored.

If the folder is unreachable, or a snapshot that held files is suddenly empty, detection **pauses**
for that mapping with a warning; nothing is treated as deleted. After checking the mount, click
**Accept contents** to take the current contents as the new baseline.

## ISM's own changes

Before ISM moves, renames, trashes, restores, uploads or copies a file from a peer, it records the
expected result (path, and size and modification time when known) for a limited time. Matching
changes are ignored by the watcher; anything else is treated as an outside change. Uploads, peer
copies and restores use the existing file APIs first. Work not already handled by an operation's
API/database updates enters the scoped synchronization queue. Successful direct synchronization
does not schedule another scan.

## Scoped synchronization and scan fallback

Each detected outside change (or ISM request) in a library's import path sets its synchronization
to run **2 minutes** later (`ISM_SCAN_DELAY_SECS`); every further change restarts the timer. A due
batch waits while a managed operation holds or prepares the server's write barrier. Exact paths,
change kinds and requests arriving during a running batch are saved in SQLite and survive restarts.

Outside additions and removals first use the fork's `/libraries/:id/files/add` and `files/remove`
APIs. Successfully handled files cause **no scan**. Modified files have no direct update endpoint,
and the add endpoint does not update existing assets, so they request the affected parent folder.
Unsupported companion additions and compatible route-unavailable add requests also fall back to
that folder. Directory imports use the directory itself. Overlapping folder scopes are coalesced;
other folders and other library import paths are not included.

Every fallback uses `POST /libraries/:id/scan` with a nonempty `path` in **Immich's path namespace**,
not the ISM container path. ISM requires the fork's external-library capability (probing when
unverified); stock Immich must not receive a scope it might ignore. There is **no automatic
full-library fallback**, including when the scoped endpoint rejects an inaccessible/deleted folder.
Explicit full scans requested manually in Immich are unchanged.

Authentication errors, known-library errors and failures of an API previously confirmed available
remain errors, following the existing capability rules. Failed work is retained and its error
appears in the Overview queue; a later request retries it. The fork's folder scan discovers new
files but does not reconcile existing-asset metadata or removals. In particular, a failed outside
removal is reported rather than pretending a folder scan repaired it. Refreshing existing metadata
remains an Immich API limitation; ISM does not work around it with a full scan.

After a successful batch, including one handled entirely by file APIs, ISM emits the existing
`library_scanned` event and looks up imported upload asset IDs. For queued folder scans it first
waits for the library queue to become idle. The Overview page shows the queue with countdowns.

On upgrade, legacy pending scans have no recoverable folder scope. They are stopped with an
explicit queue error instead of being replayed as full-library scans; request scoped synchronization
again for any work that was pending.

## Peers

Changes under a peer share's folder schedule a rescan of that share once the files have settled, so
peers learn about local additions without waiting for the periodic full rescan.
