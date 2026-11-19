# File watching

Immich's own file watcher and scheduled library scans stay **off**; ISM detects file changes in the
mapped folders and tells Immich when to scan. Every mapping linked to an Immich library or a peer
share is watched.

## Modes

Set per mapping on the Mappings page (eye button):

| Mode | Behavior |
| --- | --- |
| **Auto** (default) | Polling on network and VM-shared filesystems (SMB/CIFS, NFS, 9p, virtiofs as used by Docker Desktop, …, detected from the filesystem type), native events otherwise. |
| **Native** | Operating-system change events. Falls back to polling, with a warning, when events overflow or the watch limit is reached. |
| **Poll** | ISM compares the folder with a saved snapshot (path, size, modification time, inode) every poll interval (default 5 minutes, 1 minute to 24 hours). |

A full polling pass runs after every restart, so changes made while ISM was stopped are found.
Passes are throttled. ISM's staging and trash folders, partial or temporary files, the Immich
library's exclusion patterns and the **Ignored names** and **Ignored prefixes** from
**Settings → Files and sidecars** are ignored.

If the folder is unreachable, or a snapshot that held files is suddenly empty, detection **pauses**
for that mapping with a warning; nothing is treated as deleted. After checking the mount, click
**Accept contents** to take the current contents as the new baseline.

## ISM's own changes

Before ISM moves, renames, trashes, restores, uploads or copies a file from a peer, it records the
expected result (path, and size and modification time when known) for a limited time. Matching
changes are ignored by the watcher; anything else is treated as an outside change. Uploads, peer
copies and restores still need Immich to import them, so ISM requests a library scan for them.
Moves update Immich's database directly and need no scan.

## Library scans

Each detected outside change (or ISM request) in a library's import path sets that library's scan
to run **2 minutes** later (`ISM_SCAN_DELAY_SECS`); every further change restarts the timer. A due
scan waits while a managed operation holds the server's write barrier and starts once it is
released. The Overview page shows the queue with countdowns. When a scan finishes, ISM emits a
`library_scanned` event that clients waiting for new asset IDs can use to refresh.

## Peers

Changes under a peer share's folder schedule a rescan of that share once the files have settled, so
peers learn about local additions without waiting for the periodic full rescan.
