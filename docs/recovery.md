# Recovery

ISM fails closed: when it cannot prove a step worked, it stops changing that Immich server and says why.

| State (Servers page) | Meaning | What to do |
| --- | --- | --- |
| **Blocked** | Immich's `library` queue could not be paused, confirmed idle or resumed. Nothing was modified. | Check Immich's jobs page (a long-running library job must finish), then **Retry**. |
| **Settings drift** | Immich's file watcher or scheduled library scan was turned back on. Writes are refused. | Click **Turn them off again** on the Servers or Overview page. |
| **Recovery required** | An item failed and undoing it also failed. Immich's `library` queue stays paused. | Open the operation, fix the listed files (each item says what it expected), click **Resolve** on the operation, then **Mark recovery as done** on the server. |
| **Partial** operation | Some items succeeded and some were rolled back. | Read the item errors; resubmit what is still wanted. |

After recovery is marked done, the `library` queue is resumed and due library scans run.

If an item reports `peer_bookkeeping_failed`, its filesystem changes may already be complete, but
replica rows and events may be only partly updated. Inspect and reconcile both before resolving the
operation; peer scans and event processing remain paused while recovery is unresolved.

If the container is restarted mid-operation, unfinished items are marked for recovery on start and the
server keeps Immich's `library` queue paused until an admin confirms. If ISM had paused the queue
and no operation was running, it resumes the queue on start. Corrupt maintenance state or a failed restart recovery
prevents the server from starting rather than assuming it is safe.

Interrupted peer transfers return to the queue on restart while peer publication is disabled.
Failed and cancelled transfers are left unchanged.

If a mapped root is unavailable, verify that the intended volume is mounted before repairing its
`.ism-root` marker. The marker must be a regular file containing that mapping's root ID; an empty,
different, missing, or symlinked marker is refused. Older empty markers need manual verification and
repair. Matching root IDs alone do not prove that the correct physical volume is mounted.
When two independent source mappings share the exact same folder, the additional mapping uses
`.ism-root-<root-id>` instead. Each marker contains its own root ID; removing one mapping leaves
the other's marker intact. These markers are hidden from listings and cannot be accessed by clients.

Trash created before the corresponding file-group or recursive folder inventories were introduced
cannot be restored automatically: ISM cannot determine which entries were originally quarantined.
Preserve the quarantine and inspect it against a trusted backup; do not invent an inventory to
bypass this check. New inventories check names, types and hashes, including empty directories.
Folder trash rejects symlinks and special files. Other processes can still change files concurrently;
coordinate external writers before deleting or restoring a folder.

If Immich's `library` queue stays paused after ISM was removed or its state lost, resume it from
Immich's administration → Jobs page. Immich's file watcher and scheduled library scans are meant to
stay off while ISM manages the server.

## File watching

A mapping whose folder is unreachable, or that suddenly looks empty after holding files, pauses
change detection with a warning instead of treating every file as deleted. Check the mount, then
click **Accept contents** on the Mappings page to take the current contents as the new baseline.
