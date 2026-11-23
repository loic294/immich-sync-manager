# Recovery

ISM fails closed: sync recovery completes only after ISM verifies the filesystem, replica
bookkeeping, and any required Immich effects. Failed sync work enters a durable recovery queue
automatically; the **Needs attention** tab also provides **Fix selected** and **Fix all**.

**Operations -> Background activity** shows recovery execution and peer forwarding alongside
the tasks they depend on. Waiting entries explain unavailable evidence, mounts/peers, maintenance,
or version decisions; related operation links open existing recovery details. Interrupted background
run records are diagnostic history, not permission to bypass a barrier or automatically retry a
file operation.

| Recovery state | Meaning |
| --- | --- |
| **Queued / Fixing** | Recovery is admitted or running, not yet complete. |
| **Waiting** | A dependency, unavailable mount or peer, or incomplete evidence prevents verified progress. |
| **Needs a decision** | Valid versions disagree; select an instance's version or preserve both in ISM. |
| **Fixed** | The original work or its linked replacements completed with verified effects. |
| **Not retryable** | Recovery verified that no complete copy remains in available peers, staging, or applicable ISM trash. |

Batch admission returns an outcome for each target. A queued request is not reported as fixed.
Selections persist across pages, and **Fix all** snapshots the outstanding local and mirrored
receiver-owned issues, up to 5000 per batch. Outgoing actions are authenticated and forwarded
to the receiver; each peer can act only on its own work. Requests survive receiver
unavailability and restart. Version choices reuse the owner-scoped comparisons already reviewed
in the current page tab. Admission saves the selected revisions in one local batch transaction,
without fetching peer evidence or hashing files again. The recovery worker revalidates those
revisions before any file changes, including after restart; changed versions stay open for a new
decision. Folder comparisons include empty
directories; changed content or directory structure requires a refreshed decision. Preview
availability does not determine whether repair can proceed.

Independent paths, shares, and servers continue syncing. Overlapping paths and causally dependent
events wait, and an unresolved Immich maintenance barrier still protects that server. A replacement
transfer may pass its own failed parent's path gate, but never that parent's unresolved maintenance
barrier or another operation's gate. Waiting for busy work, unavailable mounts, or unavailable peer
evidence does not consume the recovery attempt counter.

Recovery checks retained complete staging, upload staging, local copies, and authenticated share
peers' applicable trash inventories. It can transfer the exact verified file without restoring
the rest of a trashed folder. It also follows authenticated committed move history. A duplicate
hash alone is not proof of a move. Missing old paths and HTTP errors alone
never justify successful completion or discarding work. Saved actions are previewed against current
state before replacement operations are admitted.

An unchanged verified plan has a bounded execution budget shared with its replacements and
preserved across restart. Once used, recovery waits for changed evidence instead of creating
another identical failing operation. Fresh hashes are checked even when file size and modification
time are unchanged; waiting does not prove that data is permanently unrecoverable.

For `peer_bookkeeping_failed` and interrupted publications, recovery verifies recorded sizes,
hashes, folder inventories, and journaled Immich effects before replaying bookkeeping. Repeated
Fix requests coalesce, and verified completion is idempotent. Sync **Resolve** admits this same
workflow instead of accepting an administrator's filesystem attestation; server recovery cannot
be acknowledged while sync maintenance remains unresolved.

On restart, interrupted recovery jobs are requeued. An untouched peer upload can resume only when
its staging still matches the saved request; other interrupted work retains its evidence and any
maintenance barrier. Corrupt maintenance state or failed restart bookkeeping prevents startup
rather than assuming it is safe. If ISM paused a queue and no operation holds it, restart recovery
resumes it.

Cross-filesystem moves retain the source until a complete destination copy has passed content
and metadata verification and has been published. If verification or source removal fails after
publication, the item requires recovery: the source may be complete or partly removed, while the
destination copy is retained. Journal recovery can finish a move using a complete verified source
or destination. Matching partial copies are retained in ISM trash before completion; changed or
unexpected entries are refused. File groups, nested folders, and shared companions retain their
original operation and content identities.
Rollback also uses verified cross-filesystem moves and never replaces an existing source path.
After an interruption, a hidden `.ism-move-*` directory beside the destination may contain an
unpublished `payload`. ISM must retain uncertain staging rather than deleting a potentially
recoverable copy.

An interrupted peer upload is requeued automatically only when every staging file is still present
and exactly matches the recorded size and checksum, no execution step has been recorded, and the
operation does not hold an Immich maintenance barrier. Any missing, changed, partially executed,
or otherwise uncertain upload remains in recovery until ISM can verify a safe plan.

Interrupted peer transfers return to the queue on restart while peer publication is disabled.
Failed transfers enter the shared recovery workflow. Healthy user-cancelled transfers are not
automatically restarted.

An unavailable mapped root puts recovery in **Waiting**. Its `.ism-root` marker must be a regular
file containing that mapping's root ID; an empty,
different, missing, or symlinked marker is refused. ISM does not recreate a missing marker as a
recovery shortcut. Matching root IDs alone do not prove that the correct physical volume is mounted.
When two independent source mappings share the exact same folder, the additional mapping uses
`.ism-root-<root-id>` instead. Each marker contains its own root ID; removing one mapping leaves
the other's marker intact. These markers are hidden from listings and cannot be accessed by clients.

Trash created before the corresponding file-group or recursive folder inventories were introduced
cannot be restored automatically: ISM cannot determine which entries were originally quarantined.
ISM preserves such quarantine entries rather than inventing an inventory to bypass this check.
New inventories check names, types and hashes, including empty directories.
Folder trash rejects symlinks and special files. Other processes can still change files concurrently;
coordinate external writers before deleting or restoring a folder.

If Immich's `library` queue stays paused after ISM was removed or its state lost, resume it from
Immich's administration → Jobs page. Immich's file watcher and scheduled library scans are meant to
stay off while ISM manages the server.

## File watching

A mapping whose folder is unreachable, or that suddenly looks empty after holding files, pauses
change detection with a warning instead of treating every file as deleted. Check the mount, then
click **Accept contents** on the Mappings page to take the current contents as the new baseline.
