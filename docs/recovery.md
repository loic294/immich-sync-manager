# Recovery

ISM fails closed: sync recovery completes only after ISM verifies the filesystem, replica
bookkeeping, and any required Immich effects. Failed sync work enters a durable recovery queue
automatically; the **Needs attention** tab also provides **Fix selected** and **Fix all**.

**Operations -> Background activity** shows recovery execution and peer forwarding alongside
the tasks they depend on. Waiting entries explain unavailable evidence, mounts/peers, maintenance,
or version decisions; related operation links open existing recovery details. Interrupted background
run records are diagnostic history, not permission to bypass a barrier or automatically retry a
file operation.

Recovery retries retain separate background activity outcomes, including repeated equivalent
failures and changed errors. A later finished run moves earlier failures into history; persistence
does not rewrite already-resolved failures. History still follows the normal retention limits.
This reporting does not change recovery eligibility, retry delays, or durable attempt accounting.
Outcome resolution uses indexed timestamp/revision ranges: resolved history is excluded from
older-failure updates, and late outcomes seek newer records without walking all retained history.
Resolving genuinely unresolved predecessors still requires updating each of those records.

| Recovery state | Meaning |
| --- | --- |
| **Queued / Fixing** | Recovery is admitted or running, not yet complete. |
| **Waiting** | A dependency, unavailable mount or peer, or incomplete evidence prevents verified progress. |
| **Needs a decision** | Valid versions disagree; select an instance's version or preserve both in ISM. |
| **Fixed** | The original work or its linked replacements completed with verified effects. |
| **Not retryable** | Recovery verified that no complete copy remains in available peers, staging, or applicable ISM trash. |

Batch admission returns an outcome for each target. A queued request is not reported as fixed.
The **Needs attention** filters cover the entire queue by share, subfolder, issue type (reason),
recovery status, and item kind. Subfolders include their descendants. Changing a filter clears
the selection; otherwise, selections persist across pages. **Fix all** snapshots only matching
local and mirrored receiver-owned issues, up to 5000 per batch. An operation matching a location
filter may affect other locations as well. Issues without a known location, including transfers
and operations reported by older peers, appear only when no location filter is selected.
Outgoing actions are authenticated and forwarded
to the receiver; each peer can act only on its own work. Requests survive receiver
unavailability and restart. Version choices reuse the owner-scoped comparisons already reviewed
in the current page tab. Admission saves the selected revisions in one local batch transaction,
without fetching peer evidence or hashing files again. The recovery worker revalidates those
revisions before any file changes, including after restart; changed versions stay open for a new
decision. Folder comparisons include empty
directories; changed content or directory structure requires a refreshed decision. Preview
availability does not determine whether repair can proceed.

If a folder restoration reopens with new evidence, it needs a new reviewed decision.
Completion or cleanup from an older generation cannot settle the reopened decision.

ISM prepares version evidence automatically when it detects a conflict and stores it across
restarts. Preparation uses bounded workers and retries when a peer or filesystem is unavailable;
reconnecting a peer wakes its pending work. Supported JPEG, PNG, WebP and AVIF files up to 4 MiB
can be previewed from the relevant instance after checking the saved revision; other files do not
currently have a conflict preview. Peer preview bytes observe the applicable file-transfer
schedule and overrides; local previews and evidence remain available outside transfer hours.
Saved comparisons are not permission to change files: ISM
verifies current evidence again before carrying out a decision. Folder evidence is limited to
10,000 entries; larger folders require intervention rather than an unbounded background scan.
Lineage lookup uses bounded pages, but a very slow peer or unusually long history can exceed the
evidence deadline. ISM reports the failure and retries; a retry currently starts that lookup again.

### Folder review protocol layer

`ops::review` provides local durable folder manifests as a foundation for paginated evidence,
not a new decision API. Operations and reviews share a streaming traversal that includes the
folder itself and every empty directory, rejects symlinks/special files and detects observed
concurrent changes. It retains at most 128 directory levels rather than buffering arbitrarily
wide directories; deeper trees fail explicitly. Staging writes and scans use bounded batches,
and a second full-tree comparison must succeed before a manifest is published.

Ready manifests are immutable. Pages contain at most 500 entries, ordered by binary relative
path, with a keyset cursor and one content-bound revision for the entire tree. This is a page
size, not a total-entry cap. A continuation cursor must name an entry in that same manifest;
an invalid cursor is an error, not an empty final page. The lookup also binds the authenticated local owner, root and
folder path. A peer cannot obtain or authorize receiver-owned data through this layer.
Ready evidence survives restart. Interrupted staging remains unreadable and is rebuilt under
a new ID; cleanup removes only expired manifests, so opening another connection cannot delete
an active preparation. All manifests expire after 24 hours; expired evidence must be refreshed,
not reused for a queued operation.

This layer is not yet wired to `RecoveryVersions`, `FileEvidence`, preview URLs or reviewed
operation authorization: those contracts still require complete vectors and retain their
10,000-entry fail-closed limit. Integration must negotiate manifest-aware peer capabilities,
bind both instance-owned manifests and the conflict generation into the decision revision,
retain the receiver's evidence through queued execution, and run the full live-tree verifier
inside the managed operation's maintenance barrier immediately before any mutation. Reading
every page, trusting a sender's manifest ID, or verifying only a displayed page is not sufficient.
The current verifier checks all paths, types, sizes and content hashes again, but returns no
mutation grant. ISM's barrier cannot freeze external writers; coordinate those writers before
folder changes. No new UI layout or peer wire contract is introduced by this layer.
The current move/restore decision journals and trash pre/post-mutation checks still persist
complete inventory vectors. Pinning a manifest alone cannot make that execution and compensation
path bounded or establish missing peer history, so it must not lift the decision cap.

Managed `CheckTree` execution and manifest preparation share the bounded streaming scanner.
Legacy checks compare against the existing expected inventory without building another full
actual inventory, retaining one visited bit per expected entry to reject duplicate observations.
They await the scanner's final result, including directory-change and read errors, before any
subsequent mutation. Expected inventories and persisted journals remain legacy vectors; this
reduces verification memory but is not the reference-backed execution/compensation contract
needed to authorize larger reviewed decisions.

That contract needs bounded per-action/per-entry decision storage and journaled path projections.
Receiver-owned references must bind the owner, target, conflict generation and both sides'
revisions. Pins must survive queued/restarted execution and incomplete compensation, with
explicit retention through trash and purge rather than the current review expiry. Peer decisions
also need negotiated capabilities and complete-history guarantees; without these proofs, larger
folders remain blocked even when a complete local manifest is available.

Independent paths, shares, and servers continue syncing. Overlapping paths and causally dependent
events wait, and an unresolved Immich maintenance barrier still protects that server. A replacement
transfer may pass its own failed parent's path gate, but never that parent's unresolved maintenance
barrier or another operation's gate. Waiting for busy work, unavailable mounts, or unavailable peer
evidence does not consume the recovery attempt counter.

Recovery runs up to four jobs per kind, with at most three jobs per attributed peer. Scheduling
prefers one job per peer or the unattributed group before filling spare slots. Unattributed jobs
can use up to three slots per kind. Forwarding uses up to four peers and refills a slot as soon as
its worker finishes.
These bounded limits do not guarantee immediate progress: multiple stalled jobs can fill a kind's
slots before another peer arrives, three stalled unattributed jobs block other unattributed jobs,
and shared maintenance or path barriers still apply. Jobs are not cancelled to free slots. Waiting
jobs retry after 30 seconds, except for legacy publication archives missing their original
version vector as described below; interrupted claims are recovered before the affected queue resumes.
Jobs waiting for a started repair to finish, and failed peer items that are no longer in the sync
queue (`sync_queue_entry_missing`), are rechecked every 5 minutes without consuming an attempt; Fix
rechecks them at once.

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

A peer publication that failed before execution because its destination already exists has no
pending sync entry to re-add. New transfer archives retain the exact merged queued version vector.
Recovery verifies that vector and transfer identity against the current destination. Identical
content completes the publication; different content opens a normal version conflict.
An unambiguous managed trash operation after the publication failure can also prove an identical
destination, but only when its inventory and deletion event verify the content and causal history.
Recovery preserves that deletion and its version; it waits if evidence is missing or ambiguous.
It does not write or restore the library file.

Older transfer archives have no saved version vector; matching hashes, event timestamps, or a
single matching event cannot establish their merged history. Those publications remain visible in
**Waiting**, and their files and recoverable records are preserved. They do not automatically
retry. **Fix** rechecks the evidence but cannot complete the
publication while the original vector is unavailable.

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
