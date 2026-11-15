# Known limitations

- **Immich v3.4.1 only.** Other versions block writes until support is added and tested.
- **Immich write safety rests on the barrier.** Immich-linked writes run only while Immich's watcher
  and scheduled scans are off and its `library` queue is paused and idle; this was proven end to end
  against stock v3.4.1 (`docker/immich-proof/RESULTS.md`). Other Immich job types are not paused; a
  job racing a move fails to read the old file rather than changing the library.
- **Change detection is ISM's.** Immich's file watcher and scheduled scans stay off; changes made
  outside ISM reach Immich through ISM's watcher, with a library scan 2 minutes after the last change.
  Polled network shares notice changes only at the next pass (default every 5 minutes). Mappings
  that are not linked to an Immich library or a peer share are not watched.
- **Ownership transfers** are refused when other Immich rows that reference the asset (for example
  album membership or shared links) would be left inconsistent. The operation preview lists them.
- **Trash.** ISM's trash keeps files in `.ism-trash` in each root and the assets in Immich's trash. If
  Immich empties its trash first, restoring brings the files back as new assets (with new IDs).
  File-group restores check the names, types and SHA-256 hashes of quarantined originals and
  companions, and refuse an incomplete or changed group. Folder restores check a recursive inventory,
  including empty directories; folders containing symlinks or special files cannot be trashed.
  Older file-group and folder trash entries without inventories cannot be restored automatically.
  Inventory checks do not exclude concurrent external writers.
- **Peer sync backfills by inventory, not history.** A share that is added or re-enabled compares
  each peer's current inventory of it, not the events it missed. Intermediate renames are seen as a
  deletion plus a new file, so on an Immich-linked root such a file is imported again with a new
  asset ID. Paths with an open conflict are skipped until the conflict is resolved.
- **Skipping a conflict changes nothing.** The two sides stay different until one of them changes
  that path again.
- **Cross-filesystem moves are refused.** A verified, recoverable cross-filesystem move is not yet
  implemented; move files only within the same filesystem.
- **External writers are not excluded.** Path checks reject existing symlinks, but they do not
  prevent another process from swapping a path after it has been checked. Coordinate access to
  managed roots; these checks are not an exclusive filesystem barrier.
- **One server per data directory.** Upload locking is process-local. Do not run multiple ISM
  processes against the same data directory or concurrently write the same managed root; use
  separate roots for peer instances.
- **Clock skew** over 5 minutes between peers rejects peer requests.
