# Peer sync

Several instances, each next to its own Immich, can keep the same folders in sync. Use a private
network between them (for example Netbird) and point each peer at the other's private address.

## Pairing

1. On instance A: **Peers → Create invite**. The invite is valid for 24 hours and works once.
2. On instance B: **Peers → Accept invite**, paste it, and give A's URL (e.g. `http://100.64.0.10:8080`)
   and B's own callback URL as seen from A.
3. Both sides now share a secret; every peer request is signed and must be within 5 minutes of the
   other clock, so keep NTP on.

## Shares

A share is a folder replicated under the same name on each instance: pick the local root and
subfolder, then the peers. Create the share with the same name on the other instance, mapped to its
own folder. Folders may sit at different paths on each machine.

- **Management events** (moves, renames, folder changes, trash) apply right away, as managed
  operations, so each Immich keeps its asset IDs.
- **File contents** (new and modified files) are copied by the transfer queue during **allowed
  hours** (Sync → Schedule). An override lets one share or peer transfer outside those hours for a
  set time.
- Changes made outside ISM are picked up by a periodic scan (`ISM_SCAN_INTERVAL_SECS`).

## Conflicts

When both sides changed a file, or one changed it and the other deleted it, or an incoming change
cannot be applied, the file shows up on **Conflicts** with both versions (size, checksum, time).
Choose **Keep this instance**, **Keep the peer** (the local copy goes to the trash) or **Keep both**
(the peer's version is saved next to it under a new name).
