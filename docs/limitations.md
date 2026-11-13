# Known limitations

- **Immich v3.4.1 only.** Other versions block writes until support is added and tested.
- **Untested against a live Immich.** The maintenance barrier and direct writes are covered by tests
  against a mock API and a schema fixture; validate on a non-production copy first.
- **Ownership transfers** are refused when other Immich rows that reference the asset (for example
  album membership or shared links) would be left inconsistent. The operation preview lists them.
- **Trash.** ISM's trash keeps files in `.ism-trash` in each root and the assets in Immich's trash. If
  Immich empties its trash first, restoring brings the files back as new assets (with new IDs).
- **Peer sync** compares versions per file; it is not a general-purpose backup. Very large first
  syncs are best seeded by copying files by hand, then letting the scan register them.
- **Clock skew** over 5 minutes between peers rejects peer requests.
