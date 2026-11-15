# Immich v3.4.1 safety proof

Reproducible, disposable check of the Immich behaviour ISM relies on. It runs stock images only
(`immich-server:v3.4.1`, the pinned Immich Postgres and Valkey; no machine learning) and uses
synthetic data. Latest results: [RESULTS.md](RESULTS.md).

## Run

```sh
docker/immich-proof/run.sh
```

Needs Docker with Compose and `python3`. It takes about 7 minutes. The script removes any earlier
proof stack, creates `.work/ext` (mounted as the external library `/mnt/ext`) and `.work/ism-trash`
(mounted as `/mnt/ism-trash`, outside every import path), starts the stack and runs `proof.py`.
On exit it runs `docker compose down -v` and deletes `.work/`; images are kept. The output is in
`last-output.txt` (gitignored); the exit code is the number of failed checks.

Options: `KEEP=1` leaves the stack and `.work/` in place for inspection (clean up with
`docker compose down -v && rm -rf .work`); `PROOF_HOLD_SECONDS` (default 60) and
`PROOF_NO_AUTO_SECONDS` (default 180) shorten the waits in checks B and D.

## What it checks

Setup: admin sign-up (`admin@ism.example`), an API key with exactly `ADMIN_PERMISSIONS` from
`crates/ism-server/src/immich/api.rs`, an external library on `/mnt/ext`, four tiny PNGs (one with
an XMP sidecar), `library.watch.enabled` and `library.scan.enabled` set to false (with an
every-minute cron so a scheduled scan would show), and an initial scan. ISM calls use the API key.

- **A** every endpoint in `api.rs` answers with the shape the code expects; server version 3.4.1;
  latest migration `1792568637987-AddAssetOwnerIdUpdateIdIndex`.
- **B** a scan requested while the `library` queue is paused does not run; after resume it imports
  the new file once.
- **C** managed move: album + favorite, pause, wait quiet, move file and sidecar on disk, run the
  `pg.rs` `move_asset` / `move_sidecar` statements in one guarded transaction, resume, scan. Same
  id, album, favorite, online, not trashed, no duplicate, new path and sidecar path. A control shows
  that the same move without the DB update loses identity.
- **D** with watch and scheduled scan off, a new file is not imported for 3 minutes; a manual API
  scan imports it once.
- **E** ISM trash: API trash (`force: false`), file moved to the ISM trash dir, scan; then file
  moved back, API restore, scan: same asset id, active, online.
- **F** a scan clicked as an admin session while paused is held and visible in queue statistics.
