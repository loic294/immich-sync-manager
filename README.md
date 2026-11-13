# Immich Sync Manager

A companion service for [Immich](https://immich.app) **v3.4.1** external libraries. It runs as one
Docker container on a Synology NAS, a Linux host or Docker Desktop on Windows, and gives other tools
a safe way to change library files without breaking Immich:

- **Offline downloads for the Immich local app.** Clients ask for originals *and* their sidecars
  (`.xmp`, `.aae`, …) with resumable downloads, and upload new files into their original folder
  instead of the user's upload library. Thumbnails and previews still come from Immich.
- **Managed file operations.** Moves, renames, folder create/move/delete and trash/restore, sent in
  batches by Open Photo Manager or the local app, with progress streamed back. Immich keeps asset IDs,
  albums, faces and favourites because ISM updates the asset paths instead of letting Immich see a
  delete and a new file.
- **Peer sync between instances.** Several ISM instances (each next to its own Immich) replicate
  chosen folders over a private network such as Netbird: management events right away, file contents
  during allowed hours, conflicts resolved in the UI.

## How it changes files safely

1. Immich's file watching and periodic library scans are switched off and its queues must drain.
2. The batch runs. Every step can be undone; a failing item rolls back and never half-applies.
3. After **15 minutes** without new work, ISM scans the affected libraries, waits for the scan, then
   restores exactly the settings it changed.

The Immich API is always preferred. Immich has no API to change the path or owner of an
external-library asset, so those few updates go straight to Immich's PostgreSQL, guarded by row locks
and stale-row checks. Without a database URL ISM stays API-only and refuses those operations. If any
step cannot be verified, ISM stops writing ("fails closed") and shows what to fix.

## Quick start

```sh
cp docker/compose.example.yml docker/compose.yml   # edit the CHANGE ME lines
docker compose -f docker/compose.yml up -d --build
```

Open `http://<host>:8080`, create the administrator, then:

1. **Servers**: add the Immich server and one or more accounts (username/password). Together they
   must own every external library you want managed; ISM routes each change through the owning account.
2. **Mappings**: link each mounted folder (`/mnt/...`) to its Immich library import path and, if used,
   to the Open Photo Manager path prefix.
3. **Tokens**: create a token for each client (Open Photo Manager, Immich local app).
4. **Peers** (optional): pair another instance and choose the shared folders.

More: [install](docs/install.md) · [Immich compatibility](docs/immich-compatibility.md) ·
[peer sync](docs/peers.md) · [recovery](docs/recovery.md) · [limitations](docs/limitations.md) ·
[client API](docs/client-api.md).

## Development

Rust (Axum, SQLx/SQLite) in `crates/`, Lit + DaisyUI web UI in `web/`, the same layout as
Open Photo Manager. `make check` runs every gate; see [AGENTS.md](AGENTS.md).

## License

MIT
