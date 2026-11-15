# Immich Sync Manager

A companion service for [Immich](https://immich.app) **v3.4.1** external libraries. It runs as one
Docker container on a Synology NAS, a Linux host or Docker Desktop on Windows.

Immich-linked writes are enabled. They run under the library write barrier, which was verified end
to end against stock Immich v3.4.1 (`docker/immich-proof/RESULTS.md`). Peer sync publishes and
applies changes through the same managed operations. Read [limitations](docs/limitations.md) before
relying on it, and keep backups.

- **Offline downloads for the Immich local app.** Clients ask for originals *and* their sidecars
  (`.xmp`, `.aae`, …) with resumable downloads, and upload new files into their original folder
  instead of the user's upload library. Thumbnails and previews still come from Immich.
- **Managed file operations.** Moves, renames, folder create/move/delete and trash/restore, sent in
  batches by Open Photo Manager or the local app, with progress streamed back. Asset IDs, albums
  and favorites are preserved, rather than treating moves as deletes and new files.
- **Peer sync between instances.** Several ISM instances (each next to its own Immich) replicate
  chosen folders over a private network such as Netbird: management events right away, file contents
  during allowed hours, conflicts resolved in the UI.

## Maintenance barrier

ISM, not Immich, detects file changes: Immich's file watcher and scheduled library scan stay off
permanently, and ISM watches the mapped folders itself (see [file watching](docs/watching.md)).

1. Before a batch, ISM checks that Immich's watcher and scheduled scan are still off, pauses Immich's
   `library` queue and waits until no library job is running.
2. The batch runs. Every step can be undone; a failing item rolls back and never half-applies.
   Moves update Immich's database directly and need no scan.
3. When the last batch finishes, the queue is resumed. Uploads, peer copies and outside changes get
   a library scan **2 minutes** after the last change.

The Immich API is always preferred. Immich has no API to change the path or owner of an
external-library asset, so those few updates go straight to Immich's PostgreSQL, guarded by row locks
and stale-row checks. Without a database URL ISM stays API-only and refuses those operations. If any
step cannot be verified, ISM stops writing ("fails closed") and shows what to fix.

## Quick start

Save this as `compose.yml` and replace the example paths and user ID:

```yaml
services:
  ism:
    image: ghcr.io/loic294/immich-sync-manager:main
    container_name: immich-sync-manager
    restart: unless-stopped
    user: "1000:1000" # UID:GID that owns the library files
    ports:
      - "8080:8080"
    volumes:
      - /example/photos:/mnt/photos # The same files Immich imports; add other libraries as needed
      - ./ism-data:/data # Persistent database and encryption key
    environment:
      TZ: UTC # Set your time zone
    read_only: true
    tmpfs:
      - /tmp
    cap_drop:
      - ALL
    security_opt:
      - no-new-privileges:true
    stop_grace_period: 30s
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
```

Create `ism-data` and make it writable by the configured user. The library folders must be
readable and writable by that user and accessible to Immich. On Windows, use Windows paths,
a named volume for `/data`, and remove `user:`; see [install](docs/install.md).

```sh
docker compose pull
docker compose up -d
```

Open `http://<host>:8080` and create the administrator immediately: anyone who can reach the
port can create it until setup is complete. Then:

1. **Servers**: add the Immich server and one or more accounts (username/password). Together they
   must own every external library you want managed; ISM routes each change through the owning account.
2. **Mappings**: link each mounted folder (`/mnt/...`) to its Immich library import path and, if used,
   to the Open Photo Manager path prefix.
3. **Tokens**: create a token for each client (Open Photo Manager, Immich local app).
4. **Peers** (optional): pair another instance and choose the shared folders.

For a local build instead, use [`docker/compose.example.yml`](docker/compose.example.yml).
See [install](docs/install.md) for PostgreSQL network access required by path and owner changes.

More: [install](docs/install.md) · [Immich compatibility](docs/immich-compatibility.md) ·
[file watching](docs/watching.md) · [peer sync](docs/peers.md) · [recovery](docs/recovery.md) · [limitations](docs/limitations.md) ·
[client API](docs/client-api.md) · [logs and debugging](docs/logging.md).

## Development

Rust (Axum, SQLx/SQLite) in `crates/`, Lit + DaisyUI web UI in `web/`, the same layout as
Open Photo Manager. `make check` runs every gate; `make clean` removes generated Rust build
artifacts. See [AGENTS.md](AGENTS.md).
Development and test builds disable debug symbols and incremental compilation to limit disk use.
Set `CARGO_PROFILE_DEV_DEBUG=1` or `CARGO_PROFILE_TEST_DEBUG=1` for line-level debugging.

## License

MIT
