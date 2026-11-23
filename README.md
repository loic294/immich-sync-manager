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
3. When the last batch finishes, the queue is resumed. Uploads, peer copies and outside changes use
   file APIs first. Unhandled work is debounced for **2 minutes**, with scans limited to affected
   folders on the supported Immich fork; automatic full-library scans are never a fallback.

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

### Connecting to Immich on separate Docker networks

ISM can join multiple Docker networks. If Immich and its PostgreSQL database are on different
networks, attach the `ism` service to both. Add the following to your Compose file, replacing the
network names with the existing names shown by `docker network ls`:

```yaml
services:
  ism:
    networks:
      - default
      - immich_net
      - immich_db_net

networks:
  immich_net:
    external: true
    name: immich_default
  immich_db_net:
    external: true
    name: immich_db_default
```

These are external networks created by the other Compose projects; they must exist on the same
Docker host as ISM. If Immich and PostgreSQL are already on the same network, attach ISM to that
network once instead. You can check which networks a container uses with
`docker inspect <container> --format '{{json .NetworkSettings.Networks}}'`.

In **Servers**, enter the Immich URL using its container name or network alias and its internal
port, for example `http://immich-server:2283`. For database access, use the PostgreSQL container
name or network alias and internal port, for example
`postgres://<user>:<password>@immich-database:5432/immich`. Use the actual service names and
credentials from your Immich Compose configuration; do not use `localhost` or a host-published
port for container-to-container connections.

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

## Live activity

The overview shows up to ten running or recently completed operations. **Sync** and **Operations**
are the next two menu entries; their tables and open progress dialogs refresh over an authenticated
WebSocket. Reverse proxies must forward WebSocket upgrades for `/api/admin/live`. The existing SSE
endpoints remain available to API clients.

Sync has **Active & queued**, **Sync issues**, and **Completed syncs** tabs. Downloads currently
running and managed publication operations come first, followed by queued files in FIFO order;
offline peers, retry delays and transfer-hour restrictions can defer individual files. Completed
history starts with this version and is retained in ISM's database. Thumbnails come from Immich,
including video thumbnails; unindexed or unavailable previews display a file-type icon. Source-folder and sidecar inspection
requires both peers to run a version supporting activity previews. Configure allowed hours and
temporary overrides under **Hours & overrides**.

Operations show background work, grouped blockers, and live per-action file and Immich steps.
Expand an activity for details and use **Review sync issues** to see available next steps.
**Mark handled** acknowledges an attention item without changing its outcome or releasing a recovery
barrier; **Resolve recovery** remains a separate, explicit action after checking the files. Managed
operations and sync activity default to 250 rows per page; recent background runs and sync issues
default to 25. Available page sizes range from 25 to 5,000.

## Development

Rust (Axum, SQLx/SQLite) in `crates/`, Lit + DaisyUI web UI in `web/`, the same layout as
Open Photo Manager. `make check` runs every gate; `make clean` removes generated Rust build
artifacts. See [AGENTS.md](AGENTS.md).
Development and test builds disable debug symbols and incremental compilation to limit disk use.
Set `CARGO_PROFILE_DEV_DEBUG=1` or `CARGO_PROFILE_TEST_DEBUG=1` for line-level debugging.
Only the SQLx proc-macro symbol table is kept unstripped: Apple's linker can otherwise emit a
`mis-aligned LINKEDIT string pool` dylib that prevents builds from reaching the tests. This does
not enable debug information or require clearing Cargo's cache.

## License

MIT
