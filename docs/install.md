# Installing Immich Sync Manager

ISM is one container. It needs:

- the **same library folders Immich imports**, mounted under `/mnt` (read-write);
- a writable **data folder** at `/data` (SQLite database and the key that encrypts stored passwords);
- network access to the **Immich server** (HTTP API) and, for path/owner changes, to **Immich's
  PostgreSQL** database;
- for peer sync, a private network between instances (for example Netbird).

## Synology NAS (Container Manager)

1. Copy the repository (or just `docker/`) to the NAS, e.g. `/volume1/docker/ism`.
2. Copy `docker/compose.example.yml` to `docker/compose.yml` and edit it:
   - `user:` the uid:gid that owns the photo folders (`id <dsm-user>` over SSH). Immich must be able to
     read what ISM writes, so match the owner of the library folders.
   - `volumes:` one line per library folder, `/volume1/<share>:/mnt/<name>`.
   - `TZ:` your time zone.
3. Create the data folder and give it to that user: `sudo mkdir -p ism-data && sudo chown 1026:100 ism-data`
   (with your numbers).
4. Container Manager → Project → Create, pick the folder, or `docker compose up -d --build` over SSH.
   Building on the NAS is slow; use the prebuilt image below, or build on a PC (`make image`) and
   transfer it with `docker save | docker load`.

## Prebuilt Docker image

Every push or merge to `main` builds native Linux AMD64 and ARM64 images and publishes a combined
image to GitHub Container Registry:

```text
ghcr.io/loic294/immich-sync-manager:main
```

`latest` tracks the same build. Use `sha-<full Git commit SHA>` or a digest to pin an exact build.
Publication only updates these tags after both platform builds succeed. The **Docker image**
workflow in the repository's Actions tab contains the build logs and can also be started manually
on `main`. It uses the repository's built-in `GITHUB_TOKEN`; no registry password secret is needed.
Confirm the GHCR package allows anonymous pulls before installing on another host. If its first
publication is private, the package owner can change visibility in the package's settings.

To use it, replace the example service's `image:` with the address above and remove its `build:`
block. Then run `docker compose pull && docker compose up -d` from the directory containing your
Compose file. Windows Docker Desktop must use Linux containers.

## Linux

Same compose file. Use the host user that owns the library (`id -u`, `id -g`).

## Windows (Docker Desktop)

Mount library folders as `C:\Photos:/mnt/photos`, use a named volume for `/data` and remove the `user:`
line. Note that Immich must see the same files: run Immich on the same machine with the same folders.

## Reaching Immich's database

Path and owner changes need Immich's PostgreSQL. The simplest way is to join Immich's compose network
(uncomment the `networks:` block) and use, in **Servers → Edit**:

```
postgres://<user>:<password>@database:5432/immich
```

The URL is encrypted at rest. Leave it empty to stay API-only: downloads, uploads and trash keep
working, but moves and renames of existing assets are refused.

## First run

1. Open `http://<host>:8080` and create the administrator (password: 10 characters or more). Until
   this is done, anyone who can reach the port can create it, so do it right after the first start
   or publish the port on a private interface only.
2. **Servers**: add Immich's URL, then accounts with username/password. ISM warns when a library is not
   owned by any added account.
3. **Mappings**: under **Mounted folders**, use **Browse** to navigate to a library folder at any
   depth, for example `/mnt/homes/example-user/Photos`. Each level lists only its immediate
   subfolders; **Up** and the path buttons return to earlier levels. Select **Map** beside a folder
   or **Map this folder** for the current folder, then pick the Immich library and its import path.
   Give it a short root ID (e.g. `photos`): clients see paths as `photos:2024/Trip/a.jpg`. Add the Open
   Photo Manager path prefix if OPM manages the same folder. Hidden folders and symlinks are not
   listed, and browsing stays inside `ISM_MOUNTS_DIR` (`/mnt` by default).
   When OPM manages a parent folder and Immich imports its subfolders, create an **OPM-only**
   mapping for the parent and **Immich-only** mappings for the subfolders. Separate root IDs also
   let these sources map the exact same folder independently: **Map** remains available on mapped
   folders. Leave the other source's fields empty. Overlaps within the same source (or involving
   files-only mappings) are rejected.
   Use the Immich root ID for managed changes inside an imported folder. Writes through an
   independent OPM mapping that would affect that folder are refused, rather than bypassing
   Immich's maintenance and asset bookkeeping; browsing through either mapping remains available.
4. **Tokens**: create one token per client and paste it into the client's settings.

## Updating

`docker compose up -d --build`. The database migrates on start. Do not update while **Overview** shows
running operations or a maintenance window.
For a prebuilt image, use `docker compose pull && docker compose up -d` instead. Export recent
[runtime logs](logging.md) first if investigating a problem: recreating a container discards its
Docker log history.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `ISM_LISTEN` | `0.0.0.0:8080` | Listen address. |
| `ISM_DATA_DIR` | `/data` | Database and key. |
| `ISM_MOUNTS_DIR` | `/mnt` | Folders offered on the Mappings page. |
| `ISM_WEB_DIR` | `/app/web` | Built web UI. |
| `ISM_SECURE_COOKIES` | unset | `true` when served over HTTPS. |
| `ISM_LOG` | `info` | Log filter (`debug`, `ism_server=debug`, …). |
| `ISM_WORKER_TICK_SECS` | `5` | Background worker interval. |
| `ISM_SCAN_INTERVAL_SECS` | `300` | Full safety rescan of peer shares; watcher changes trigger earlier rescans. |
| `ISM_VERIFY_INTERVAL_SECS` | `86400` | Re-hash of shared files whose size and date did not change, to catch edits that kept both. |
| `ISM_PEER_TRANSFER_CONCURRENCY` | `3` | Concurrent peer file downloads (1–32); raise gradually on high-latency links. |
| `ISM_QUIESCE_TIMEOUT_SECS` | `600` | Longest wait for Immich's paused `library` queue to have no running job. |
| `ISM_SCAN_TIMEOUT_SECS` | `7200` | Longest wait for a library scan to finish. |
| `ISM_SCAN_DELAY_SECS` | `120` | Library scans run this long after the last detected change. |

## Debugging

Use **Audit**, **Operations** and **Sync** for persistent history, and Docker/Container Manager logs
for runtime diagnostics. See [logs and debugging](logging.md) for log commands, retention,
debug-level configuration and safe diagnostic exports.
