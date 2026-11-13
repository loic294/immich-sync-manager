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
   Building on the NAS is slow; building the image on a PC (`make image`) and loading it with
   `docker save | docker load` is faster.

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

1. Open `http://<host>:8080` and create the administrator (password: 10 characters or more).
2. **Servers**: add Immich's URL, then accounts with username/password. ISM warns when a library is not
   owned by any added account.
3. **Mappings**: for each `/mnt/<name>`, pick the Immich library and its import path. Give it a short
   root ID (e.g. `photos`): clients see paths as `photos:2024/Trip/a.jpg`. Add the Open Photo Manager
   path prefix if OPM manages the same folder.
4. **Tokens**: create one token per client and paste it into the client's settings.

## Updating

`docker compose up -d --build`. The database migrates on start. Do not update while **Overview** shows
running operations or a maintenance window.

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
| `ISM_SCAN_INTERVAL_SECS` | `300` | How often shared folders are checked for changes. |
| `ISM_QUIESCE_TIMEOUT_SECS` | `600` | Longest wait for Immich queues to drain before changes. |
| `ISM_SCAN_TIMEOUT_SECS` | `7200` | Longest wait for the reconciliation scan. |
