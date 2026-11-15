# Immich v3.4.1 proof results

Run of `docker/immich-proof/run.sh` on 2026-11-14 against stock `immich-server:v3.4.1`, the pinned
Immich Postgres image and Valkey (no machine learning). Synthetic data only. Exit code 0.

## Observations worth knowing

- **Held jobs show as `waiting`, never `paused`.** While `library` is paused (`isPaused: true`),
  queued scan jobs are counted in `statistics.waiting`; `statistics.paused` stayed 0 throughout.
  `Queue::is_idle` in `api.rs` checks all four counters, so it is correct either way.
- **One scan request = two jobs**: `LibrarySyncFilesQueueAll` and `LibraryScanAssetsQueueAll`.
  Repeated scan clicks are not deduplicated (two clicks while paused = 4 held jobs); each runs on
  resume, harmlessly.
- **A UI scan clicked during maintenance blocks ISM's post-pause check** (`waiting > 0` ⇒
  "Immich queued new work while pausing"). That is fail-closed and correct; the held job runs after
  resume and imports each new file exactly once.
- `PUT /api/queues/{name}` returns 200 with the queue object; queue entries also carry
  `completed` and `failed` counters (ignored by ISM).
- External-library assets use `checksumAlgorithm = sha1-path`, so the `pg.rs` path-checksum branch
  is the one exercised; Immich accepted `sha1("path:" + newPath)` and the next scan created no
  duplicate. The guarded `UPDATE` bumps `updatedAt`/`updateId` via Immich's triggers, so mobile sync
  sees the new path and file name.
- Sidecars are `asset_file` rows with `type = 'sidecar'`, as `pg.rs` assumes.
- **Trash semantics:** a scan does **not** mark a trashed asset offline when its file is missing
  (it ignores trashed assets). Moving the file back, `POST /api/trash/restore/assets` and a scan
  give back the same asset id, active and online.
- An unmanaged move (no DB update) makes the old asset `isOffline = true` with `deletedAt` set
  while `status` stays `active`, and imports a new asset id: album, favorite and id are lost.
- `GET /api/albums/{id}` in v3.4.1 has `assetCount` but no `assets` array (not used by ISM).
- On macOS Docker Desktop, bind mounts may not deliver inotify events, so check D proves that the
  disabled scheduled scan does not run (cron set to every minute while `enabled: false`); it does
  not independently prove that Immich's watcher honours `watch.enabled: false`.

## Output

```text
setup: admin, API key (14 permissions), library 622d4ee6, config library={"scan": {"enabled": false, "cronExpression": "* * * * *"}, "watch": {"enabled": false}}
initial scan imported 4 assets: ['/mnt/ext/2026/keep/k.png', '/mnt/ext/2026/trip/a.png', '/mnt/ext/2026/trip/b.png', '/mnt/ext/2026/trip/e.png']
[PASS] A. ISM endpoints and response shapes on v3.4.1
       GET /api/server/version -> {"major": 3, "minor": 4, "patch": 1, "prerelease": null, "featureLevel": 2} (=> 3.4.1)
       latest kysely_migrations.name = 1792568637987-AddAssetOwnerIdUpdateIdIndex
       POST /api/api-keys with ADMIN_PERMISSIONS -> 201, secret present=True, apiKey.id present=True; login fields accessToken/userId/isAdmin = True
       POST /api/auth/logout -> 200
       GET /api/admin/config (api key): library.watch.enabled=False, library.scan.enabled=False
       PUT /api/admin/config (api key, unchanged document) -> 200
       GET /api/queues -> list of 19; shape ok=True; all interfering queues present=True
         library entry: {"name": "library", "isPaused": false, "statistics": {"active": 0, "completed": 0, "failed": 0, "delayed": 0, "waiting": 0, "paused": 0}}
       PUT /api/queues/library {isPaused:true} -> 200 body={"name": "library", "isPaused": true, "statistics": {"active": 0, "completed": 0, "failed": 0, "delayed": 0, "waiting": 0, "paused": 0}}
         after pause: isPaused=True active=0 waiting=0 delayed=0 paused=0; after resume: isPaused=False active=0 waiting=0 delayed=0 paused=0
       GET /api/libraries -> 1 libraries, fields ['assetCount', 'createdAt', 'exclusionPatterns', 'id', 'importPaths', 'name', 'ownerId', 'refreshedAt', 'updatedAt']
         assetCount present=True (api.rs treats it as optional)
       PUT /api/libraries/{id} {exclusionPatterns} -> 200
       POST /api/libraries/{id}/scan {} -> 204
       GET /api/assets/{id} -> 200; fields include isOffline=True, isTrashed=True, originalPath=True
       DELETE /api/assets and POST /api/trash/restore/assets are exercised in check E
[PASS] B. Scan requested while the library queue is paused is held, runs once on resume
       paused library queue; added /mnt/ext/2026/new/n.png; POST scan -> 204
       over 60s: new file imported=False; queue samples:
         isPaused=True active=0 waiting=2 delayed=0 paused=0
       held BullMQ jobs in immich_bull:library:wait: ['LibraryScanAssetsQueueAll', 'LibrarySyncFilesQueueAll']
       held job count (waiting+paused+delayed) = 2; ISM is_idle() = False
       after resume: idle=True; assets at /mnt/ext/2026/new/n.png: 1; library count 4 -> 5
[PASS] C. Managed move (pause, disk move, guarded SQL, resume, scan) keeps identity
       asset b5bed374 at /mnt/ext/2026/trip/a.png, album d6f30ec8, favorite set; checksumAlgorithm=sha1-path; sidecar rows=['/mnt/ext/2026/trip/a.png.xmp']
       paused + quiet=True; moved file and sidecar on disk to /mnt/ext/2026/trip-renamed/a-renamed.png(.xmp)
       guarded transaction output: COMMITTED
       asset updatedAt/updateId bumped by the UPDATE (Immich triggers, so sync clients see it): updatedAt=True updateId=True
       after resume + scan (idle=True): id=b5bed374 path=/mnt/ext/2026/trip-renamed/a-renamed.png offline=False trashed=False status=active favorite=True
       originalFileName=a-renamed.png; checksumAlgorithm=sha1-path; checksum = sha1(path:new)
       in album=True; assets at new path=1; assets at old path=0; library count 5 -> 5
       sidecar rows now=['/mnt/ext/2026/trip-renamed/a-renamed.png.xmp']
[PASS] C-control. Unmanaged move (no DB update) loses identity: old offline, new asset id
       old asset: id=5c4bc3ae path=/mnt/ext/2026/trip/b.png offline=True trashed=True status=active favorite=False
       new path: ['id=600f1821 path=/mnt/ext/2026/elsewhere/b.png offline=False trashed=False status=active favorite=False']
[PASS] D. Watch and scheduled scan off: no automatic import; manual scan imports once
       config library={"scan": {"enabled": false, "cronExpression": "* * * * *"}, "watch": {"enabled": false}}
       added /mnt/ext/2026/new/d.png; after 183s imported automatically=False
       library queue states seen: ['isPaused=False active=0 waiting=0 delayed=0 paused=0']
       manual API scan -> assets at path: 1; library count 6 -> 7
[PASS] E. ISM trash: API trash + file out of import path, then back + API restore keeps id
       DELETE /api/assets {force:false} -> 204: id=96b3ddfe path=/mnt/ext/2026/trip/e.png offline=False trashed=True status=trashed favorite=False
       file moved to ISM trash (/mnt/ism-trash/e.png) + scan: id=96b3ddfe path=/mnt/ext/2026/trip/e.png offline=False trashed=True status=trashed favorite=False
         GET /api/assets/{id} -> isTrashed=True isOffline=False
       file moved back + POST /api/trash/restore/assets -> 200 {"count": 1}: id=96b3ddfe path=/mnt/ext/2026/trip/e.png offline=False trashed=False status=active favorite=False
       after scan: id=96b3ddfe path=/mnt/ext/2026/trip/e.png offline=False trashed=False status=active favorite=False; assets at path=1
[PASS] F. A UI scan while paused is held and visible in queue statistics
       admin session (as the web UI) clicked Scan twice -> 204, 204; library: isPaused=True active=0 waiting=4 delayed=0 paused=0
       held jobs: ['LibraryScanAssetsQueueAll', 'LibrarySyncFilesQueueAll', 'LibraryScanAssetsQueueAll', 'LibrarySyncFilesQueueAll']
       held count=4; ISM is_idle()=False (so ISM's post-pause check fails closed with 'Immich queued new work while pausing')
       25s later file imported=False
       after resume: idle=True; assets at path=1; library count 7 -> 8

SUMMARY: 7/7 PASS
```
