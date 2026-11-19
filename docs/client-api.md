# Client API contract (v1)

This is the contract between Immich Sync Manager (ISM) and its clients: Open Photo Manager (OPM),
Immich Local App (ILA) and other ISM instances (peers). The Rust types in
`crates/ism-core/src/dto` are the source of truth; TypeScript copies are generated into
`web/src/api/generated` (`make gen-api`).

All bodies are JSON with `snake_case` fields. Errors always use:

```json
{ "error": { "code": "stable_machine_code", "message": "Human readable, redacted text" } }
```

## Authentication

| Caller | Credential | Header |
|---|---|---|
| Admin web UI | Session cookie `ism_session` (HttpOnly, SameSite=Strict) | `x-ism-request: 1` on every non-GET request |
| OPM / ILA | Client token created in **Settings → Client credentials** | `Authorization: Bearer ism_<id>_<secret>` |
| Peer | Pairing secret, HMAC-signed requests | `x-ism-peer`, `x-ism-timestamp`, `x-ism-signature` |

A client token has a `kind` (`opm`, `ila`), an allow-list of logical roots and an allow-list of
actions (`read`, `download`, `upload`, `manage`, `trash`, `purge`, `transfer_owner`). Tokens are
shown once; only a hash is stored. A token never inherits more than its grants: ISM decides which
Immich account performs each change from the path mapping, never from an owner id supplied by the
client.

## Discovery

`GET /api/v1/client/info` →

```json
{
  "instance_id": "ism_3f2a…",
  "instance_name": "Example NAS",
  "version": "0.1.0",
  "api_version": 1,
  "token_kind": "ila",
  "actions": ["read", "download", "upload", "manage", "trash"],
  "capabilities": ["trash_if_asset_matches", "cancel_receiving_upload", "cancel_operation_by_key", "instance_scoped_operations",
                   "find_operation_by_key", "trash_expectations", "partial_submit",
                   "shared_file_settings", "immich_assets"],
  "roots": [
    {
      "root_id": "photos",
      "name": "Photos",
      "immich_import_path": "/external/photos",
      "opm_prefix": "/volumes/photos",
      "available": true,
      "direct_writes": "enabled",
      "direct_writes_reason": null,
      "direct_writes_detail": null
    }
  ]
}
```

Each root also carries `direct_writes_reason` and `direct_writes_detail` (both `null` unless
blocked). `direct_writes` is one of:

| Value | Meaning |
|---|---|
| `enabled` | The root is inside an Immich external library and managed writes can run now. Moves, renames, folder moves, trash and uploads run under the library write barrier (see [immich-compatibility.md](immich-compatibility.md) and the proof in `docker/immich-proof/RESULTS.md`). |
| `not_linked` | The root is not inside any Immich library. Plain file operations are allowed; there is no Immich side to update. |
| `blocked` | The root is linked, but Immich-linked writes are refused. Downloads keep working. |

`direct_writes_reason` for a blocked root:

| Reason | Cause | Writes fail with |
|---|---|---|
| `not_configured` | Compatibility check/database access missing, or `db_only` selected without direct writes | `direct_writes_unavailable` |
| `unknown_server` | The root points to an Immich server that is no longer registered | `direct_writes_unavailable` |
| `drift` | Immich's watcher or scheduled scan was turned back on | `maintenance_blocked` |
| `maintenance` | The library queue could not be paused or resumed | `maintenance_blocked` |
| `recovery_required` | A change could not be completed or verified; an admin must resolve it | `maintenance_blocked` |

## Logical locations

Every path is a **logical location**: `{ "root_id": "photos", "path": "2024/Trip/IMG_0001.CR3" }`.
`path` is relative, uses `/`, has no `.`/`..`/empty segments, and is case-exact. Clients translate
their own absolute paths using the prefixes from `client/info` (`opm_prefix` for OPM, the local
binding for ILA, `immich_import_path` when starting from an Immich `originalPath`).

`GET /api/v1/locations/resolve?immich_path=/external/photos/2024/a.jpg` → `{ "root_id", "path" }`.

## Browsing folders

`GET /api/v1/folders?root_id=photos&path=2024` →

```json
{ "root_id": "photos", "path": "2024",
  "folders": [{ "name": "Trip", "path": "2024/Trip" }],
  "files": [{ "name": "a.jpg", "path": "2024/a.jpg", "size": 123, "revision": "123-1700000000000000000", "modified_at": "2024-01-01T00:00:00Z" }] }
```

## Manifests (original + sidecars)

- `GET /api/v1/assets/{immich_asset_id}/manifest`
- `GET /api/v1/manifest?root_id=photos&path=2024/a.CR3`

```json
{
  "group_id": "photos:2024/a",
  "asset_id": "uuid-or-null",
  "location": { "root_id": "photos", "path": "2024/a.CR3" },
  "folder": "2024",
  "files": [
    { "role": "original", "name": "a.CR3", "path": "2024/a.CR3", "size": 25000000,
      "revision": "25000000-1700000000000000000", "sha256": "…hex…" },
    { "role": "sidecar", "name": "a.CR3.xmp", "path": "2024/a.CR3.xmp", "size": 4096,
      "revision": "4096-1700000000000000001", "sha256": "…hex…", "immich_linked": true }
  ]
}
```

`role` is `original`, `sidecar` (XMP, `photo.ext.xmp` or `photo.xmp`), or `companion` (other
files sharing the stem whose extension is one of the shared sidecar extensions, e.g. `.pp3`,
`.dop`, `.on1`; see [File settings](#file-settings)). `sha256` is the transfer hash; it is
unrelated to Immich's checksum. Clients must download **every** file of the manifest.

## File settings

`GET /api/v1/client/settings` (capability `shared_file_settings`) returns the "Files and sidecars"
settings the client must use while it is connected:

```json
{
  "files": {
    "sidecar_extensions": ["xmp", "json", "aae", "dop", "pp3", "xml", "on1", "arp", "cos"],
    "junk_names": ["@eaDir", "#recycle", ".DS_Store", "Thumbs.db"],
    "junk_prefixes": ["._"],
    "name_max_len": 255
  },
  "revision": 1700000000000,
  "origin_instance_id": "ism_3f2a…",
  "primary": { "instance_id": "ism_3f2a…", "name": "Example NAS" }
}
```

Extensions are lowercase without the dot; names and prefixes match whole names or name starts,
ignoring case. `name_max_len` is in UTF-8 bytes (16-255). `primary` is `null` when every instance
keeps its own values. Changes are pushed as `settings_changed` on `GET /api/v1/events`.
Administrators edit these in **Settings → Files and sidecars**, on the primary instance when one
is chosen (see [peers.md](peers.md#shared-file-settings)).

## Immich assets

`GET /api/v1/assets?root_id=photos&path=2024&after=…&limit=1000` (capability `immich_assets`,
`read` grant) lists the live (not trashed) Immich assets below a folder of a root, ordered by
path:

```json
{ "root_id": "photos", "linked": true,
  "assets": [ { "asset_id": "uuid", "path": "2024/a.CR3" } ], "next": "2024/a.CR3" }
```

`path` defaults to the root. `limit` is 1-5000 (default 1000). While `next` is not `null`, pass it
as `after` to get the next page. `linked: false` (with no assets) means the root is not inside an
Immich library. `503 immich_unavailable` means the Immich database cannot be read right now.

## Downloading content

`GET /api/v1/content?root_id=photos&path=2024/a.CR3&revision=<revision>`

- Streams the bytes; supports `Range: bytes=…` (single range) and returns `ETag: "<revision>"`.
- When the file changed since the manifest: `412 revision_changed`. Never combine ranges of
  different revisions; restart with a fresh manifest.
- Verify each completed download against the manifest SHA-256 before publishing it. Metadata
  revisions alone cannot detect every content change, including writes that preserve size and mtime.

## Uploads that keep the original folder

1. `POST /api/v1/uploads`
   ```json
   { "idempotency_key": "client-generated-unique-string",
     "target": { "root_id": "photos", "path": "2024/Trip" },
     "provenance": { "source": "ila", "original_location": { "root_id": "photos", "path": "2024/Trip/a.jpg" }, "expected_revision": "123-1700…" },
     "files": [ { "name": "a.jpg", "role": "original", "size": 123, "sha256": "…" },
                { "name": "a.jpg.xmp", "role": "sidecar", "size": 10, "sha256": "…" } ],
     "on_conflict": "fail" }
   ```
   → `201 { "upload_id": "…", "status": "receiving", "files": [{ "index": 0, "received": 0 }, …] }`
   (`status`: `receiving` | `publishing` | `committed` | `failed` | `cancelled`; `publishing` means the commit was
   accepted and the managed operation that moves the files into the library is running).
   Persist the optional `idempotency_key` before sending the request. Retrying the identical request
   with the same credential and key returns the existing upload and its current status; using that
   key for a different request returns `idempotency_key_conflict`. A failed, cancelled or expired upload keeps
   its key, so starting a new upload requires a new key. Without a key, each request creates an upload.
   With `replace_if_expected`, each existing destination file must include its own
   `expected_revision` and `expected_sha256` from the manifest. For the original,
   `provenance.expected_revision` remains supported. Omit both fields only for a file that
   must not already exist.
   ISM rejects changed, removed, or unexpectedly existing files rather than replacing them.
   `on_conflict`: `fail` | `rename` | `replace_if_expected`.
2. `PUT /api/v1/uploads/{upload_id}/files/{index}` with `Content-Range: bytes start-end/total`
   (or the whole body). Resumable: `GET /api/v1/uploads/{upload_id}` returns `received` per file.
   Status reads wait for in-flight chunk writes and expiry checks before reporting offsets.
   If a receiving upload's staging file is missing, status, chunk writes and commit return
   `upload_staging_missing` instead of recreating it; start a new upload.
3. `POST /api/v1/uploads/{upload_id}/commit` → `202 { "operation_id": "…", "status": "queued" }`. ISM verifies sizes
   and hashes, then publishes into the mapped external-library folder through a managed
   operation, never into an Immich upload library. When `provenance.original_location` is set,
   `target` must be that file's folder (`upload_target_mismatch` otherwise). ISM records its own
   writes so the watcher does not report them, and schedules a scan of the library about 2 minutes
   later. The operation result's `items[].result.asset_id` is `null` until Immich imports the
   file; when that scan finishes, ISM looks the asset up by path (as the library owner) and fills
   in `asset_id`, announcing the updated item on the operation's event stream
   (`GET /api/v1/operations/{id}/events`) and in `GET /api/v1/operations/{id}`. Clients no longer
   need to poll the manifest for the new asset id.

### Sidecar-only uploads

To sync an edited sidecar without re-uploading an unchanged original, name the original that
stays in place instead of sending it:

```json
{ "idempotency_key": "…",
  "target": { "root_id": "photos", "path": "2024/Trip" },
  "existing_original": { "name": "a.jpg", "expected_revision": "123-1700…" },
  "files": [ { "name": "a.jpg.xmp", "role": "sidecar", "size": 10, "sha256": "…",
               "expected_revision": "11-1700…", "expected_sha256": "…" } ],
  "on_conflict": "replace_if_expected" }
```

- `files` holds only `sidecar`/`companion` roles, each a sidecar or companion of the named
  original (`not_a_companion` otherwise); `on_conflict` must be `replace_if_expected`.
- The original must still have `expected_revision`, else the operation fails with
  `revision_changed` and nothing changes.
- Each sidecar being replaced needs its `expected_revision`/`expected_sha256`; omit them for a
  sidecar that must not exist yet (an existing file then fails with `revision_changed`).
- Only the replaced sidecars go to the ISM trash; the original is never copied.
- ISM schedules a library scan so Immich re-reads the sidecar; `asset_id` is filled as above.

`DELETE /api/v1/uploads/{upload_id}` cancels a receiving upload and removes its staged files.
It is safe to retry after an interrupted request or a cleanup error. Once publishing has started,
cancellation is refused; the operation and its recovery state must be handled separately. Check
the `cancel_receiving_upload` capability before using this endpoint.

## Managed operations (moves, renames, folders, trash)

`POST /api/v1/operations/preview` and `POST /api/v1/operations` take the same body:

```json
{
  "idempotency_key": "client-generated-unique-string",
  "label": "Move 3 files",
  "actions": [
    { "kind": "move", "source": { "root_id": "photos", "path": "2024/a.CR3" },
      "target": { "root_id": "photos", "path": "2024/Trip/a.CR3" }, "expected_revision": "…" },
    { "kind": "rename", "source": { "root_id": "photos", "path": "2024/b.jpg" },
      "target": { "root_id": "photos", "path": "2024/b-renamed.jpg" } },
    { "kind": "create_folder", "target": { "root_id": "photos", "path": "2024/New" } },
    { "kind": "move_folder", "source": { "root_id": "photos", "path": "2024/Old" },
      "target": { "root_id": "archive", "path": "2024/Old" } },
    { "kind": "trash_if_asset_matches", "source": { "root_id": "photos", "path": "2024/c.jpg" },
      "expected_asset_id": "immich-asset-id", "expected_revision": "…" },
    { "kind": "trash", "source": { "root_id": "photos", "path": "2024/d.jpg" }, "expected_revision": "…" },
    { "kind": "trash_folder", "source": { "root_id": "photos", "path": "2023/Old" },
      "expected_entries": [ { "path": "e.jpg", "revision": "…" }, { "path": "sub/f.jpg", "revision": "…" } ] },
    { "kind": "restore", "trash_id": "…" },
    { "kind": "purge", "trash_id": "…" }
  ],
  "allow_owner_transfer": false,
  "allow_partial": false
}
```

- Moving or renaming a file always moves its whole sidecar/companion group; clients send only the
  original. Targets name the new path of the original; companions follow the same stem.
- ILA file trash actions must use `trash_if_asset_matches` with `expected_asset_id`; ISM verifies
  that exactly that asset occupies the path when planning and again before execution.
  Clients must require the `trash_if_asset_matches` capability; older servers reject this distinct
  action rather than ignoring an unknown field. Legacy retries with a previously submitted request lacking
  the ID conflict rather than silently changing the existing request. This check does not exclude
  concurrent external writers; access to managed roots must still be coordinated.
- `trash` and `trash_if_asset_matches` accept an optional `expected_revision` (from a listing or manifest). If the file changed,
  the item fails with `revision_changed`.
- `trash_folder` accepts an optional `expected_entries`: every file below the folder, as a path
  relative to it (`/` separators) and its revision. If any file was added, changed or removed, the
  item fails with `revision_changed` and nothing is touched. Without it, the folder is trashed as
  it is. Both checks run when planning and again when the item executes. Servers with the
  `trash_expectations` capability support them.
- By default, a request with any blocker is refused as a whole (`409` with the first blocker's
  code) and nothing is queued. With `"allow_partial": true` (capability `partial_submit`), the
  operation is queued anyway: blocked items are recorded as `failed` with their blocker's code and
  are never run, and the other items run as usual. The operation then ends `partial` (or `failed`
  if nothing could run).
- `allow_owner_transfer` must be `true` (and the token must hold `transfer_owner`) when a move
  crosses into a library owned by another Immich user.
- Retrying the identical request with the same credential and `idempotency_key` returns the existing
  operation instead of running twice. Reusing the key for a different request returns
  `idempotency_key_conflict`.

Preview response:

```json
{ "ok": true,
  "items": [ { "index": 0, "kind": "move", "files": [ { "from": {…}, "to": {…}, "role": "original" } ],
               "assets": [ { "asset_id": "…", "library_id": "…", "owner": "account label", "owner_change": false } ],
               "blockers": [], "warnings": [] } ],
  "blockers": [] }
```

Submit → `202 { "operation_id": "op_…", "status": "queued" }`.

`GET /api/v1/operations/{id}` →

```json
{ "operation_id": "op_…", "status": "queued|waiting_maintenance|running|completed|partial|failed|cancelled|recovery_required",
  "label": "…", "source": "ila", "created_at": "…", "finished_at": null,
  "progress": { "done": 2, "total": 5 },
  "items": [ { "index": 0, "kind": "move", "status": "pending|running|done|skipped|failed|recovery_required",
               "error": null, "result": { "files": […], "asset_id": "…" } } ] }
```

`POST /api/v1/operations/{id}/cancel` stops before the next item; started items finish safely.

`GET /api/v1/operations?idempotency_key=…` (capability `find_operation_by_key`) returns the
operation this credential submitted under that key, with the same JSON as above. It has no side
effects: unlike `cancel-by-key`, it never reserves the key. Without an operation, it returns
`404` with code `not_found`, or `cancelled_before_submission` if the key was cancelled first. The
instance-scoped form is `GET /api/v1/instances/{instance_id}/operations?idempotency_key=…`.

For uncertain submissions, clients with `cancel_operation_by_key` may send
`POST /api/v1/operations/cancel-by-key` with
`{"idempotency_key":"…","expected_request":{…}}`, where `expected_request` is the complete
original operation request. If no operation has been inserted, the server durably reserves the
key and replies `{"status":"cancelled_before_submission"}`; a later submission under that
credential and key fails with `operation_key_cancelled`. If an operation exists, a mismatched
request is refused, otherwise the response is `{"status":"operation","operation":{…}}`.
Only a still-queued operation is cancelled by this endpoint. For running or terminal operations,
the response reports its actual state; cancellation is not implied. Even a `cancelled` batch may
have completed earlier items, so inspect every item before treating it as having made no changes.
Key reservations are scoped to the credential and do not expire.

Clients that require server identity fencing must check `instance_scoped_operations` and use
`/api/v1/instances/{instance_id}/operations` for submission,
`/api/v1/instances/{instance_id}/operations/{id}` for status, and
`/api/v1/instances/{instance_id}/operations/cancel-by-key` for cancellation. A different
instance rejects these requests with `instance_mismatch`. Persist the instance identity and
credential association before submission; a different or unverified credential cannot prove that
an earlier submission was cancelled.

### Progress stream

`GET /api/v1/operations/{id}/events` is Server-Sent Events. Each event has `id: <seq>` and
`event: progress|item|finished`. `progress` and `finished` carry the full operation (same JSON as
`GET /operations/{id}`); `item` carries one `items[]` entry (it has `index`, no `operation_id`). Reconnect with `Last-Event-ID`; when the cursor is too old the server sends
`event: resync` and the client must refetch `GET /operations/{id}`. The stream ends after
`finished`, except while a published upload still waits for its `asset_id`: it then stays open
until the `item` carrying the asset id is sent (at most about 30 minutes after the scan delay;
afterwards read `GET /operations/{id}`).

`GET /api/v1/events` (SSE) streams `operation_committed` events for all operations visible to the
token so OPM can refresh its index:

```json
{ "operation_id": "op_…", "changes": [ { "kind": "moved", "from": {…}, "to": {…} },
  { "kind": "trashed", "location": {…}, "trash_id": "…" }, { "kind": "created", "location": {…} } ] }
```

The same stream also sends:
- `settings_changed`: the new [file settings](#file-settings) (same JSON as
  `GET /api/v1/client/settings`);
- `assets_changed`: `{ "root_ids": ["photos"] }` after an Immich library scan finished for those
  roots, so the client can refetch [`/assets`](#immich-assets);
- `resync` (`{}`) when events were dropped: refetch everything.

## Trash

`GET /api/v1/trash?root_id=photos` → `[ { "trash_id", "location": {…}, "is_folder", "size",
"trashed_at", "operation_id", "asset_ids": [] } ]` (newest first) lists recoverable items; restore and purge are operation
actions (`restore`, `purge`). Purge requires the `purge` action grant. Nothing is purged
automatically.

## Error codes clients must handle

| Code | Meaning |
|---|---|
| `unauthorized`, `forbidden` | Missing/invalid token, or the token lacks the root/action |
| `invalid_location` | Path escapes the root, has bad segments, or root unknown |
| `root_unavailable` | Volume not mounted/unreadable (never treated as empty) |
| `revision_changed` | Source changed since the client last saw it |
| `asset_identity_required`, `asset_identity_changed`, `asset_identity_unverified` | The asset at a trash location is missing, changed, or cannot be verified |
| `destination_exists` | Target already exists |
| `direct_writes_unavailable` | Immich compatibility gate not passed or disabled for that root |
| `owner_transfer_required` / `owner_transfer_blocked` | Cross-owner move needs approval / is unsafe |
| `immich_unavailable` | The Immich database cannot be read (`/assets`); retry later |
| `maintenance_blocked` | Immich's library queue could not be paused and confirmed idle, or its watcher/scheduled scan was turned back on; nothing was changed |
