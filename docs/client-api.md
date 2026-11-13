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
  "roots": [
    {
      "root_id": "photos",
      "name": "Photos",
      "immich_import_path": "/external/photos",
      "opm_prefix": "/volumes/photos",
      "available": true,
      "direct_writes": "enabled"
    }
  ]
}
```

`direct_writes` is `enabled`, `disabled` (compatibility gate not passed or turned off) or
`blocked` (maintenance/recovery). When not `enabled`, `manage` operations on that root are
rejected with `409 direct_writes_unavailable`; downloads keep working.

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
editor files sharing the stem, e.g. `.pp3`, `.dop`, `.on1`). `sha256` is the transfer hash; it is
unrelated to Immich's checksum. Clients must download **every** file of the manifest.

## Downloading content

`GET /api/v1/content?root_id=photos&path=2024/a.CR3&revision=<revision>`

- Streams the bytes; supports `Range: bytes=…` (single range) and returns `ETag: "<revision>"`.
- When the file changed since the manifest: `412 revision_changed`. Never combine ranges of
  different revisions; restart with a fresh manifest.
- Response header `x-ism-sha256` is set when the hash is already known.

## Uploads that keep the original folder

1. `POST /api/v1/uploads`
   ```json
   { "target": { "root_id": "photos", "path": "2024/Trip" },
     "provenance": { "source": "ila", "original_location": { "root_id": "photos", "path": "2024/Trip/a.jpg" }, "expected_revision": "123-1700…" },
     "files": [ { "name": "a.jpg", "role": "original", "size": 123, "sha256": "…" },
                { "name": "a.jpg.xmp", "role": "sidecar", "size": 10, "sha256": "…" } ],
     "on_conflict": "fail" }
   ```
   → `201 { "upload_id": "…", "status": "receiving", "files": [{ "index": 0, "received": 0 }, …] }`
   (`status`: `receiving` | `publishing` | `committed` | `failed`; `publishing` means the commit was
   accepted and the managed operation that moves the files into the library is running).
   `provenance.expected_revision` is set when replacing an existing file; ISM rejects the commit
   with a conflict if the server copy changed meanwhile. `on_conflict`: `fail` | `rename` | `replace_if_expected`.
2. `PUT /api/v1/uploads/{upload_id}/files/{index}` with `Content-Range: bytes start-end/total`
   (or the whole body). Resumable: `GET /api/v1/uploads/{upload_id}` returns `received` per file.
3. `POST /api/v1/uploads/{upload_id}/commit` → `202 { "operation_id": "…", "status": "queued" }`. ISM verifies sizes
   and hashes, then publishes into the mapped external-library folder through a managed
   operation. The new asset appears in Immich after the post-maintenance library scan;
   `GET /api/v1/operations/{id}` reports `items[].asset_id` once known, `null` before.

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
    { "kind": "trash", "source": { "root_id": "photos", "path": "2024/c.jpg" } },
    { "kind": "trash_folder", "source": { "root_id": "photos", "path": "2023/Empty" } },
    { "kind": "restore", "trash_id": "…" },
    { "kind": "purge", "trash_id": "…" }
  ],
  "allow_owner_transfer": false
}
```

- Moving or renaming a file always moves its whole sidecar/companion group; clients send only the
  original. Targets name the new path of the original; companions follow the same stem.
- `allow_owner_transfer` must be `true` (and the token must hold `transfer_owner`) when a move
  crosses into a library owned by another Immich user.
- The same `idempotency_key` returns the existing operation instead of running twice.

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

### Progress stream

`GET /api/v1/operations/{id}/events` is Server-Sent Events. Each event has `id: <seq>` and
`event: progress|item|finished`. `progress` and `finished` carry the full operation (same JSON as
`GET /operations/{id}`); `item` carries one `items[]` entry (it has `index`, no `operation_id`). Reconnect with `Last-Event-ID`; when the cursor is too old the server sends
`event: resync` and the client must refetch `GET /operations/{id}`.

`GET /api/v1/events` (SSE) streams `operation_committed` events for all operations visible to the
token so OPM can refresh its index:

```json
{ "operation_id": "op_…", "changes": [ { "kind": "moved", "from": {…}, "to": {…} },
  { "kind": "trashed", "location": {…}, "trash_id": "…" }, { "kind": "created", "location": {…} } ] }
```

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
| `destination_exists` | Target already exists |
| `direct_writes_unavailable` | Immich compatibility gate not passed or disabled for that root |
| `owner_transfer_required` / `owner_transfer_blocked` | Cross-owner move needs approval / is unsafe |
| `maintenance_blocked` | Immich watcher/queues could not be quiesced; nothing was changed |
