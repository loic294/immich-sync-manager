-- Immich Sync Manager state. All timestamps are RFC 3339 UTC strings.

CREATE TABLE settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE admin_users (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE sessions (
    token_hash TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES admin_users(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE TABLE immich_servers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    database_url_enc TEXT,
    direct_writes_enabled INTEGER NOT NULL DEFAULT 0,
    version TEXT,
    compatible INTEGER NOT NULL DEFAULT 0,
    compatibility_detail TEXT,
    maintenance_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE immich_accounts (
    id TEXT PRIMARY KEY,
    server_id TEXT NOT NULL REFERENCES immich_servers(id) ON DELETE CASCADE,
    label TEXT NOT NULL,
    email TEXT NOT NULL,
    immich_user_id TEXT NOT NULL,
    is_admin INTEGER NOT NULL,
    api_key_enc TEXT NOT NULL,
    api_key_id TEXT,
    status TEXT NOT NULL,
    last_error TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (server_id, immich_user_id)
);

CREATE TABLE immich_libraries (
    id TEXT PRIMARY KEY,
    server_id TEXT NOT NULL REFERENCES immich_servers(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    owner_user_id TEXT NOT NULL,
    import_paths_json TEXT NOT NULL,
    exclusion_patterns_json TEXT NOT NULL DEFAULT '[]',
    asset_count INTEGER,
    refreshed_at TEXT NOT NULL
);

CREATE TABLE mappings (
    root_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    container_path TEXT NOT NULL,
    server_id TEXT REFERENCES immich_servers(id) ON DELETE SET NULL,
    library_id TEXT,
    immich_import_path TEXT,
    opm_prefix TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE client_tokens (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    secret_hash TEXT NOT NULL,
    roots_json TEXT NOT NULL,
    actions_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_used_at TEXT,
    revoked_at TEXT
);

CREATE TABLE operations (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    principal TEXT NOT NULL,
    source TEXT NOT NULL,
    label TEXT,
    request_json TEXT NOT NULL,
    status TEXT NOT NULL,
    server_ids_json TEXT NOT NULL DEFAULT '[]',
    library_ids_json TEXT NOT NULL DEFAULT '[]',
    admitted_server_ids_json TEXT NOT NULL DEFAULT '[]',
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    recovery_resolved INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    UNIQUE (principal, idempotency_key)
);
CREATE INDEX operations_status ON operations(status);

CREATE TABLE operation_items (
    operation_id TEXT NOT NULL REFERENCES operations(id) ON DELETE CASCADE,
    idx INTEGER NOT NULL,
    kind TEXT NOT NULL,
    action_json TEXT NOT NULL,
    status TEXT NOT NULL,
    error_code TEXT,
    error_message TEXT,
    result_json TEXT,
    journal_json TEXT,
    PRIMARY KEY (operation_id, idx)
);

CREATE TABLE operation_events (
    operation_id TEXT NOT NULL REFERENCES operations(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    event TEXT NOT NULL,
    data TEXT NOT NULL,
    at TEXT NOT NULL,
    PRIMARY KEY (operation_id, seq)
);

CREATE TABLE trash (
    id TEXT PRIMARY KEY,
    root_id TEXT NOT NULL,
    path TEXT NOT NULL,
    quarantine_rel TEXT NOT NULL,
    is_folder INTEGER NOT NULL,
    size INTEGER NOT NULL,
    trashed_at TEXT NOT NULL,
    operation_id TEXT,
    asset_ids_json TEXT NOT NULL DEFAULT '[]',
    server_id TEXT,
    restored_at TEXT,
    purged_at TEXT
);

CREATE TABLE uploads (
    id TEXT PRIMARY KEY,
    principal TEXT NOT NULL,
    request_json TEXT NOT NULL,
    status TEXT NOT NULL,
    operation_id TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE file_hashes (
    root_id TEXT NOT NULL,
    path TEXT NOT NULL,
    size INTEGER NOT NULL,
    mtime_ns INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    PRIMARY KEY (root_id, path)
);

-- Replication: every change this instance knows about, local or applied from a peer.
CREATE TABLE event_log (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    origin TEXT NOT NULL,
    origin_seq INTEGER NOT NULL,
    share_name TEXT NOT NULL,
    kind TEXT NOT NULL,
    data_json TEXT NOT NULL,
    at TEXT NOT NULL,
    UNIQUE (origin, origin_seq)
);

CREATE TABLE peers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    instance_id TEXT UNIQUE,
    secret_enc TEXT NOT NULL,
    status TEXT NOT NULL,
    last_seen_at TEXT,
    last_error TEXT,
    cursor INTEGER NOT NULL DEFAULT 0,
    events_applied INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE peer_invites (
    id TEXT PRIMARY KEY,
    secret_enc TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    used_at TEXT
);

CREATE TABLE shares (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    root_id TEXT NOT NULL,
    path TEXT NOT NULL,
    peer_ids_json TEXT NOT NULL,
    enabled INTEGER NOT NULL,
    last_scan_at TEXT
);

CREATE TABLE replica_files (
    share_id TEXT NOT NULL REFERENCES shares(id) ON DELETE CASCADE,
    rel_path TEXT NOT NULL,
    size INTEGER NOT NULL,
    mtime_ns INTEGER NOT NULL,
    sha256 TEXT,
    deleted INTEGER NOT NULL DEFAULT 0,
    vv_json TEXT NOT NULL,
    PRIMARY KEY (share_id, rel_path)
);

CREATE TABLE transfers (
    id TEXT PRIMARY KEY,
    peer_id TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
    share_id TEXT NOT NULL REFERENCES shares(id) ON DELETE CASCADE,
    rel_path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size INTEGER NOT NULL,
    vv_json TEXT NOT NULL,
    status TEXT NOT NULL,
    bytes_done INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    override_id TEXT,
    queued_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX transfers_status ON transfers(status, queued_at);

CREATE TABLE conflicts (
    id TEXT PRIMARY KEY,
    share_id TEXT NOT NULL REFERENCES shares(id) ON DELETE CASCADE,
    rel_path TEXT NOT NULL,
    kind TEXT NOT NULL,
    detail TEXT NOT NULL,
    local_json TEXT NOT NULL,
    remote_json TEXT NOT NULL,
    remote_vv_json TEXT NOT NULL,
    peer_id TEXT NOT NULL,
    status TEXT NOT NULL,
    resolution TEXT,
    created_at TEXT NOT NULL,
    resolved_at TEXT
);

CREATE TABLE overrides (
    id TEXT PRIMARY KEY,
    share_id TEXT,
    peer_id TEXT,
    until TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL,
    revoked_at TEXT
);

CREATE TABLE audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at TEXT NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    detail TEXT NOT NULL
);

-- Peer events applied through managed operations; finished when the operation ends.
CREATE TABLE pending_peer_ops (
    operation_id TEXT NOT NULL,
    idx INTEGER NOT NULL,
    share_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    from_path TEXT,
    to_path TEXT,
    vv_json TEXT NOT NULL,
    peer_id TEXT NOT NULL,
    conflict_id TEXT
);
CREATE INDEX pending_peer_ops_op ON pending_peer_ops(operation_id);
