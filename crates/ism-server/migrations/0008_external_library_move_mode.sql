ALTER TABLE immich_servers
ADD COLUMN external_library_move_mode TEXT NOT NULL DEFAULT 'api_with_db_fallback'
CHECK (external_library_move_mode IN ('api_only', 'api_with_db_fallback', 'db_only'));

ALTER TABLE immich_servers
ADD COLUMN external_library_api_capable INTEGER;
