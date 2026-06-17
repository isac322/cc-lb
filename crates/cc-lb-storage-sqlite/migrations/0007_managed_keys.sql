CREATE TABLE IF NOT EXISTS managed_keys_v1 (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    secret_hash TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER,
    status TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    key_id TEXT NOT NULL,
    label TEXT NOT NULL,
    revoked_at INTEGER,
    verify_hash BLOB NOT NULL,
    secret_salt BLOB NOT NULL,
    upstream_kind TEXT NOT NULL,
    limit_overrides TEXT NOT NULL,
    last_4 TEXT NOT NULL,
    description TEXT,
    principal_kind TEXT NOT NULL,
    index_hash BLOB NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (principal_id, key_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS managed_keys_v1_active_index_hash
ON managed_keys_v1 (index_hash)
WHERE status != 'revoked';

CREATE INDEX IF NOT EXISTS managed_keys_v1_principal_id
ON managed_keys_v1 (principal_id);
