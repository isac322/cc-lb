-- managed_keys_v1 duplicated principal/upstream kinds already owned by their
-- respective DB records. SQLite cannot drop columns in place while preserving
-- the table contract, so rebuild it and verify every key survived.

DROP TABLE IF EXISTS managed_keys_v1_new;

CREATE TEMP TABLE managed_keys_rebuild_before AS
SELECT COUNT(*) AS key_rows FROM managed_keys_v1;

CREATE TABLE managed_keys_v1_new (
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
    limit_overrides TEXT NOT NULL,
    last_4 TEXT NOT NULL,
    description TEXT,
    index_hash BLOB NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (principal_id, key_id)
);

INSERT INTO managed_keys_v1_new (
    id,
    name,
    secret_hash,
    created_at,
    expires_at,
    status,
    principal_id,
    key_id,
    label,
    revoked_at,
    verify_hash,
    secret_salt,
    limit_overrides,
    last_4,
    description,
    index_hash,
    updated_at
)
SELECT
    id,
    name,
    secret_hash,
    created_at,
    expires_at,
    status,
    principal_id,
    key_id,
    label,
    revoked_at,
    verify_hash,
    secret_salt,
    limit_overrides,
    last_4,
    description,
    index_hash,
    updated_at
FROM managed_keys_v1;

DROP TABLE managed_keys_v1;
ALTER TABLE managed_keys_v1_new RENAME TO managed_keys_v1;

CREATE UNIQUE INDEX managed_keys_v1_active_index_hash
    ON managed_keys_v1 (index_hash)
    WHERE status != 'revoked';

CREATE INDEX managed_keys_v1_principal_id
    ON managed_keys_v1 (principal_id);

CREATE TEMP TABLE managed_keys_rebuild_guard (
    ok INTEGER NOT NULL CHECK (ok = 1)
);

INSERT INTO managed_keys_rebuild_guard (ok)
SELECT CASE
    WHEN (SELECT COUNT(*) FROM managed_keys_v1) = b.key_rows THEN 1
    ELSE 0
END
FROM managed_keys_rebuild_before b;

DROP TABLE temp.managed_keys_rebuild_guard;
DROP TABLE temp.managed_keys_rebuild_before;
