-- no-transaction
-- upstream_spec_v1.name must be unique only among live rows so a soft-deleted
-- upstream stops reserving its name. SQLite cannot drop a table-level UNIQUE,
-- so the table is rebuilt. upstream_spec_v1 is an FK parent whose children use
-- ON DELETE CASCADE, and DROP TABLE runs an implicit DELETE FROM that fires
-- those cascades, so FK enforcement is disabled outside the transaction
-- (PRAGMA foreign_keys is a no-op inside one). The guards below turn any
-- accidental cascade into a hard migration failure instead of silent data loss.

PRAGMA foreign_keys = OFF;

BEGIN IMMEDIATE;

DROP TABLE IF EXISTS upstream_spec_v1_new;

CREATE TEMP TABLE upstream_rebuild_before AS
SELECT
    (SELECT COUNT(*) FROM upstream_spec_v1) AS spec_rows,
    (SELECT COUNT(*) FROM upstream_api_key_secret_v1) AS api_key_rows,
    (SELECT COUNT(*) FROM upstream_oauth_token_v1) AS oauth_token_rows,
    (SELECT COUNT(*) FROM upstream_status_v1) AS status_rows,
    (SELECT COUNT(*) FROM warmup_attempts_v1) AS warmup_attempt_rows;

CREATE TABLE upstream_spec_v1_new (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('anthropic_api_key','anthropic_oauth')),
    base_url TEXT,
    enabled INTEGER NOT NULL DEFAULT 1,
    warmup_enabled INTEGER NOT NULL DEFAULT 0,
    warmup_dialect_plugin TEXT,
    spec_revision INTEGER NOT NULL DEFAULT 1 CHECK (spec_revision >= 0),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    deleted_at INTEGER
);

INSERT INTO upstream_spec_v1_new (
    id, name, kind, base_url, enabled, warmup_enabled, warmup_dialect_plugin,
    spec_revision, created_at, updated_at, deleted_at
)
SELECT
    id, name, kind, base_url, enabled, warmup_enabled, warmup_dialect_plugin,
    spec_revision, created_at, updated_at, deleted_at
FROM upstream_spec_v1;

DROP TABLE upstream_spec_v1;

ALTER TABLE upstream_spec_v1_new RENAME TO upstream_spec_v1;

CREATE UNIQUE INDEX upstream_spec_v1_name_active_uniq
    ON upstream_spec_v1 (name)
    WHERE deleted_at IS NULL;

CREATE INDEX upstream_spec_v1_warmup_wasm_registry_id_idx
    ON upstream_spec_v1 (json_extract(warmup_dialect_plugin, '$.wasm_registry_id'))
    WHERE deleted_at IS NULL AND warmup_dialect_plugin IS NOT NULL;

CREATE TEMP TABLE upstream_rebuild_guard (ok INTEGER NOT NULL CHECK (ok = 1));

INSERT INTO upstream_rebuild_guard (ok)
SELECT CASE
    WHEN (SELECT COUNT(*) FROM upstream_spec_v1) = b.spec_rows
     AND (SELECT COUNT(*) FROM upstream_api_key_secret_v1) = b.api_key_rows
     AND (SELECT COUNT(*) FROM upstream_oauth_token_v1) = b.oauth_token_rows
     AND (SELECT COUNT(*) FROM upstream_status_v1) = b.status_rows
     AND (SELECT COUNT(*) FROM warmup_attempts_v1) = b.warmup_attempt_rows
    THEN 1
    ELSE 0
END
FROM upstream_rebuild_before b;

INSERT INTO upstream_rebuild_guard (ok)
SELECT CASE WHEN
    NOT EXISTS (
        SELECT 1 FROM upstream_api_key_secret_v1 c
        LEFT JOIN upstream_spec_v1 s ON s.id = c.upstream_id
        WHERE s.id IS NULL
    )
AND NOT EXISTS (
        SELECT 1 FROM upstream_oauth_token_v1 c
        LEFT JOIN upstream_spec_v1 s ON s.id = c.upstream_id
        WHERE s.id IS NULL
    )
AND NOT EXISTS (
        SELECT 1 FROM upstream_status_v1 c
        LEFT JOIN upstream_spec_v1 s ON s.id = c.upstream_id
        WHERE s.id IS NULL
    )
AND NOT EXISTS (
        SELECT 1 FROM warmup_attempts_v1 c
        LEFT JOIN upstream_spec_v1 s ON s.id = c.upstream_id
        WHERE s.id IS NULL
    )
    THEN 1
    ELSE 0
END;

DROP TABLE temp.upstream_rebuild_guard;
DROP TABLE temp.upstream_rebuild_before;

COMMIT;

PRAGMA foreign_keys = ON;
