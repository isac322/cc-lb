-- principals_v1.name must be unique only among live rows so a soft-deleted
-- principal stops reserving its name. SQLite cannot drop a table-level UNIQUE,
-- so the table is rebuilt. principals_v1 has no FK children, so no PRAGMA
-- foreign_keys handling is required and sqlx's own transaction is sufficient.

DROP TABLE IF EXISTS principals_v1_new;

CREATE TEMP TABLE principal_rebuild_before AS
SELECT (SELECT COUNT(*) FROM principals_v1) AS principal_rows;

CREATE TABLE principals_v1_new (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'machine' CHECK (kind IN ('machine','human','admin')),
    enabled INTEGER NOT NULL DEFAULT 1,
    allowed_models TEXT NOT NULL DEFAULT '[]',
    allowed_upstreams TEXT NOT NULL DEFAULT '[]',
    default_limits TEXT NOT NULL DEFAULT '[]',
    router_terminal_strategy TEXT NOT NULL DEFAULT 'first-pick' CHECK (router_terminal_strategy IN ('first-pick','random')),
    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    last_apply_error TEXT,
    last_apply_at INTEGER,
    deleted_at INTEGER,
    cache_keepalive_json TEXT
);

INSERT INTO principals_v1_new (
    id, name, kind, enabled, allowed_models, allowed_upstreams, default_limits,
    router_terminal_strategy, revision, created_at, updated_at,
    last_apply_error, last_apply_at, deleted_at, cache_keepalive_json
)
SELECT
    id, name, kind, enabled, allowed_models, allowed_upstreams, default_limits,
    router_terminal_strategy, revision, created_at, updated_at,
    last_apply_error, last_apply_at, deleted_at, cache_keepalive_json
FROM principals_v1;

DROP TABLE principals_v1;

ALTER TABLE principals_v1_new RENAME TO principals_v1;

CREATE UNIQUE INDEX principals_v1_name_active_uniq
    ON principals_v1 (name)
    WHERE deleted_at IS NULL;

CREATE TEMP TABLE principal_rebuild_guard (ok INTEGER NOT NULL CHECK (ok = 1));

INSERT INTO principal_rebuild_guard (ok)
SELECT CASE
    WHEN (SELECT COUNT(*) FROM principals_v1) = b.principal_rows THEN 1
    ELSE 0
END
FROM principal_rebuild_before b;

DROP TABLE temp.principal_rebuild_guard;
DROP TABLE temp.principal_rebuild_before;
