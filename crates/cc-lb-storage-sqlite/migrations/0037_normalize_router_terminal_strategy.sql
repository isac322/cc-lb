-- no-transaction
BEGIN TRANSACTION;

DROP TABLE IF EXISTS principals_v1_new;

CREATE TABLE principals_v1_new (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
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
    deleted_at INTEGER
);

INSERT INTO principals_v1_new (
    id,
    name,
    kind,
    enabled,
    allowed_models,
    allowed_upstreams,
    default_limits,
    router_terminal_strategy,
    revision,
    created_at,
    updated_at,
    last_apply_error,
    last_apply_at,
    deleted_at
)
SELECT
    id,
    name,
    kind,
    enabled,
    allowed_models,
    allowed_upstreams,
    default_limits,
    CASE
        WHEN router_terminal_strategy IN ('first-pick', 'random') THEN router_terminal_strategy
        ELSE 'first-pick'
    END AS router_terminal_strategy,
    revision,
    created_at,
    updated_at,
    last_apply_error,
    last_apply_at,
    deleted_at
FROM principals_v1;

DROP TABLE principals_v1;
ALTER TABLE principals_v1_new RENAME TO principals_v1;

COMMIT;
