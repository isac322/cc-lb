CREATE TABLE IF NOT EXISTS principals_v1 (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    enabled INTEGER NOT NULL DEFAULT 1,
    allowed_upstreams TEXT NOT NULL DEFAULT '[]',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    deleted_at INTEGER
);

CREATE TABLE IF NOT EXISTS upstream_spec_v1 (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
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

CREATE TABLE IF NOT EXISTS upstream_api_key_secret_v1 (
    upstream_id TEXT PRIMARY KEY REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    api_key_ciphertext BLOB,
    secret_revision INTEGER NOT NULL DEFAULT 1 CHECK (secret_revision >= 0),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS upstream_oauth_token_v1 (
    upstream_id TEXT PRIMARY KEY REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    oauth_credentials_ciphertext BLOB,
    token_revision INTEGER NOT NULL DEFAULT 1 CHECK (token_revision >= 0),
    refreshed_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS upstream_status_v1 (
    upstream_id TEXT PRIMARY KEY REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    last_apply_error TEXT,
    last_apply_at INTEGER,
    observed_spec_revision INTEGER,
    observed_api_key_secret_revision INTEGER,
    observed_oauth_token_revision INTEGER,
    next_warmup_at INTEGER,
    last_warmup_cycle_key TEXT,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS upstream_lease_v1 (
    upstream_id TEXT NOT NULL REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    lease_kind TEXT NOT NULL CHECK (lease_kind IN ('warmup','refresh')),
    holder TEXT NOT NULL,
    until_unix_secs INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (upstream_id, lease_kind)
);
