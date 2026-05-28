CREATE TABLE IF NOT EXISTS managed_api_keys_v1 (
    principal_id            TEXT        NOT NULL,
    key_id                  TEXT        NOT NULL,
    label                   TEXT        NOT NULL,
    issued_at_unix_secs     BIGINT      NOT NULL,
    revoked_at_unix_secs    BIGINT,
    key_hash_b64            TEXT        NOT NULL,
    verify_hash             BYTEA       NOT NULL,
    secret_salt             BYTEA       NOT NULL,
    upstream_kind           TEXT        NOT NULL CHECK (upstream_kind IN ('anthropic_key', 'anthropic_oauth')),
    upstream_credential_ref TEXT        NOT NULL,
    limit_overrides         JSONB       NOT NULL DEFAULT '[]'::jsonb,
    status                  TEXT        NOT NULL CHECK (status IN ('active', 'disabled', 'revoked')),
    expires_at_unix_secs    BIGINT,
    last_4                  TEXT        NOT NULL,
    description             TEXT,
    principal_kind          TEXT        NOT NULL CHECK (principal_kind IN ('human', 'machine')),
    index_hash              BYTEA       NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (principal_id, key_id)
);
CREATE INDEX IF NOT EXISTS managed_api_keys_v1_principal_id ON managed_api_keys_v1 (principal_id);
CREATE INDEX IF NOT EXISTS managed_api_keys_v1_active_idx ON managed_api_keys_v1 (principal_id) WHERE status = 'active';
