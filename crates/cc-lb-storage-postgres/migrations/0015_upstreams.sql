CREATE TABLE IF NOT EXISTS upstreams_v1 (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL CHECK (kind IN ('anthropic_api_key', 'anthropic_oauth', 'custom')),
    base_url TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    oauth_credentials BYTEA,
    api_key_ciphertext BYTEA,
    refresh_lease_holder UUID,
    refresh_lease_until TIMESTAMPTZ,
    last_apply_error TEXT,
    last_apply_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    revision BIGINT NOT NULL CHECK (revision >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS upstreams_v1_name_idx ON upstreams_v1 (name);
CREATE INDEX IF NOT EXISTS upstreams_v1_enabled_idx ON upstreams_v1 (enabled);
CREATE INDEX IF NOT EXISTS upstreams_v1_active_idx ON upstreams_v1 (id) WHERE deleted_at IS NULL;
