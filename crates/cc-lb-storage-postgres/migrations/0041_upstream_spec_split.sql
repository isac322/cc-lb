SELECT pg_advisory_xact_lock(410041);

CREATE TABLE IF NOT EXISTS upstream_spec_v1 (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL CHECK (kind IN ('anthropic_api_key', 'anthropic_oauth')),
    base_url TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    warmup_enabled BOOLEAN NOT NULL DEFAULT false,
    warmup_dialect_plugin JSONB,
    spec_revision BIGINT NOT NULL CHECK (spec_revision >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS upstream_api_key_secret_v1 (
    upstream_id UUID PRIMARY KEY REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    api_key_ciphertext BYTEA,
    secret_revision BIGINT NOT NULL CHECK (secret_revision >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS upstream_oauth_token_v1 (
    upstream_id UUID PRIMARY KEY REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    oauth_credentials_ciphertext BYTEA,
    token_revision BIGINT NOT NULL CHECK (token_revision >= 0),
    refreshed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS upstream_status_v1 (
    upstream_id UUID PRIMARY KEY REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    last_apply_error TEXT,
    last_apply_at TIMESTAMPTZ,
    observed_spec_revision BIGINT,
    observed_api_key_secret_revision BIGINT,
    observed_oauth_token_revision BIGINT,
    next_warmup_at TIMESTAMPTZ,
    last_warmup_cycle_key BIGINT,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS upstream_lease_v1 (
    upstream_id UUID NOT NULL REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    lease_kind TEXT NOT NULL CHECK (lease_kind IN ('warmup', 'refresh')),
    holder TEXT NOT NULL,
    until_unix_secs BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (upstream_id, lease_kind)
);

CREATE INDEX IF NOT EXISTS upstream_spec_v1_name_idx ON upstream_spec_v1 (name);
CREATE INDEX IF NOT EXISTS upstream_spec_v1_enabled_idx ON upstream_spec_v1 (enabled);
CREATE INDEX IF NOT EXISTS upstream_spec_v1_active_idx ON upstream_spec_v1 (id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS upstream_status_v1_warmup_due_idx
ON upstream_status_v1 (next_warmup_at)
WHERE next_warmup_at IS NOT NULL;

INSERT INTO upstream_spec_v1 (
    id, name, kind, base_url, enabled, warmup_enabled, warmup_dialect_plugin,
    spec_revision, created_at, updated_at, deleted_at
)
SELECT
    id, name, kind, base_url, enabled, warmup_enabled, warmup_dialect_plugin,
    revision, created_at, updated_at, deleted_at
FROM upstreams_v1
ON CONFLICT (id) DO NOTHING;

INSERT INTO upstream_api_key_secret_v1 (
    upstream_id, api_key_ciphertext, secret_revision, created_at, updated_at
)
SELECT id, api_key_ciphertext, revision, created_at, updated_at
FROM upstreams_v1
WHERE api_key_ciphertext IS NOT NULL
ON CONFLICT (upstream_id) DO NOTHING;

INSERT INTO upstream_oauth_token_v1 (
    upstream_id, oauth_credentials_ciphertext, token_revision, refreshed_at, created_at, updated_at
)
SELECT id, oauth_credentials, revision, updated_at, created_at, updated_at
FROM upstreams_v1
WHERE oauth_credentials IS NOT NULL
ON CONFLICT (upstream_id) DO NOTHING;

INSERT INTO upstream_status_v1 (
    upstream_id, last_apply_error, last_apply_at,
    observed_spec_revision, observed_api_key_secret_revision, observed_oauth_token_revision,
    next_warmup_at, last_warmup_cycle_key, updated_at
)
SELECT
    id, last_apply_error, last_apply_at,
    NULL, NULL, NULL,
    next_warmup_at, last_warmup_cycle_key, updated_at
FROM upstreams_v1
WHERE last_apply_error IS NOT NULL
   OR last_apply_at IS NOT NULL
   OR next_warmup_at IS NOT NULL
   OR last_warmup_cycle_key IS NOT NULL
ON CONFLICT (upstream_id) DO NOTHING;

INSERT INTO upstream_lease_v1 (upstream_id, lease_kind, holder, until_unix_secs, updated_at)
SELECT id, 'refresh', refresh_lease_holder::text, extract(epoch from refresh_lease_until)::bigint, updated_at
FROM upstreams_v1
WHERE refresh_lease_holder IS NOT NULL AND refresh_lease_until IS NOT NULL
ON CONFLICT (upstream_id, lease_kind) DO NOTHING;

INSERT INTO upstream_lease_v1 (upstream_id, lease_kind, holder, until_unix_secs, updated_at)
SELECT id, 'warmup', warmup_lease_holder, warmup_lease_until_unix_secs, updated_at
FROM upstreams_v1
WHERE warmup_lease_holder IS NOT NULL AND warmup_lease_until_unix_secs IS NOT NULL
ON CONFLICT (upstream_id, lease_kind) DO NOTHING;

DROP INDEX IF EXISTS upstreams_v1_name_idx;
DROP INDEX IF EXISTS upstreams_v1_enabled_idx;
DROP INDEX IF EXISTS upstreams_v1_active_idx;
DROP TABLE IF EXISTS upstreams_v1;
