SET LOCAL lock_timeout = '1s';

-- Parent/principal/owner authority is checked under ordered row locks in the adapter.
-- Avoid authority foreign keys: principal deletion must not invert the owner-first lock order.
CREATE TABLE IF NOT EXISTS codex_session_families_v1 (
    family_id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL,
    parent_key_id TEXT NOT NULL,
    owner_binding_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    owner_generation BIGINT NOT NULL CHECK (owner_generation >= 1),
    binding_revision BIGINT NOT NULL CHECK (binding_revision >= 1),
    workspace_id TEXT NOT NULL,
    workspace_generation BIGINT NOT NULL CHECK (workspace_generation >= 1),
    access_generation BIGINT NOT NULL CHECK (access_generation >= 1),
    local_user_id TEXT NOT NULL,
    plan_type TEXT NOT NULL,
    origin TEXT NOT NULL,
    access_ttl_secs BIGINT NOT NULL CHECK (access_ttl_secs > 0),
    created_at BIGINT NOT NULL CHECK (created_at >= 0),
    expires_at BIGINT NOT NULL CHECK (expires_at > created_at),
    revoked_at BIGINT CHECK (revoked_at >= created_at),
    CHECK (access_ttl_secs <= expires_at - created_at)
);

CREATE TABLE IF NOT EXISTS codex_session_credentials_v1 (
    credential_id TEXT PRIMARY KEY,
    family_id TEXT NOT NULL REFERENCES codex_session_families_v1(family_id),
    purpose TEXT NOT NULL CHECK (purpose IN ('access', 'renewal')),
    index_hash BYTEA NOT NULL UNIQUE CHECK (octet_length(index_hash) = 32),
    verify_hash BYTEA NOT NULL CHECK (octet_length(verify_hash) = 32),
    salt BYTEA NOT NULL CHECK (octet_length(salt) = 16),
    workspace_generation BIGINT NOT NULL CHECK (workspace_generation >= 1),
    access_generation BIGINT NOT NULL CHECK (access_generation >= 1),
    issued_at BIGINT NOT NULL CHECK (issued_at >= 0),
    expires_at BIGINT NOT NULL CHECK (expires_at > issued_at),
    revoked_at BIGINT CHECK (revoked_at >= issued_at)
);

CREATE INDEX IF NOT EXISTS codex_session_credentials_v1_family_idx
    ON codex_session_credentials_v1(family_id);
CREATE UNIQUE INDEX IF NOT EXISTS codex_session_credentials_v1_renewal_idx
    ON codex_session_credentials_v1(family_id) WHERE purpose = 'renewal';
