-- Local session authority; only fixed-size verifier material is persisted.
CREATE TABLE codex_session_families_v1 (
    family_id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL REFERENCES principals_v1(id),
    parent_key_id TEXT NOT NULL,
    owner_binding_id TEXT NOT NULL REFERENCES account_owner_bindings_v1(binding_id),
    provider TEXT NOT NULL,
    owner_generation INTEGER NOT NULL CHECK(owner_generation > 0),
    binding_revision INTEGER NOT NULL CHECK(binding_revision > 0),
    workspace_id TEXT NOT NULL,
    workspace_generation INTEGER NOT NULL CHECK(workspace_generation > 0),
    access_generation INTEGER NOT NULL CHECK(access_generation > 0),
    local_user_id TEXT NOT NULL,
    plan_type TEXT NOT NULL,
    origin TEXT NOT NULL,
    access_ttl_secs INTEGER NOT NULL CHECK(access_ttl_secs > 0),
    created_at INTEGER NOT NULL CHECK(created_at >= 0),
    expires_at INTEGER NOT NULL CHECK(expires_at > created_at),
    revoked_at INTEGER,
    FOREIGN KEY(principal_id, parent_key_id) REFERENCES managed_keys_v1(principal_id, key_id)
);
CREATE TABLE codex_session_credentials_v1 (
    credential_id TEXT PRIMARY KEY,
    family_id TEXT NOT NULL REFERENCES codex_session_families_v1(family_id),
    purpose TEXT NOT NULL CHECK(purpose IN ('access', 'renewal')),
    index_hash BLOB NOT NULL UNIQUE CHECK(length(index_hash) = 32),
    verify_hash BLOB NOT NULL CHECK(length(verify_hash) = 32),
    salt BLOB NOT NULL CHECK(length(salt) = 16),
    workspace_generation INTEGER NOT NULL CHECK(workspace_generation > 0),
    access_generation INTEGER NOT NULL CHECK(access_generation > 0),
    issued_at INTEGER NOT NULL CHECK(issued_at >= 0),
    expires_at INTEGER NOT NULL CHECK(expires_at > issued_at),
    revoked_at INTEGER
);
CREATE INDEX codex_session_credentials_v1_family ON codex_session_credentials_v1(family_id);
CREATE INDEX codex_session_families_v1_principal ON codex_session_families_v1(principal_id);
