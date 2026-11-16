CREATE TABLE IF NOT EXISTS codex_relay_enrollments_v1 (
    enrollment_id TEXT NOT NULL UNIQUE CHECK (length(trim(enrollment_id)) > 0),
    family_id TEXT NOT NULL,
    server_id TEXT NOT NULL CHECK (length(trim(server_id)) > 0),
    environment_id TEXT NOT NULL CHECK (length(trim(environment_id)) > 0),
    installation_id TEXT NOT NULL CHECK (length(trim(installation_id)) > 0),
    principal_id TEXT NOT NULL CHECK (length(trim(principal_id)) > 0),
    parent_key_id TEXT NOT NULL CHECK (length(trim(parent_key_id)) > 0),
    owner_binding_id TEXT NOT NULL CHECK (length(trim(owner_binding_id)) > 0),
    provider TEXT NOT NULL CHECK (length(trim(provider)) > 0),
    owner_generation INTEGER NOT NULL CHECK (owner_generation > 0),
    binding_revision INTEGER NOT NULL CHECK (binding_revision > 0),
    workspace_id TEXT NOT NULL CHECK (length(trim(workspace_id)) > 0),
    workspace_generation INTEGER NOT NULL CHECK (workspace_generation > 0),
    access_generation INTEGER NOT NULL CHECK (access_generation > 0),
    origin TEXT NOT NULL CHECK (length(trim(origin)) > 0),
    vendor_ciphertext BLOB NOT NULL CHECK (length(vendor_ciphertext) > 0),
    provider_expires_at_unix_secs INTEGER NOT NULL CHECK (provider_expires_at_unix_secs > 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    updated_at_unix_secs INTEGER NOT NULL CHECK (updated_at_unix_secs >= 0),
    PRIMARY KEY (family_id, server_id, environment_id, installation_id),
    UNIQUE (enrollment_id, family_id, server_id, environment_id, installation_id),
    FOREIGN KEY (family_id) REFERENCES codex_session_families_v1 (family_id)
);

CREATE TABLE IF NOT EXISTS codex_relay_credentials_v1 (
    credential_id TEXT PRIMARY KEY CHECK (length(trim(credential_id)) > 0),
    enrollment_id TEXT NOT NULL,
    family_id TEXT NOT NULL,
    server_id TEXT NOT NULL,
    environment_id TEXT NOT NULL,
    installation_id TEXT NOT NULL,
    enrollment_generation INTEGER NOT NULL CHECK (enrollment_generation > 0),
    index_hash BLOB NOT NULL UNIQUE CHECK (length(index_hash) = 32),
    verify_hash BLOB NOT NULL CHECK (length(verify_hash) = 32),
    salt BLOB NOT NULL CHECK (length(salt) = 16),
    issued_at_unix_secs INTEGER NOT NULL CHECK (issued_at_unix_secs >= 0),
    expires_at_unix_secs INTEGER NOT NULL CHECK (expires_at_unix_secs > issued_at_unix_secs),
    FOREIGN KEY (enrollment_id, family_id, server_id, environment_id, installation_id)
        REFERENCES codex_relay_enrollments_v1
            (enrollment_id, family_id, server_id, environment_id, installation_id)
);

CREATE INDEX IF NOT EXISTS codex_relay_credentials_v1_enrollment_idx
    ON codex_relay_credentials_v1 (enrollment_id);
