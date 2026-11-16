SELECT pg_advisory_xact_lock(410173);

CREATE TABLE IF NOT EXISTS codex_relay_enrollments_v1 (
    enrollment_id TEXT NOT NULL UNIQUE CHECK (btrim(enrollment_id) <> ''),
    family_id TEXT NOT NULL,
    server_id TEXT NOT NULL CHECK (btrim(server_id) <> ''),
    environment_id TEXT NOT NULL CHECK (btrim(environment_id) <> ''),
    installation_id TEXT NOT NULL CHECK (btrim(installation_id) <> ''),
    principal_id TEXT NOT NULL CHECK (btrim(principal_id) <> ''),
    parent_key_id TEXT NOT NULL CHECK (btrim(parent_key_id) <> ''),
    owner_binding_id TEXT NOT NULL CHECK (btrim(owner_binding_id) <> ''),
    provider TEXT NOT NULL CHECK (btrim(provider) <> ''),
    owner_generation BIGINT NOT NULL CHECK (owner_generation > 0),
    binding_revision BIGINT NOT NULL CHECK (binding_revision > 0),
    workspace_id TEXT NOT NULL CHECK (btrim(workspace_id) <> ''),
    workspace_generation BIGINT NOT NULL CHECK (workspace_generation > 0),
    access_generation BIGINT NOT NULL CHECK (access_generation > 0),
    origin TEXT NOT NULL CHECK (btrim(origin) <> ''),
    vendor_ciphertext BYTEA NOT NULL CHECK (octet_length(vendor_ciphertext) > 0),
    provider_expires_at_unix_secs BIGINT NOT NULL CHECK (provider_expires_at_unix_secs > 0),
    generation BIGINT NOT NULL CHECK (generation > 0),
    updated_at_unix_secs BIGINT NOT NULL CHECK (updated_at_unix_secs >= 0),
    PRIMARY KEY (family_id, server_id, environment_id, installation_id),
    UNIQUE (enrollment_id, family_id, server_id, environment_id, installation_id),
    FOREIGN KEY (family_id) REFERENCES codex_session_families_v1 (family_id)
);

CREATE TABLE IF NOT EXISTS codex_relay_credentials_v1 (
    credential_id TEXT PRIMARY KEY CHECK (btrim(credential_id) <> ''),
    enrollment_id TEXT NOT NULL,
    family_id TEXT NOT NULL,
    server_id TEXT NOT NULL,
    environment_id TEXT NOT NULL,
    installation_id TEXT NOT NULL,
    enrollment_generation BIGINT NOT NULL CHECK (enrollment_generation > 0),
    index_hash BYTEA NOT NULL UNIQUE CHECK (octet_length(index_hash) = 32),
    verify_hash BYTEA NOT NULL CHECK (octet_length(verify_hash) = 32),
    salt BYTEA NOT NULL CHECK (octet_length(salt) = 16),
    issued_at_unix_secs BIGINT NOT NULL CHECK (issued_at_unix_secs >= 0),
    expires_at_unix_secs BIGINT NOT NULL CHECK (expires_at_unix_secs > issued_at_unix_secs),
    FOREIGN KEY (enrollment_id, family_id, server_id, environment_id, installation_id)
        REFERENCES codex_relay_enrollments_v1
            (enrollment_id, family_id, server_id, environment_id, installation_id)
);

CREATE INDEX IF NOT EXISTS codex_relay_credentials_v1_enrollment_idx
    ON codex_relay_credentials_v1 (enrollment_id);
