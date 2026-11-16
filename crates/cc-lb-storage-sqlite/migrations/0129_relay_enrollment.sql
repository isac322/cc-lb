CREATE TABLE IF NOT EXISTS relay_enrollments_v1 (
    family_id TEXT NOT NULL CHECK (length(trim(family_id)) > 0),
    server_id TEXT NOT NULL CHECK (length(trim(server_id)) > 0),
    environment_id TEXT NOT NULL CHECK (length(trim(environment_id)) > 0),
    installation_id TEXT NOT NULL CHECK (length(trim(installation_id)) > 0),
    principal_id TEXT NOT NULL CHECK (length(trim(principal_id)) > 0),
    owner_epoch INTEGER NOT NULL CHECK (owner_epoch > 0),
    vendor_ciphertext BLOB NOT NULL CHECK (length(vendor_ciphertext) > 0),
    provider_expires_at_unix_secs INTEGER NOT NULL CHECK (provider_expires_at_unix_secs > 0),
    revision INTEGER NOT NULL CHECK (revision > 0),
    updated_at_unix_secs INTEGER NOT NULL CHECK (updated_at_unix_secs >= 0),
    PRIMARY KEY (family_id, server_id, environment_id, installation_id)
);

CREATE TABLE IF NOT EXISTS relay_credentials_v1 (
    credential_id TEXT PRIMARY KEY CHECK (length(trim(credential_id)) > 0),
    family_id TEXT NOT NULL CHECK (length(trim(family_id)) > 0),
    server_id TEXT NOT NULL CHECK (length(trim(server_id)) > 0),
    environment_id TEXT NOT NULL CHECK (length(trim(environment_id)) > 0),
    installation_id TEXT NOT NULL CHECK (length(trim(installation_id)) > 0),
    principal_id TEXT NOT NULL CHECK (length(trim(principal_id)) > 0),
    owner_epoch INTEGER NOT NULL CHECK (owner_epoch > 0),
    enrollment_revision INTEGER NOT NULL CHECK (enrollment_revision > 0),
    verifier BLOB NOT NULL CHECK (length(verifier) > 0),
    issued_at_unix_secs INTEGER NOT NULL CHECK (issued_at_unix_secs >= 0),
    expires_at_unix_secs INTEGER NOT NULL CHECK (expires_at_unix_secs > issued_at_unix_secs),
    UNIQUE (verifier)
);

CREATE INDEX IF NOT EXISTS relay_credentials_v1_lookup_idx
    ON relay_credentials_v1 (verifier, expires_at_unix_secs);
CREATE INDEX IF NOT EXISTS relay_credentials_v1_scope_idx
    ON relay_credentials_v1 (family_id, server_id, environment_id, installation_id);
