SELECT pg_advisory_xact_lock(410173);

CREATE TABLE IF NOT EXISTS relay_enrollments_v1 (
    family_id TEXT NOT NULL CHECK (btrim(family_id) <> ''),
    server_id TEXT NOT NULL CHECK (btrim(server_id) <> ''),
    environment_id TEXT NOT NULL CHECK (btrim(environment_id) <> ''),
    installation_id TEXT NOT NULL CHECK (btrim(installation_id) <> ''),
    principal_id TEXT NOT NULL CHECK (btrim(principal_id) <> ''),
    owner_epoch BIGINT NOT NULL CHECK (owner_epoch > 0),
    vendor_ciphertext BYTEA NOT NULL CHECK (octet_length(vendor_ciphertext) > 0),
    provider_expires_at_unix_secs BIGINT NOT NULL CHECK (provider_expires_at_unix_secs > 0),
    revision BIGINT NOT NULL CHECK (revision > 0),
    updated_at_unix_secs BIGINT NOT NULL CHECK (updated_at_unix_secs >= 0),
    PRIMARY KEY (family_id, server_id, environment_id, installation_id)
);

CREATE TABLE IF NOT EXISTS relay_credentials_v1 (
    credential_id TEXT PRIMARY KEY CHECK (btrim(credential_id) <> ''),
    family_id TEXT NOT NULL CHECK (btrim(family_id) <> ''),
    server_id TEXT NOT NULL CHECK (btrim(server_id) <> ''),
    environment_id TEXT NOT NULL CHECK (btrim(environment_id) <> ''),
    installation_id TEXT NOT NULL CHECK (btrim(installation_id) <> ''),
    principal_id TEXT NOT NULL CHECK (btrim(principal_id) <> ''),
    owner_epoch BIGINT NOT NULL CHECK (owner_epoch > 0),
    enrollment_revision BIGINT NOT NULL CHECK (enrollment_revision > 0),
    verifier BYTEA NOT NULL CHECK (octet_length(verifier) > 0),
    issued_at_unix_secs BIGINT NOT NULL CHECK (issued_at_unix_secs >= 0),
    expires_at_unix_secs BIGINT NOT NULL CHECK (expires_at_unix_secs > issued_at_unix_secs),
    UNIQUE (verifier)
);

CREATE INDEX IF NOT EXISTS relay_credentials_v1_lookup_idx
    ON relay_credentials_v1 (verifier, expires_at_unix_secs);
CREATE INDEX IF NOT EXISTS relay_credentials_v1_scope_idx
    ON relay_credentials_v1 (family_id, server_id, environment_id, installation_id);
