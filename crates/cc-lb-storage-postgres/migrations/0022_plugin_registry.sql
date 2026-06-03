CREATE TABLE IF NOT EXISTS plugin_registry (
    sha256 BYTEA PRIMARY KEY CHECK (length(sha256) = 32),
    plugin_name TEXT NOT NULL,
    plugin_version TEXT NOT NULL,
    abi_envelope BIGINT NOT NULL CHECK (abi_envelope >= 0 AND abi_envelope <= 4294967295),
    augmented_metadata BYTEA NOT NULL CHECK (length(augmented_metadata) <= 262144),
    host_offer_hash BYTEA NOT NULL CHECK (length(host_offer_hash) = 32),
    handshake_schema_version BIGINT NOT NULL CHECK (handshake_schema_version >= 0 AND handshake_schema_version <= 4294967295),
    last_handshake_at BIGINT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled'))
);

CREATE INDEX IF NOT EXISTS plugin_registry_status_idx
    ON plugin_registry (status, plugin_name, sha256);

CREATE TABLE IF NOT EXISTS plugin_registry_marker (
    key TEXT PRIMARY KEY,
    unix_secs BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS plugin_registry_blobs (
    sha256 BYTEA PRIMARY KEY CHECK (length(sha256) = 32),
    bytes BYTEA NOT NULL
);
