CREATE TABLE IF NOT EXISTS api_keys_v1 (
    principal_id TEXT NOT NULL,
    key_id TEXT NOT NULL,
    ciphertext BYTEA NOT NULL,
    revision BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (principal_id, key_id)
);
