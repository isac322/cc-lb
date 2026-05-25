CREATE TABLE IF NOT EXISTS anthropic_api_keys_v1 (
    storage_key TEXT PRIMARY KEY,
    ciphertext BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
