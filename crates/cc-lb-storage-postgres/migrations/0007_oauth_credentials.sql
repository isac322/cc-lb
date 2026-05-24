CREATE TABLE IF NOT EXISTS oauth_credentials_v1 (
    principal_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    ciphertext BYTEA NOT NULL,
    revision BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (principal_id, provider)
);
