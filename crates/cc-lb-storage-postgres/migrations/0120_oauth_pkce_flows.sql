SET LOCAL lock_timeout = '1s';

CREATE TABLE IF NOT EXISTS oauth_pkce_flows_v1 (
    state_token TEXT PRIMARY KEY,
    encrypted_payload BYTEA NOT NULL,
    created_at_unix_secs BIGINT NOT NULL CHECK (created_at_unix_secs >= 0),
    expires_at_unix_secs BIGINT NOT NULL CHECK (expires_at_unix_secs >= 0)
);

CREATE INDEX IF NOT EXISTS oauth_pkce_flows_v1_expires_at_idx
    ON oauth_pkce_flows_v1 (expires_at_unix_secs);
