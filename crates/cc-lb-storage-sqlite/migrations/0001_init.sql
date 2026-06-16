CREATE TABLE IF NOT EXISTS meta_v1 (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS api_keys_v1 (
    id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL,
    name TEXT NOT NULL,
    secret_hash TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS oauth_credentials_v1 (
    principal_id TEXT PRIMARY KEY,
    client_id TEXT NOT NULL,
    client_secret_ciphertext BLOB NOT NULL,
    auth_url TEXT NOT NULL,
    token_url TEXT NOT NULL,
    redirect_uri TEXT NOT NULL,
    scopes TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
