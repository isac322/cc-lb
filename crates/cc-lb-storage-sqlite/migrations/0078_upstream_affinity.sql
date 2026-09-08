CREATE TABLE IF NOT EXISTS upstream_affinity_v1 (
    principal_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('anthropic_web_search_encrypted_content')),
    value_sha256 BLOB NOT NULL CHECK (length(value_sha256) = 32),
    upstream_id TEXT NOT NULL REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    observed_at_unix_secs INTEGER NOT NULL CHECK (observed_at_unix_secs >= 0),
    expires_at_unix_secs INTEGER CHECK (expires_at_unix_secs IS NULL OR expires_at_unix_secs >= 0),
    PRIMARY KEY (principal_id, provider, kind, value_sha256)
);

CREATE INDEX IF NOT EXISTS upstream_affinity_v1_upstream_id_idx
    ON upstream_affinity_v1 (upstream_id);

CREATE INDEX IF NOT EXISTS upstream_affinity_v1_expires_at_idx
    ON upstream_affinity_v1 (expires_at_unix_secs)
    WHERE expires_at_unix_secs IS NOT NULL;
