-- Provider-native cache evidence is isolated from Anthropic v3 prefix rows.
CREATE TABLE IF NOT EXISTS provider_native_cache_observations (
    upstream_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    canonical_model_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    isolation_scope TEXT NOT NULL,
    identity_hash TEXT NOT NULL,
    boundary TEXT NOT NULL,
    evidence_source TEXT NOT NULL,
    observed_cached_tokens INTEGER,
    observed_write_tokens INTEGER,
    retention_secs INTEGER,
    expires_at INTEGER,
    last_observed_at INTEGER NOT NULL,
    PRIMARY KEY (upstream_id, provider, canonical_model_id, mode, isolation_scope, identity_hash, boundary)
);
CREATE INDEX IF NOT EXISTS provider_native_cache_observations_active_idx
    ON provider_native_cache_observations (upstream_id, provider, canonical_model_id, mode, isolation_scope, identity_hash, expires_at);
