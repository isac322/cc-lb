DROP TABLE IF EXISTS prompt_cache_observations;

CREATE TABLE prompt_cache_observations (
    upstream_id TEXT NOT NULL,
    canonical_model_id TEXT NOT NULL,
    v3_prefix_key TEXT NOT NULL,
    ttl_class TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    last_observed_at INTEGER NOT NULL,
    hash_schema_version INTEGER NOT NULL,
    prefix_content_block_index INTEGER NOT NULL CHECK (prefix_content_block_index >= 0),
    estimated_prefix_tokens INTEGER NOT NULL CHECK (estimated_prefix_tokens >= 0),
    token_estimate_source TEXT NOT NULL,
    last_provider_cache_read_tokens INTEGER CHECK (last_provider_cache_read_tokens IS NULL OR last_provider_cache_read_tokens >= 0),
    last_provider_cache_creation_tokens INTEGER CHECK (last_provider_cache_creation_tokens IS NULL OR last_provider_cache_creation_tokens >= 0),
    PRIMARY KEY (upstream_id, canonical_model_id, v3_prefix_key, ttl_class)
);

CREATE INDEX IF NOT EXISTS prompt_cache_observations_upstream_model_expires_idx
    ON prompt_cache_observations (upstream_id, canonical_model_id, expires_at);

CREATE INDEX IF NOT EXISTS prompt_cache_observations_v3_prefix_lookup_idx
    ON prompt_cache_observations (upstream_id, canonical_model_id, v3_prefix_key, ttl_class, expires_at);
