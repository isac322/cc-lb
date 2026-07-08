DROP TABLE IF EXISTS prompt_cache_observations;

CREATE TABLE prompt_cache_observations (
  upstream_id UUID NOT NULL,
  canonical_model_id TEXT NOT NULL,
  v3_prefix_key TEXT NOT NULL,
  ttl_class SMALLINT NOT NULL,
  expires_at BIGINT NOT NULL,
  last_observed_at BIGINT NOT NULL,
  hash_schema_version SMALLINT NOT NULL,
  prefix_content_block_index BIGINT NOT NULL CHECK (prefix_content_block_index >= 0),
  estimated_prefix_tokens BIGINT NOT NULL CHECK (estimated_prefix_tokens >= 0),
  token_estimate_source TEXT NOT NULL,
  last_provider_cache_read_tokens BIGINT NULL CHECK (last_provider_cache_read_tokens IS NULL OR last_provider_cache_read_tokens >= 0),
  last_provider_cache_creation_tokens BIGINT NULL CHECK (last_provider_cache_creation_tokens IS NULL OR last_provider_cache_creation_tokens >= 0),
  PRIMARY KEY (upstream_id, canonical_model_id, v3_prefix_key, ttl_class)
);

CREATE INDEX prompt_cache_observations_upstream_model_expires_idx
  ON prompt_cache_observations (upstream_id, canonical_model_id, expires_at);

CREATE INDEX prompt_cache_observations_v3_prefix_lookup_idx
  ON prompt_cache_observations (upstream_id, canonical_model_id, v3_prefix_key, ttl_class, expires_at);
