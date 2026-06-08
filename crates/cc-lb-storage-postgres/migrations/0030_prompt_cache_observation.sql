CREATE TABLE prompt_cache_observations (
  upstream_id        UUID NOT NULL,
  canonical_model_id TEXT NOT NULL,
  prefix_hash        TEXT NOT NULL,
  ttl_class          SMALLINT NOT NULL,
  expires_at         BIGINT NOT NULL,
  last_observed_at   BIGINT NOT NULL,
  hash_schema_version SMALLINT NOT NULL DEFAULT 1,
  PRIMARY KEY (upstream_id, canonical_model_id, prefix_hash, ttl_class)
);
CREATE INDEX idx_prompt_cache_obs_upstream_expires
  ON prompt_cache_observations (upstream_id, expires_at);
