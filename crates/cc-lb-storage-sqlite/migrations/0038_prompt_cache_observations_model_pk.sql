-- prompt_cache_observations was keyed on (upstream_id, prefix_hash, ttl_class),
-- so a second model's observation overwrote another model's row for the same
-- prefix. Widen the primary key to include canonical_model_id so per-model cache
-- facts persist independently, matching the Postgres schema. The rebuild
-- preserves every existing row (each is already unique under the wider key).
CREATE TABLE prompt_cache_observations_new (
    upstream_id TEXT NOT NULL,
    canonical_model_id TEXT NOT NULL,
    prefix_hash TEXT NOT NULL,
    ttl_class TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    last_observed_at INTEGER NOT NULL,
    hash_schema_version INTEGER NOT NULL,
    PRIMARY KEY (upstream_id, canonical_model_id, prefix_hash, ttl_class)
);

INSERT INTO prompt_cache_observations_new (
    upstream_id,
    canonical_model_id,
    prefix_hash,
    ttl_class,
    expires_at,
    last_observed_at,
    hash_schema_version
)
SELECT
    upstream_id,
    canonical_model_id,
    prefix_hash,
    ttl_class,
    expires_at,
    last_observed_at,
    hash_schema_version
FROM prompt_cache_observations;

DROP TABLE prompt_cache_observations;
ALTER TABLE prompt_cache_observations_new RENAME TO prompt_cache_observations;
