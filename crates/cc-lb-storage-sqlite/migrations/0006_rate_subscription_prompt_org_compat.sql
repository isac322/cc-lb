CREATE TABLE IF NOT EXISTS upstream_rate_limit_states_v1 (
    upstream_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (upstream_id, key)
);

CREATE TABLE IF NOT EXISTS upstream_subscription_quotas_v1 (
    upstream_id TEXT NOT NULL,
    sample_id TEXT NOT NULL,
    sample_kind TEXT NOT NULL,
    observed_at INTEGER NOT NULL,
    input_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    request_count INTEGER NOT NULL,
    cost_usd_micros INTEGER NOT NULL,
    PRIMARY KEY (upstream_id, sample_id)
);

CREATE TABLE IF NOT EXISTS upstream_subscription_metadata_v1 (
    upstream_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS prompt_cache_observations (
    upstream_id TEXT NOT NULL,
    canonical_model_id TEXT NOT NULL,
    prefix_hash TEXT NOT NULL,
    ttl_class TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    last_observed_at INTEGER NOT NULL,
    hash_schema_version INTEGER NOT NULL,
    PRIMARY KEY (upstream_id, prefix_hash, ttl_class)
);

CREATE TABLE IF NOT EXISTS organization_metadata_v1 (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS anthropic_compatibility_kv_v1 (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
