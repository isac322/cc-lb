CREATE TABLE IF NOT EXISTS upstream_rate_limit_states_v1 (
    upstream_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (upstream_id, key)
);

CREATE TABLE IF NOT EXISTS upstream_subscription_quota_observations_v1 (
    upstream_id TEXT NOT NULL,
    window TEXT NOT NULL CHECK (window IN ('5h','7d','7d_sonnet','7d_opus','overage','unified')),
    source TEXT NOT NULL CHECK (source IN ('header','api')),
    sample_kind TEXT NOT NULL DEFAULT 'sample' CHECK (sample_kind IN ('sample','process_start')),
    observed_at_unix_millis INTEGER NOT NULL CHECK (observed_at_unix_millis >= 0),
    sample_id TEXT NOT NULL,
    utilization REAL CHECK (utilization IS NULL OR (utilization >= 0.0 AND utilization <= 1.0)),
    status TEXT CHECK (status IS NULL OR status IN ('allowed','allowed_warning','rejected')),
    resets_at_unix_secs INTEGER CHECK (resets_at_unix_secs IS NULL OR resets_at_unix_secs >= 0),
    surpassed_threshold INTEGER CHECK (surpassed_threshold IS NULL OR surpassed_threshold IN (0, 1)),
    representative_claim TEXT,
    disabled_reason TEXT,
    extra_usage_enabled INTEGER CHECK (extra_usage_enabled IS NULL OR extra_usage_enabled IN (0, 1)),
    extra_usage_monthly_limit REAL,
    extra_usage_used_credits REAL,
    ingested_at_unix_millis INTEGER NOT NULL CHECK (ingested_at_unix_millis >= 0),
    PRIMARY KEY (upstream_id, window, source, observed_at_unix_millis, sample_id)
);

CREATE INDEX IF NOT EXISTS upstream_subscription_quota_obs_series_idx
    ON upstream_subscription_quota_observations_v1
    (upstream_id, window, source, observed_at_unix_millis DESC);

CREATE INDEX IF NOT EXISTS upstream_subscription_quota_obs_gc_idx
    ON upstream_subscription_quota_observations_v1
    (observed_at_unix_millis);

CREATE TABLE IF NOT EXISTS upstream_subscription_quota_latest_v1 (
    upstream_id TEXT NOT NULL,
    window TEXT NOT NULL CHECK (window IN ('5h','7d','7d_sonnet','7d_opus','overage','unified')),
    source TEXT NOT NULL CHECK (source IN ('header','api')),
    sample_kind TEXT NOT NULL DEFAULT 'sample' CHECK (sample_kind IN ('sample','process_start')),
    observed_at_unix_millis INTEGER NOT NULL CHECK (observed_at_unix_millis >= 0),
    sample_id TEXT NOT NULL,
    utilization REAL CHECK (utilization IS NULL OR (utilization >= 0.0 AND utilization <= 1.0)),
    status TEXT CHECK (status IS NULL OR status IN ('allowed','allowed_warning','rejected')),
    resets_at_unix_secs INTEGER CHECK (resets_at_unix_secs IS NULL OR resets_at_unix_secs >= 0),
    surpassed_threshold INTEGER CHECK (surpassed_threshold IS NULL OR surpassed_threshold IN (0, 1)),
    representative_claim TEXT,
    disabled_reason TEXT,
    extra_usage_enabled INTEGER CHECK (extra_usage_enabled IS NULL OR extra_usage_enabled IN (0, 1)),
    extra_usage_monthly_limit REAL,
    extra_usage_used_credits REAL,
    ingested_at_unix_millis INTEGER NOT NULL CHECK (ingested_at_unix_millis >= 0),
    PRIMARY KEY (upstream_id, window, source)
);

CREATE INDEX IF NOT EXISTS upstream_subscription_quota_latest_lookup_idx
    ON upstream_subscription_quota_latest_v1
    (upstream_id, window, observed_at_unix_millis DESC);

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
