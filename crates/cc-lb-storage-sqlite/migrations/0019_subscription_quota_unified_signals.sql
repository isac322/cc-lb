CREATE TABLE upstream_subscription_quota_observations_v1_new (
    upstream_id TEXT NOT NULL,
    window TEXT NOT NULL CHECK (window IN ('5h','7d','7d_sonnet','7d_opus','overage','unified')),
    source TEXT NOT NULL CHECK (source IN ('header','api')),
    sample_kind TEXT NOT NULL DEFAULT 'sample' CHECK (sample_kind IN ('sample','process_start')),
    observed_at_unix_millis INTEGER NOT NULL CHECK (observed_at_unix_millis >= 0),
    sample_id TEXT NOT NULL,
    utilization REAL CHECK (utilization IS NULL OR (utilization >= 0.0 AND utilization <= 1.0)),
    status TEXT CHECK (status IS NULL OR status IN ('allowed','allowed_warning','rejected')),
    resets_at_unix_secs INTEGER CHECK (resets_at_unix_secs IS NULL OR resets_at_unix_secs >= 0),
    surpassed_threshold REAL CHECK (surpassed_threshold IS NULL OR (surpassed_threshold >= 0.0 AND surpassed_threshold <= 1.0)),
    representative_claim TEXT,
    fallback_percentage REAL CHECK (fallback_percentage IS NULL OR (fallback_percentage >= 0.0 AND fallback_percentage <= 1.0)),
    fallback_available INTEGER CHECK (fallback_available IS NULL OR fallback_available IN (0, 1)),
    overage_in_use INTEGER CHECK (overage_in_use IS NULL OR overage_in_use IN (0, 1)),
    overage_period_monthly_utilization REAL CHECK (overage_period_monthly_utilization IS NULL OR (overage_period_monthly_utilization >= 0.0 AND overage_period_monthly_utilization <= 1.0)),
    upgrade_paths TEXT,
    disabled_reason TEXT,
    extra_usage_enabled INTEGER CHECK (extra_usage_enabled IS NULL OR extra_usage_enabled IN (0, 1)),
    extra_usage_monthly_limit REAL,
    extra_usage_used_credits REAL,
    ingested_at_unix_millis INTEGER NOT NULL CHECK (ingested_at_unix_millis >= 0),
    PRIMARY KEY (upstream_id, window, source, observed_at_unix_millis, sample_id)
);

INSERT INTO upstream_subscription_quota_observations_v1_new
    (upstream_id, window, source, sample_kind, observed_at_unix_millis, sample_id,
     utilization, status, resets_at_unix_secs, surpassed_threshold, representative_claim,
     fallback_percentage, fallback_available, overage_in_use, overage_period_monthly_utilization, upgrade_paths,
     disabled_reason, extra_usage_enabled, extra_usage_monthly_limit, extra_usage_used_credits, ingested_at_unix_millis)
SELECT
    upstream_id, window, source, sample_kind, observed_at_unix_millis, sample_id,
    utilization, status, resets_at_unix_secs,
    CASE WHEN surpassed_threshold = 1 THEN 1.0
         WHEN surpassed_threshold = 0 THEN 0.0
         ELSE NULL END,
    representative_claim,
    fallback_percentage, NULL, NULL, NULL, NULL,
    disabled_reason, extra_usage_enabled, extra_usage_monthly_limit, extra_usage_used_credits, ingested_at_unix_millis
FROM upstream_subscription_quota_observations_v1;

DROP TABLE upstream_subscription_quota_observations_v1;
ALTER TABLE upstream_subscription_quota_observations_v1_new RENAME TO upstream_subscription_quota_observations_v1;

CREATE INDEX IF NOT EXISTS upstream_subscription_quota_obs_series_idx
    ON upstream_subscription_quota_observations_v1
    (upstream_id, window, source, observed_at_unix_millis DESC);

CREATE INDEX IF NOT EXISTS upstream_subscription_quota_obs_gc_idx
    ON upstream_subscription_quota_observations_v1
    (observed_at_unix_millis);

CREATE TABLE upstream_subscription_quota_latest_v1_new (
    upstream_id TEXT NOT NULL,
    window TEXT NOT NULL CHECK (window IN ('5h','7d','7d_sonnet','7d_opus','overage','unified')),
    source TEXT NOT NULL CHECK (source IN ('header','api')),
    sample_kind TEXT NOT NULL DEFAULT 'sample' CHECK (sample_kind IN ('sample','process_start')),
    observed_at_unix_millis INTEGER NOT NULL CHECK (observed_at_unix_millis >= 0),
    sample_id TEXT NOT NULL,
    utilization REAL CHECK (utilization IS NULL OR (utilization >= 0.0 AND utilization <= 1.0)),
    status TEXT CHECK (status IS NULL OR status IN ('allowed','allowed_warning','rejected')),
    resets_at_unix_secs INTEGER CHECK (resets_at_unix_secs IS NULL OR resets_at_unix_secs >= 0),
    surpassed_threshold REAL CHECK (surpassed_threshold IS NULL OR (surpassed_threshold >= 0.0 AND surpassed_threshold <= 1.0)),
    representative_claim TEXT,
    fallback_percentage REAL CHECK (fallback_percentage IS NULL OR (fallback_percentage >= 0.0 AND fallback_percentage <= 1.0)),
    fallback_available INTEGER CHECK (fallback_available IS NULL OR fallback_available IN (0, 1)),
    overage_in_use INTEGER CHECK (overage_in_use IS NULL OR overage_in_use IN (0, 1)),
    overage_period_monthly_utilization REAL CHECK (overage_period_monthly_utilization IS NULL OR (overage_period_monthly_utilization >= 0.0 AND overage_period_monthly_utilization <= 1.0)),
    upgrade_paths TEXT,
    disabled_reason TEXT,
    extra_usage_enabled INTEGER CHECK (extra_usage_enabled IS NULL OR extra_usage_enabled IN (0, 1)),
    extra_usage_monthly_limit REAL,
    extra_usage_used_credits REAL,
    ingested_at_unix_millis INTEGER NOT NULL CHECK (ingested_at_unix_millis >= 0),
    PRIMARY KEY (upstream_id, window, source)
);

INSERT INTO upstream_subscription_quota_latest_v1_new
    (upstream_id, window, source, sample_kind, observed_at_unix_millis, sample_id,
     utilization, status, resets_at_unix_secs, surpassed_threshold, representative_claim,
     fallback_percentage, fallback_available, overage_in_use, overage_period_monthly_utilization, upgrade_paths,
     disabled_reason, extra_usage_enabled, extra_usage_monthly_limit, extra_usage_used_credits, ingested_at_unix_millis)
SELECT
    upstream_id, window, source, sample_kind, observed_at_unix_millis, sample_id,
    utilization, status, resets_at_unix_secs,
    CASE WHEN surpassed_threshold = 1 THEN 1.0
         WHEN surpassed_threshold = 0 THEN 0.0
         ELSE NULL END,
    representative_claim,
    fallback_percentage, NULL, NULL, NULL, NULL,
    disabled_reason, extra_usage_enabled, extra_usage_monthly_limit, extra_usage_used_credits, ingested_at_unix_millis
FROM upstream_subscription_quota_latest_v1;

DROP TABLE upstream_subscription_quota_latest_v1;
ALTER TABLE upstream_subscription_quota_latest_v1_new RENAME TO upstream_subscription_quota_latest_v1;

CREATE INDEX IF NOT EXISTS upstream_subscription_quota_latest_lookup_idx
    ON upstream_subscription_quota_latest_v1
    (upstream_id, window, observed_at_unix_millis DESC);
