CREATE TABLE IF NOT EXISTS upstream_subscription_quota_checkpoints_v1 (
    upstream_id TEXT NOT NULL,
    window TEXT NOT NULL CHECK (window IN ('5h','7d','7d_sonnet','7d_opus','overage','unified')),
    source TEXT NOT NULL CHECK (source IN ('header','api')),
    changed_at_unix_millis INTEGER NOT NULL CHECK (changed_at_unix_millis >= 0),
    sample_id TEXT NOT NULL,
    semantic_fingerprint BLOB NOT NULL CHECK (length(semantic_fingerprint) = 32),
    sample_kind TEXT NOT NULL DEFAULT 'sample' CHECK (sample_kind IN ('sample','process_start')),
    representative_claim TEXT,
    utilization REAL CHECK (utilization IS NULL OR (utilization >= 0.0 AND utilization <= 1.0)),
    status TEXT CHECK (status IS NULL OR status IN ('allowed','allowed_warning','rejected')),
    resets_at_unix_secs INTEGER CHECK (resets_at_unix_secs IS NULL OR resets_at_unix_secs >= 0),
    surpassed_threshold REAL CHECK (surpassed_threshold IS NULL OR (surpassed_threshold >= 0.0 AND surpassed_threshold <= 1.0)),
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
    PRIMARY KEY (upstream_id, window, source, changed_at_unix_millis, sample_id)
);

CREATE INDEX IF NOT EXISTS upstream_subscription_quota_checkpoints_range_idx
    ON upstream_subscription_quota_checkpoints_v1
    (upstream_id, window, source, changed_at_unix_millis ASC, sample_id ASC);

CREATE INDEX IF NOT EXISTS upstream_subscription_quota_checkpoints_anchor_idx
    ON upstream_subscription_quota_checkpoints_v1
    (upstream_id, window, source, changed_at_unix_millis DESC, sample_id DESC);
