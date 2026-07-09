CREATE TABLE IF NOT EXISTS upstream_subscription_quota_checkpoints_v1 (
    upstream_id                 UUID NOT NULL,
    "window"                    TEXT NOT NULL CHECK ("window" IN ('5h','7d','7d_sonnet','7d_opus','overage','unified')),
    source                      TEXT NOT NULL CHECK (source IN ('header','api')),
    changed_at_unix_millis      BIGINT NOT NULL CHECK (changed_at_unix_millis >= 0),
    sample_id                   UUID NOT NULL,
    semantic_fingerprint        BYTEA NOT NULL CHECK (octet_length(semantic_fingerprint) = 32),
    sample_kind                 TEXT NOT NULL DEFAULT 'sample' CHECK (sample_kind IN ('sample','process_start')),
    representative_claim        TEXT,

    utilization                 DOUBLE PRECISION CHECK (utilization IS NULL OR (utilization >= 0.0 AND utilization <= 1.0)),
    status                      TEXT CHECK (status IS NULL OR status IN ('allowed','allowed_warning','rejected')),
    resets_at_unix_secs         BIGINT CHECK (resets_at_unix_secs IS NULL OR resets_at_unix_secs >= 0),
    surpassed_threshold         DOUBLE PRECISION CHECK (surpassed_threshold IS NULL OR (surpassed_threshold >= 0.0 AND surpassed_threshold <= 1.0)),
    fallback_percentage         DOUBLE PRECISION CHECK (fallback_percentage IS NULL OR (fallback_percentage >= 0.0 AND fallback_percentage <= 1.0)),
    fallback_available          BOOLEAN,
    overage_in_use              BOOLEAN,
    overage_period_monthly_utilization DOUBLE PRECISION
        CHECK (overage_period_monthly_utilization IS NULL OR (overage_period_monthly_utilization >= 0.0 AND overage_period_monthly_utilization <= 1.0)),
    upgrade_paths               TEXT,
    disabled_reason             TEXT,

    extra_usage_enabled         BOOLEAN,
    extra_usage_monthly_limit   DOUBLE PRECISION,
    extra_usage_used_credits    DOUBLE PRECISION,
    ingested_at_unix_millis     BIGINT NOT NULL CHECK (ingested_at_unix_millis >= 0),

    PRIMARY KEY (upstream_id, "window", source, changed_at_unix_millis, sample_id)
);

CREATE INDEX IF NOT EXISTS upstream_subscription_quota_checkpoints_range_idx
    ON upstream_subscription_quota_checkpoints_v1
    (upstream_id, "window", source, changed_at_unix_millis ASC, sample_id ASC);

CREATE INDEX IF NOT EXISTS upstream_subscription_quota_checkpoints_anchor_idx
    ON upstream_subscription_quota_checkpoints_v1
    (upstream_id, "window", source, changed_at_unix_millis DESC, sample_id DESC);
