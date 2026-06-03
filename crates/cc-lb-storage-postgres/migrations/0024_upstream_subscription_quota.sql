CREATE TABLE IF NOT EXISTS upstream_subscription_quota_observations_v1 (
    upstream_id                 UUID NOT NULL,
    "window"                    TEXT NOT NULL CHECK ("window" IN ('5h','7d','7d_sonnet','7d_opus','overage','unified')),
    source                      TEXT NOT NULL CHECK (source IN ('header','api')),
    sample_kind                 TEXT NOT NULL DEFAULT 'sample' CHECK (sample_kind IN ('sample','process_start')),
    observed_at_unix_millis     BIGINT NOT NULL CHECK (observed_at_unix_millis >= 0),
    sample_id                   UUID NOT NULL,

    utilization                 DOUBLE PRECISION CHECK (utilization IS NULL OR (utilization >= 0.0 AND utilization <= 1.0)),
    status                      TEXT CHECK (status IS NULL OR status IN ('allowed','allowed_warning','rejected')),
    resets_at_unix_secs         BIGINT CHECK (resets_at_unix_secs IS NULL OR resets_at_unix_secs >= 0),
    surpassed_threshold         BOOLEAN,
    representative_claim        TEXT,
    disabled_reason             TEXT,

    extra_usage_enabled         BOOLEAN,
    extra_usage_monthly_limit   DOUBLE PRECISION,
    extra_usage_used_credits    DOUBLE PRECISION,

    ingested_at_unix_millis     BIGINT NOT NULL CHECK (ingested_at_unix_millis >= 0),

    PRIMARY KEY (upstream_id, "window", source, observed_at_unix_millis, sample_id)
);

CREATE INDEX IF NOT EXISTS upstream_subscription_quota_obs_series_idx
    ON upstream_subscription_quota_observations_v1
    (upstream_id, "window", source, observed_at_unix_millis DESC);

CREATE INDEX IF NOT EXISTS upstream_subscription_quota_obs_gc_idx
    ON upstream_subscription_quota_observations_v1
    (observed_at_unix_millis);

CREATE TABLE IF NOT EXISTS upstream_subscription_quota_latest_v1 (
    upstream_id                 UUID NOT NULL,
    "window"                    TEXT NOT NULL CHECK ("window" IN ('5h','7d','7d_sonnet','7d_opus','overage','unified')),
    source                      TEXT NOT NULL CHECK (source IN ('header','api')),
    sample_kind                 TEXT NOT NULL DEFAULT 'sample' CHECK (sample_kind IN ('sample','process_start')),
    observed_at_unix_millis     BIGINT NOT NULL CHECK (observed_at_unix_millis >= 0),
    sample_id                   UUID NOT NULL,

    utilization                 DOUBLE PRECISION CHECK (utilization IS NULL OR (utilization >= 0.0 AND utilization <= 1.0)),
    status                      TEXT CHECK (status IS NULL OR status IN ('allowed','allowed_warning','rejected')),
    resets_at_unix_secs         BIGINT CHECK (resets_at_unix_secs IS NULL OR resets_at_unix_secs >= 0),
    surpassed_threshold         BOOLEAN,
    representative_claim        TEXT,
    disabled_reason             TEXT,

    extra_usage_enabled         BOOLEAN,
    extra_usage_monthly_limit   DOUBLE PRECISION,
    extra_usage_used_credits    DOUBLE PRECISION,

    ingested_at_unix_millis     BIGINT NOT NULL CHECK (ingested_at_unix_millis >= 0),

    PRIMARY KEY (upstream_id, "window", source)
);

CREATE INDEX IF NOT EXISTS upstream_subscription_quota_latest_lookup_idx
    ON upstream_subscription_quota_latest_v1
    (upstream_id, "window", observed_at_unix_millis DESC);
