ALTER TABLE upstream_subscription_quota_observations_v1
    ADD COLUMN IF NOT EXISTS fallback_available BOOLEAN,
    ADD COLUMN IF NOT EXISTS overage_in_use BOOLEAN,
    ADD COLUMN IF NOT EXISTS overage_period_monthly_utilization DOUBLE PRECISION
        CHECK (overage_period_monthly_utilization IS NULL OR (overage_period_monthly_utilization >= 0.0 AND overage_period_monthly_utilization <= 1.0)),
    ADD COLUMN IF NOT EXISTS upgrade_paths TEXT;

ALTER TABLE upstream_subscription_quota_observations_v1
    ALTER COLUMN surpassed_threshold TYPE DOUBLE PRECISION
        USING (CASE WHEN surpassed_threshold IS TRUE THEN 1.0
                    WHEN surpassed_threshold IS FALSE THEN 0.0
                    ELSE NULL END),
    ADD CONSTRAINT upstream_subscription_quota_obs_surpassed_threshold_range
        CHECK (surpassed_threshold IS NULL OR (surpassed_threshold >= 0.0 AND surpassed_threshold <= 1.0));

ALTER TABLE upstream_subscription_quota_latest_v1
    ADD COLUMN IF NOT EXISTS fallback_available BOOLEAN,
    ADD COLUMN IF NOT EXISTS overage_in_use BOOLEAN,
    ADD COLUMN IF NOT EXISTS overage_period_monthly_utilization DOUBLE PRECISION
        CHECK (overage_period_monthly_utilization IS NULL OR (overage_period_monthly_utilization >= 0.0 AND overage_period_monthly_utilization <= 1.0)),
    ADD COLUMN IF NOT EXISTS upgrade_paths TEXT;

ALTER TABLE upstream_subscription_quota_latest_v1
    ALTER COLUMN surpassed_threshold TYPE DOUBLE PRECISION
        USING (CASE WHEN surpassed_threshold IS TRUE THEN 1.0
                    WHEN surpassed_threshold IS FALSE THEN 0.0
                    ELSE NULL END),
    ADD CONSTRAINT upstream_subscription_quota_latest_surpassed_threshold_range
        CHECK (surpassed_threshold IS NULL OR (surpassed_threshold >= 0.0 AND surpassed_threshold <= 1.0));
