ALTER TABLE upstream_subscription_quota_observations_v1
    ADD COLUMN IF NOT EXISTS fallback_percentage DOUBLE PRECISION CHECK (fallback_percentage IS NULL OR (fallback_percentage >= 0.0 AND fallback_percentage <= 1.0));

ALTER TABLE upstream_subscription_quota_latest_v1
    ADD COLUMN IF NOT EXISTS fallback_percentage DOUBLE PRECISION CHECK (fallback_percentage IS NULL OR (fallback_percentage >= 0.0 AND fallback_percentage <= 1.0));
