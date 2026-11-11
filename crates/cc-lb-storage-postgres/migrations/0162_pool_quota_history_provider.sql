-- ADD COLUMN with a constant default preserves legacy Anthropic rows through
-- PostgreSQL's metadata-only missing-value mechanism, without a data backfill.
-- The history table is retention-bounded; its two replacement indexes must be
-- rehearsed within the normal startup migration timeout before release.
SET LOCAL lock_timeout = '5s';

ALTER TABLE pool_subscription_quota_history_v1
    ADD COLUMN provider TEXT NOT NULL DEFAULT 'anthropic';
ALTER TABLE pool_subscription_quota_history_v1
    ADD CONSTRAINT pool_subscription_quota_history_v1_provider_check
    CHECK (provider IN ('anthropic','openai','gemini')) NOT VALID;
ALTER TABLE pool_subscription_quota_history_v1
    VALIDATE CONSTRAINT pool_subscription_quota_history_v1_provider_check;
ALTER TABLE pool_subscription_quota_history_v1
    ALTER COLUMN provider DROP DEFAULT;
ALTER TABLE pool_subscription_quota_history_v1
    DROP CONSTRAINT pool_subscription_quota_history_v1_pkey;
ALTER TABLE pool_subscription_quota_history_v1
    ADD PRIMARY KEY (provider, snapshot_at_unix_secs, quota_window);

DROP INDEX pool_subscription_quota_history_v1_window_time_idx;
CREATE INDEX pool_subscription_quota_history_v1_window_time_idx
    ON pool_subscription_quota_history_v1 (provider, quota_window, snapshot_at_unix_secs DESC)
    INCLUDE (utilization);
