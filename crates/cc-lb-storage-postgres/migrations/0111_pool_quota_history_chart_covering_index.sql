-- Cover compact pool-history chart scans without fetching the wide history row.
-- Plain CREATE INDEX is intentional: sqlx's database-wide migration advisory lock
-- serializes migrations, so CREATE INDEX CONCURRENTLY cannot be used safely here.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '90s';

DROP INDEX IF EXISTS pool_subscription_quota_history_v1_window_time_idx;
CREATE INDEX pool_subscription_quota_history_v1_window_time_idx
    ON pool_subscription_quota_history_v1 (quota_window, snapshot_at_unix_secs DESC)
    INCLUDE (utilization);
