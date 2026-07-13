-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS upstream_subscription_quota_checkpoints_slim_cover_idx
    ON upstream_subscription_quota_checkpoints_v1
    (upstream_id, "window", source, changed_at_unix_millis ASC, sample_id ASC)
    INCLUDE (utilization, status, resets_at_unix_secs);
