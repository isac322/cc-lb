-- Plain CREATE INDEX, not CONCURRENTLY: see 0085_quota_aggregate_covering_indexes.sql
-- for why CONCURRENTLY + sqlx's database-wide migration advisory lock
-- deadlocks under concurrent migrators.
CREATE INDEX IF NOT EXISTS usage_rollups_v2_interval_tokens_cover_idx
    ON usage_rollups_v2 (upstream_id, bucket_start_unix_secs)
    INCLUDE (
        input_tokens,
        output_tokens,
        cache_creation_input_tokens,
        cache_read_input_tokens
    )
    WHERE resolution = 'minute';
