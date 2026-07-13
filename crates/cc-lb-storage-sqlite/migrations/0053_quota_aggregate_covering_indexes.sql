CREATE INDEX IF NOT EXISTS upstream_subscription_quota_checkpoints_slim_idx
    ON upstream_subscription_quota_checkpoints_v1
    (upstream_id, window, source, changed_at_unix_millis ASC, sample_id ASC,
     utilization, status, resets_at_unix_secs);

CREATE INDEX IF NOT EXISTS usage_rollups_v2_token_interval_idx
    ON usage_rollups_v2
    (upstream_id, resolution, bucket_start_unix_secs ASC,
     input_tokens, output_tokens, cache_creation_input_tokens, cache_read_input_tokens);
