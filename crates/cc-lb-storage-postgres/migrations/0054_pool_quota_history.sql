CREATE TABLE IF NOT EXISTS pool_subscription_quota_history_v1 (
    snapshot_at_unix_secs BIGINT NOT NULL,
    quota_window TEXT NOT NULL CHECK (quota_window IN ('5h','7d','7d_sonnet','7d_opus','overage','unified')),
    utilization DOUBLE PRECISION,
    weighted_utilization_sum DOUBLE PRECISION NOT NULL,
    capacity_ratio_sum DOUBLE PRECISION NOT NULL,
    eligible_upstreams BIGINT NOT NULL,
    contributing_upstreams BIGINT NOT NULL,
    stale_upstreams BIGINT NOT NULL,
    missing_observation_upstreams BIGINT NOT NULL,
    missing_metadata_upstreams BIGINT NOT NULL,
    header_contributing_upstreams BIGINT NOT NULL,
    api_contributing_upstreams BIGINT NOT NULL,
    max_observed_at_unix_millis BIGINT,
    contributors_json TEXT,
    computed_at_unix_millis BIGINT NOT NULL,
    policy_version INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (snapshot_at_unix_secs, quota_window)
);

CREATE INDEX IF NOT EXISTS pool_subscription_quota_history_v1_window_time_idx
    ON pool_subscription_quota_history_v1 (quota_window, snapshot_at_unix_secs DESC);
