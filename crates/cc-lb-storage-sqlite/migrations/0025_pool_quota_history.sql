CREATE TABLE IF NOT EXISTS pool_subscription_quota_history_v1 (
    snapshot_at_unix_secs INTEGER NOT NULL,
    quota_window TEXT NOT NULL CHECK (quota_window IN ('5h','7d','7d_sonnet','7d_opus','overage','unified')),
    utilization REAL,
    weighted_utilization_sum REAL NOT NULL,
    capacity_ratio_sum REAL NOT NULL,
    eligible_upstreams INTEGER NOT NULL,
    contributing_upstreams INTEGER NOT NULL,
    stale_upstreams INTEGER NOT NULL,
    missing_observation_upstreams INTEGER NOT NULL,
    missing_metadata_upstreams INTEGER NOT NULL,
    header_contributing_upstreams INTEGER NOT NULL,
    api_contributing_upstreams INTEGER NOT NULL,
    max_observed_at_unix_millis INTEGER,
    contributors_json TEXT,
    computed_at_unix_millis INTEGER NOT NULL,
    policy_version INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (snapshot_at_unix_secs, quota_window)
);

CREATE INDEX IF NOT EXISTS pool_subscription_quota_history_v1_window_time_idx
    ON pool_subscription_quota_history_v1 (quota_window, snapshot_at_unix_secs DESC);
