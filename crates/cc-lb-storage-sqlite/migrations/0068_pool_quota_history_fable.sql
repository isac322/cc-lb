CREATE TABLE pool_subscription_quota_history_v1_fable (
    snapshot_at_unix_secs INTEGER NOT NULL,
    quota_window TEXT NOT NULL CHECK (quota_window IN ('5h','7d','7d_sonnet','7d_opus','7d_fable','overage','unified')),
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
    computed_at_unix_millis INTEGER NOT NULL,
    policy_version INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (snapshot_at_unix_secs, quota_window)
);

INSERT INTO pool_subscription_quota_history_v1_fable (
    snapshot_at_unix_secs,
    quota_window,
    utilization,
    weighted_utilization_sum,
    capacity_ratio_sum,
    eligible_upstreams,
    contributing_upstreams,
    stale_upstreams,
    missing_observation_upstreams,
    missing_metadata_upstreams,
    header_contributing_upstreams,
    api_contributing_upstreams,
    max_observed_at_unix_millis,
    computed_at_unix_millis,
    policy_version
)
SELECT
    snapshot_at_unix_secs,
    quota_window,
    utilization,
    weighted_utilization_sum,
    capacity_ratio_sum,
    eligible_upstreams,
    contributing_upstreams,
    stale_upstreams,
    missing_observation_upstreams,
    missing_metadata_upstreams,
    header_contributing_upstreams,
    api_contributing_upstreams,
    max_observed_at_unix_millis,
    computed_at_unix_millis,
    policy_version
FROM pool_subscription_quota_history_v1;

DROP TABLE pool_subscription_quota_history_v1;

ALTER TABLE pool_subscription_quota_history_v1_fable
    RENAME TO pool_subscription_quota_history_v1;

CREATE INDEX pool_subscription_quota_history_v1_window_time_idx
    ON pool_subscription_quota_history_v1 (quota_window, snapshot_at_unix_secs DESC);
