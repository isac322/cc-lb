-- Preserve the existing Anthropic table, keys, and indexes in place.
-- Other providers start collecting snapshots in an empty table.
SET LOCAL lock_timeout = '5s';

CREATE TABLE pool_subscription_quota_history_v2 (
    provider TEXT NOT NULL CHECK (provider IN ('openai','gemini')),
    snapshot_at_unix_secs BIGINT NOT NULL,
    quota_window TEXT NOT NULL CHECK (quota_window IN ('5h','7d','7d_sonnet','7d_opus','7d_fable','gemini_5h','gemini_7d','third_party_5h','third_party_7d','overage','unified')),
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
    computed_at_unix_millis BIGINT NOT NULL,
    policy_version INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (snapshot_at_unix_secs, provider, quota_window)
);

CREATE INDEX pool_subscription_quota_history_v2_window_time_idx
    ON pool_subscription_quota_history_v2 (provider, quota_window, snapshot_at_unix_secs DESC)
    INCLUDE (utilization);
