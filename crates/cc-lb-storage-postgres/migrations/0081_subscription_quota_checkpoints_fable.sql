ALTER TABLE upstream_subscription_quota_latest_v1
    DROP CONSTRAINT upstream_subscription_quota_latest_v1_window_check;

ALTER TABLE upstream_subscription_quota_latest_v1
    ADD CONSTRAINT upstream_subscription_quota_latest_v1_window_check
    CHECK ("window" IN ('5h','7d','7d_sonnet','7d_opus','7d_fable','overage','unified'));

ALTER TABLE upstream_subscription_quota_checkpoints_v1
    DROP CONSTRAINT upstream_subscription_quota_checkpoints_v1_window_check;

ALTER TABLE upstream_subscription_quota_checkpoints_v1
    ADD CONSTRAINT upstream_subscription_quota_checkpoints_v1_window_check
    CHECK ("window" IN ('5h','7d','7d_sonnet','7d_opus','7d_fable','overage','unified'));
