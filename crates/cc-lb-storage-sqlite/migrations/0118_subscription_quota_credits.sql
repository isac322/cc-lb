ALTER TABLE upstream_subscription_quota_latest_v1
    ADD COLUMN credits_json TEXT NULL;

ALTER TABLE upstream_subscription_quota_checkpoints_v1
    ADD COLUMN credits_json TEXT NULL;
