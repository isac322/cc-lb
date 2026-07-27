ALTER TABLE upstream_subscription_quota_latest_v1
    DROP CONSTRAINT upstream_subscription_quota_latest_v1_sample_kind_check;

ALTER TABLE upstream_subscription_quota_latest_v1
    ADD CONSTRAINT upstream_subscription_quota_latest_v1_sample_kind_check
    CHECK (sample_kind IN ('sample','absent','process_start'));
