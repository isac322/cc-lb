SET LOCAL lock_timeout = '1s';

ALTER TABLE pool_subscription_quota_history_v1
    ADD CONSTRAINT pool_subscription_quota_history_v1_quota_window_check_fable
    CHECK (quota_window IN ('5h','7d','7d_sonnet','7d_opus','7d_fable','overage','unified'))
    NOT VALID;
