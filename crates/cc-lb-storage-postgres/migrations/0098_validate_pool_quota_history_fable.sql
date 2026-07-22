SET LOCAL lock_timeout = '1s';

ALTER TABLE pool_subscription_quota_history_v1
    VALIDATE CONSTRAINT pool_subscription_quota_history_v1_quota_window_check_fable;

ALTER TABLE pool_subscription_quota_history_v1
    DROP CONSTRAINT pool_subscription_quota_history_v1_quota_window_check;
