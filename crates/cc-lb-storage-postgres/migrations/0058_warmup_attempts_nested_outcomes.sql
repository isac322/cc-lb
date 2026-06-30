ALTER TABLE warmup_attempts_v1 ADD COLUMN IF NOT EXISTS dispatch_kind TEXT;

ALTER TABLE warmup_attempts_v1 DROP CONSTRAINT IF EXISTS warmup_attempts_v1_outcome_check;
ALTER TABLE warmup_attempts_v1 DROP CONSTRAINT IF EXISTS warmup_attempts_v1_reason_check;
ALTER TABLE warmup_attempts_v1 DROP CONSTRAINT IF EXISTS warmup_attempts_v1_dispatch_kind_check;

UPDATE warmup_attempts_v1
SET
    outcome = CASE
        WHEN outcome IN ('success_fresh', 'success_redundant') THEN 'success'
        WHEN outcome = 'skipped' AND reason = 'window_already_active' THEN 'success'
        WHEN outcome = 'skipped' AND reason = 'oauth_credentials_missing' THEN 'permanent_failure'
        ELSE outcome
    END,
    reason = CASE
        WHEN outcome = 'success_fresh' THEN 'cycle_advanced'
        WHEN outcome = 'success_redundant' THEN 'window_already_active'
        WHEN outcome = 'skipped' AND reason = 'window_already_active' THEN 'window_already_active'
        WHEN outcome = 'skipped' AND reason = 'seven_day_quota_exhausted' THEN 'seven_day_quota_exhausted'
        WHEN outcome = 'skipped' AND reason = 'upstream_disabled' THEN 'upstream_disabled'
        WHEN outcome = 'skipped' AND reason = 'upstream_deleted' THEN 'upstream_deleted'
        WHEN outcome = 'skipped' AND reason = 'oauth_credentials_missing' THEN 'oauth_credentials_missing'
        WHEN outcome = 'skipped' AND reason = 'lease_held' THEN 'upstream_disabled'
        WHEN outcome = 'transient_failure' AND reason = 'http_429_missing_cycle_key' THEN 'rate_limited_cycle_key_missing'
        ELSE reason
    END;

ALTER TABLE warmup_attempts_v1 ALTER COLUMN reason SET NOT NULL;

ALTER TABLE warmup_attempts_v1
    ADD CONSTRAINT warmup_attempts_v1_outcome_check
    CHECK (outcome IN ('success', 'skipped', 'transient_failure', 'permanent_failure'));

ALTER TABLE warmup_attempts_v1
    ADD CONSTRAINT warmup_attempts_v1_reason_check
    CHECK (reason IN ('cycle_advanced', 'window_already_active', 'seven_day_quota_exhausted', 'upstream_disabled', 'upstream_deleted', 'rate_limited_cycle_key_missing', 'upstream_5xx', 'network_error', 'request_timeout', 'dialect_plugin_transient', 'oauth_credentials_missing', 'request_build_failed', 'oauth_refresh_failed', 'credential_decrypt_failed', 'auth_failed', 'forbidden', 'bad_request', 'not_found', 'dialect_plugin_failed'));

ALTER TABLE warmup_attempts_v1
    ADD CONSTRAINT warmup_attempts_v1_dispatch_kind_check
    CHECK (dispatch_kind IS NULL OR dispatch_kind IN ('not_dispatched', 'http', 'dialect_plugin'));
