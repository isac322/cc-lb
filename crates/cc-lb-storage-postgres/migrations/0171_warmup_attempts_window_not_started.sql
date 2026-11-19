-- Allow Gemini warm-up verification failures to record an unstarted quota window.
SET LOCAL lock_timeout = '1s';

ALTER TABLE warmup_attempts_v1 DROP CONSTRAINT IF EXISTS warmup_attempts_v1_reason_check;
ALTER TABLE warmup_attempts_v1
    ADD CONSTRAINT warmup_attempts_v1_reason_check
    CHECK (reason IN ('cycle_advanced', 'window_already_active', 'seven_day_quota_exhausted', 'upstream_disabled', 'upstream_deleted', 'quota_group_unavailable', 'rate_limited_cycle_key_missing', 'upstream_5xx', 'network_error', 'request_timeout', 'dialect_plugin_transient', 'oauth_credentials_missing', 'request_build_failed', 'oauth_refresh_failed', 'credential_decrypt_failed', 'auth_failed', 'forbidden', 'bad_request', 'not_found', 'dialect_plugin_failed', 'window_not_started')) NOT VALID;
