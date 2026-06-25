ALTER TABLE warmup_attempts_v1 DROP CONSTRAINT IF EXISTS warmup_attempts_v1_reason_check;
ALTER TABLE warmup_attempts_v1
    ADD CONSTRAINT warmup_attempts_v1_reason_check
    CHECK (reason IS NULL OR reason IN ('window_already_active', 'seven_day_quota_exhausted', 'http_429_missing_cycle_key', 'upstream_5xx', 'network_error', 'request_timeout', 'request_build_failed', 'oauth_refresh_failed', 'credential_decrypt_failed', 'auth_failed', 'forbidden', 'bad_request', 'not_found', 'dialect_plugin_failed', 'dialect_plugin_transient', 'oauth_credentials_missing', 'lease_held', 'upstream_disabled', 'upstream_deleted'));
