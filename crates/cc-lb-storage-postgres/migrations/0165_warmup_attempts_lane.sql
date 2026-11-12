-- Warm-up lanes: each attempt records the quota group it warmed. Existing rows
-- belong to the primary lane. A constant DEFAULT makes ADD COLUMN metadata-only.
SET LOCAL lock_timeout = '1s';

ALTER TABLE warmup_attempts_v1
    ADD COLUMN IF NOT EXISTS lane TEXT NOT NULL DEFAULT 'primary';

ALTER TABLE warmup_attempts_v1 DROP CONSTRAINT IF EXISTS warmup_attempts_v1_lane_check;
ALTER TABLE warmup_attempts_v1
    ADD CONSTRAINT warmup_attempts_v1_lane_check
    CHECK (lane IN ('primary', 'third_party')) NOT VALID;

-- Skips for a lane whose quota group the account does not serve.
ALTER TABLE warmup_attempts_v1 DROP CONSTRAINT IF EXISTS warmup_attempts_v1_reason_check;
ALTER TABLE warmup_attempts_v1
    ADD CONSTRAINT warmup_attempts_v1_reason_check
    CHECK (reason IN ('cycle_advanced', 'window_already_active', 'seven_day_quota_exhausted', 'upstream_disabled', 'upstream_deleted', 'quota_group_unavailable', 'rate_limited_cycle_key_missing', 'upstream_5xx', 'network_error', 'request_timeout', 'dialect_plugin_transient', 'oauth_credentials_missing', 'request_build_failed', 'oauth_refresh_failed', 'credential_decrypt_failed', 'auth_failed', 'forbidden', 'bad_request', 'not_found', 'dialect_plugin_failed')) NOT VALID;

-- Validation runs in 0166, a separate transaction, so this migration holds
-- ACCESS EXCLUSIVE only for the metadata changes above.
