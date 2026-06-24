CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS warmup_attempts_v1 (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    upstream_id UUID NOT NULL REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    attempted_at_unix_secs BIGINT NOT NULL,
    completed_at_unix_secs BIGINT,
    scheduled_for_unix_secs BIGINT NOT NULL,
    trigger TEXT NOT NULL CHECK (trigger IN ('scheduled', 'manual')),
    outcome TEXT NOT NULL CHECK (outcome IN ('success_fresh', 'success_redundant', 'transient_failure', 'permanent_failure', 'skipped')),
    reason TEXT CHECK (reason IS NULL OR reason IN ('window_already_active', 'http_429_missing_cycle_key', 'upstream_5xx', 'network_error', 'request_timeout', 'request_build_failed', 'oauth_refresh_failed', 'credential_decrypt_failed', 'auth_failed', 'forbidden', 'bad_request', 'not_found', 'dialect_plugin_failed', 'dialect_plugin_transient', 'oauth_credentials_missing', 'lease_held', 'upstream_disabled', 'upstream_deleted')),
    http_status INTEGER CHECK (http_status IS NULL OR (http_status >= 100 AND http_status <= 599)),
    cycle_key BIGINT,
    expected_cycle_key BIGINT,
    idle_secs_since_prev_window BIGINT CHECK (idle_secs_since_prev_window IS NULL OR idle_secs_since_prev_window >= 0),
    replica_id UUID,
    lease_holder TEXT,
    upstream_spec_revision BIGINT NOT NULL,
    dialect_plugin_snapshot JSONB,
    error_detail TEXT,
    CHECK (completed_at_unix_secs IS NULL OR completed_at_unix_secs >= attempted_at_unix_secs)
);

CREATE INDEX IF NOT EXISTS idx_warmup_attempts_v1_upstream_attempted
    ON warmup_attempts_v1 (upstream_id, attempted_at_unix_secs DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_warmup_attempts_v1_upstream_trigger_scheduled
    ON warmup_attempts_v1 (upstream_id, trigger, scheduled_for_unix_secs DESC);

CREATE INDEX IF NOT EXISTS idx_warmup_attempts_v1_outcome_attempted
    ON warmup_attempts_v1 (outcome, attempted_at_unix_secs DESC);
