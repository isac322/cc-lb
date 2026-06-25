PRAGMA foreign_keys = OFF;

CREATE TABLE warmup_attempts_v1_new (
    id TEXT PRIMARY KEY,
    upstream_id TEXT NOT NULL REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    attempted_at_unix_secs INTEGER NOT NULL,
    completed_at_unix_secs INTEGER,
    scheduled_for_unix_secs INTEGER NOT NULL,
    trigger TEXT NOT NULL CHECK (trigger IN ('scheduled','manual')),
    outcome TEXT NOT NULL CHECK (outcome IN ('success_fresh','success_redundant','transient_failure','permanent_failure','skipped')),
    reason TEXT CHECK (reason IS NULL OR reason IN ('window_already_active','seven_day_quota_exhausted','http_429_missing_cycle_key','upstream_5xx','network_error','request_timeout','request_build_failed','oauth_refresh_failed','credential_decrypt_failed','auth_failed','forbidden','bad_request','not_found','dialect_plugin_failed','dialect_plugin_transient','oauth_credentials_missing','lease_held','upstream_disabled','upstream_deleted')),
    http_status INTEGER CHECK (http_status IS NULL OR (http_status >= 100 AND http_status <= 599)),
    cycle_key INTEGER,
    expected_cycle_key INTEGER,
    idle_secs_since_prev_window INTEGER CHECK (idle_secs_since_prev_window IS NULL OR idle_secs_since_prev_window >= 0),
    replica_id TEXT,
    lease_holder TEXT,
    upstream_spec_revision INTEGER NOT NULL,
    dialect_plugin_snapshot TEXT,
    error_detail TEXT,
    CHECK (completed_at_unix_secs IS NULL OR completed_at_unix_secs >= attempted_at_unix_secs)
);

INSERT INTO warmup_attempts_v1_new (
    id,
    upstream_id,
    attempted_at_unix_secs,
    completed_at_unix_secs,
    scheduled_for_unix_secs,
    trigger,
    outcome,
    reason,
    http_status,
    cycle_key,
    expected_cycle_key,
    idle_secs_since_prev_window,
    replica_id,
    lease_holder,
    upstream_spec_revision,
    dialect_plugin_snapshot,
    error_detail
)
SELECT
    id,
    upstream_id,
    attempted_at_unix_secs,
    completed_at_unix_secs,
    scheduled_for_unix_secs,
    trigger,
    outcome,
    reason,
    http_status,
    cycle_key,
    expected_cycle_key,
    idle_secs_since_prev_window,
    replica_id,
    lease_holder,
    upstream_spec_revision,
    dialect_plugin_snapshot,
    error_detail
FROM warmup_attempts_v1;

DROP TABLE warmup_attempts_v1;
ALTER TABLE warmup_attempts_v1_new RENAME TO warmup_attempts_v1;

CREATE INDEX idx_warmup_attempts_v1_upstream_attempted ON warmup_attempts_v1(upstream_id, attempted_at_unix_secs DESC, id DESC);
CREATE INDEX idx_warmup_attempts_v1_upstream_trigger_scheduled ON warmup_attempts_v1(upstream_id, trigger, scheduled_for_unix_secs DESC);
CREATE INDEX idx_warmup_attempts_v1_outcome_attempted ON warmup_attempts_v1(outcome, attempted_at_unix_secs DESC);

PRAGMA foreign_keys = ON;
