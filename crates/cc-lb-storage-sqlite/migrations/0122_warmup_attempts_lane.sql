-- Warm-up lanes: each attempt records the quota group it warmed, and lanes can
-- skip with quota_group_unavailable. SQLite cannot alter the inline reason
-- CHECK, so rebuild the table. Every existing row belongs to the primary lane.
CREATE TABLE warmup_attempts_v1_lane (
    id TEXT PRIMARY KEY,
    upstream_id TEXT NOT NULL REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    attempted_at_unix_secs INTEGER NOT NULL,
    completed_at_unix_secs INTEGER,
    scheduled_for_unix_secs INTEGER NOT NULL,
    trigger TEXT NOT NULL CHECK (trigger IN ('scheduled','manual')),
    lane TEXT NOT NULL DEFAULT 'primary' CHECK (lane IN ('primary','third_party')),
    outcome TEXT NOT NULL CHECK (outcome IN ('success','skipped','transient_failure','permanent_failure')),
    reason TEXT NOT NULL CHECK (reason IN ('cycle_advanced','window_already_active','seven_day_quota_exhausted','upstream_disabled','upstream_deleted','quota_group_unavailable','rate_limited_cycle_key_missing','upstream_5xx','network_error','request_timeout','dialect_plugin_transient','oauth_credentials_missing','request_build_failed','oauth_refresh_failed','credential_decrypt_failed','auth_failed','forbidden','bad_request','not_found','dialect_plugin_failed')),
    dispatch_kind TEXT CHECK (dispatch_kind IS NULL OR dispatch_kind IN ('not_dispatched','http','dialect_plugin')),
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

INSERT INTO warmup_attempts_v1_lane (
    id, upstream_id, attempted_at_unix_secs, completed_at_unix_secs,
    scheduled_for_unix_secs, trigger, lane, outcome, reason, dispatch_kind,
    http_status, cycle_key, expected_cycle_key, idle_secs_since_prev_window,
    replica_id, lease_holder, upstream_spec_revision, dialect_plugin_snapshot,
    error_detail
)
SELECT id, upstream_id, attempted_at_unix_secs, completed_at_unix_secs,
       scheduled_for_unix_secs, trigger, 'primary', outcome, reason, dispatch_kind,
       http_status, cycle_key, expected_cycle_key, idle_secs_since_prev_window,
       replica_id, lease_holder, upstream_spec_revision, dialect_plugin_snapshot,
       error_detail
FROM warmup_attempts_v1;

DROP TABLE warmup_attempts_v1;
ALTER TABLE warmup_attempts_v1_lane RENAME TO warmup_attempts_v1;
CREATE INDEX idx_warmup_attempts_v1_upstream_attempted
    ON warmup_attempts_v1(upstream_id, attempted_at_unix_secs DESC, id DESC);
