-- Keep sqlite's wider audit columns (api_key_id, cost_usd_micros,
-- limit_violation, admin_action, actor). Postgres migration 0045 adds the
-- same trait-surface columns so the backend schemas intentionally converge.
ALTER TABLE audit_entries_v1 RENAME TO audit_log_v1;

CREATE INDEX IF NOT EXISTS idx_audit_log_principal_seq
    ON audit_log_v1 (principal_id, id);
CREATE INDEX IF NOT EXISTS idx_audit_log_ts
    ON audit_log_v1 (ts);
