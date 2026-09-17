-- Partial read-order index for query_recent_audit with admin_only=true,
-- principal scope. The query carries the literal predicate
-- (admin_action IS NOT NULL OR kind IS NOT NULL); the identical WHERE clause
-- makes this partial index applicable. This is the admin audit page's primary
-- scoped path, so it gets a dedicated index rather than filtering admin rows
-- out of audit_log_v1_admin_ts_seq.
--
-- Plain CREATE INDEX, not CONCURRENTLY: see
-- 0120_audit_log_principal_ts_seq_index.sql for the advisory-lock rationale,
-- the one-index-per-file split, and the operator pre-create requirement for
-- large tables.
SET LOCAL lock_timeout = '1s';

CREATE INDEX IF NOT EXISTS audit_log_v1_admin_principal_ts_seq
    ON audit_log_v1 (principal_id, ts DESC, seq DESC)
    WHERE admin_action IS NOT NULL OR kind IS NOT NULL;
