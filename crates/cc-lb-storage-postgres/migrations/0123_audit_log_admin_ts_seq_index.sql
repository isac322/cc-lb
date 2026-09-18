-- Partial read-order index for query_recent_audit with admin_only=true
-- (unscoped). The query carries the literal predicate
-- (admin_action IS NOT NULL OR kind IS NOT NULL), so a partial index with the
-- identical WHERE clause applies.
--
-- The unscoped non-admin query keeps using audit_log_v1_ts (ts): a backward
-- scan plus incremental sort on seq covers ORDER BY ts DESC, seq DESC, so a
-- full (ts, seq) index would duplicate an existing read path.
--
-- Plain CREATE INDEX, not CONCURRENTLY: see
-- 0121_audit_log_principal_ts_seq_index.sql for the advisory-lock rationale,
-- the one-index-per-file split, and the operator pre-create requirement for
-- large tables.
SET LOCAL lock_timeout = '1s';

CREATE INDEX IF NOT EXISTS audit_log_v1_admin_ts_seq
    ON audit_log_v1 (ts DESC, seq DESC)
    WHERE admin_action IS NOT NULL OR kind IS NOT NULL;
