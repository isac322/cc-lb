-- Scoped read-order index for query_recent_audit (principal scope).
--
-- audit_log_v1_principal_seq (principal_id, seq) orders by seq ASC, so the
-- recent query's ORDER BY ts DESC, seq DESC fell back to a backward scan of
-- audit_log_v1_ts with principal_id as a filter (observed: ~20k index rows
-- scanned to return 200). Ordering by ts inside each principal serves the
-- ordering directly. The legacy index is kept: query_audit still uses
-- ORDER BY seq ASC and the principal delete check counts by principal_id.
--
-- Plain CREATE INDEX, not CONCURRENTLY: sqlx's database-wide migration
-- advisory lock serializes migrators and CONCURRENTLY deadlocks under it
-- (see 0085_quota_aggregate_covering_indexes.sql). One index per migration
-- file so each build's SHARE lock is released at its own commit instead of
-- being held across every index build in this set.
--
-- A startup CREATE INDEX holds a SHARE lock that blocks writes to
-- audit_log_v1 for the entire build; lock_timeout bounds only lock
-- acquisition, not the held lock. On a large existing table an operator
-- MUST pre-create this index with CREATE INDEX CONCURRENTLY using the
-- identical definition and verify pg_index.indisvalid before deploying
-- this migration; IF NOT EXISTS then makes this statement a no-op.
SET LOCAL lock_timeout = '1s';

CREATE INDEX IF NOT EXISTS audit_log_v1_principal_ts_seq
    ON audit_log_v1 (principal_id, ts DESC, seq DESC);
