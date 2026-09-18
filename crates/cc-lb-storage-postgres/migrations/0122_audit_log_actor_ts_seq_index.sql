-- Scoped read-order index for query_recent_audit (actor scope).
--
-- audit_log_v1_actor_subject (actor_authority, actor_subject, seq) orders by
-- seq ASC, so the recent query's ORDER BY ts DESC, seq DESC needed a sort or
-- an unscoped ts scan. Ordering by ts inside each actor serves the ordering
-- directly. The legacy index is kept: query_audit_by_actor still uses
-- ORDER BY seq ASC.
--
-- Plain CREATE INDEX, not CONCURRENTLY: see
-- 0126_audit_log_principal_ts_seq_index.sql for the advisory-lock rationale,
-- the one-index-per-file split, and the operator pre-create requirement for
-- large tables.
SET LOCAL lock_timeout = '1s';
-- actor_authority = $n implies a non-NULL authority, so this index only
-- covers rows that can match an actor-scoped lookup.
CREATE INDEX IF NOT EXISTS audit_log_v1_actor_ts_seq
    ON audit_log_v1 (actor_authority, actor_subject, ts DESC, seq DESC)
    WHERE actor_authority IS NOT NULL;
