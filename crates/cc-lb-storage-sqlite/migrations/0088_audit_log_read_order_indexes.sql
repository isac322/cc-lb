-- Read-order indexes for query_recent_audit (admin audit page).
--
-- The existing scoped indexes idx_audit_log_principal_seq (principal_id, id)
-- and idx_audit_log_actor_subject (actor_authority, actor_subject, id) order by
-- id ASC, so the recent-queries' ORDER BY ts DESC, id DESC forced a TEMP B-TREE
-- sort over every scoped row in the time window. The composite indexes below
-- put ts before id inside each scope so the ordering is served directly.
-- Those legacy indexes are kept: query_audit / query_audit_by_actor still use
-- ORDER BY id ASC, and the principal delete check counts by principal_id.
--
-- The unscoped (All) non-admin query needs no new index: id is the rowid
-- alias, so a backward scan of idx_audit_log_ts (ts) already yields
-- ts DESC, id DESC order.
--
-- The admin_only=true queries carry the literal predicate
-- (admin_action IS NOT NULL OR kind IS NOT NULL), so partial indexes with the
-- identical WHERE clause apply.

CREATE INDEX IF NOT EXISTS idx_audit_log_principal_ts_id
    ON audit_log_v1 (principal_id, ts DESC, id DESC);

-- actor_authority = ? implies a non-NULL authority, so this index only
-- covers rows that can match an actor-scoped lookup.
CREATE INDEX IF NOT EXISTS idx_audit_log_actor_ts_id
    ON audit_log_v1 (actor_authority, actor_subject, ts DESC, id DESC)
    WHERE actor_authority IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_audit_log_admin_ts_id
    ON audit_log_v1 (ts DESC, id DESC)
    WHERE admin_action IS NOT NULL OR kind IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_audit_log_admin_principal_ts_id
    ON audit_log_v1 (principal_id, ts DESC, id DESC)
    WHERE admin_action IS NOT NULL OR kind IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_audit_log_admin_actor_ts_id
    ON audit_log_v1 (actor_authority, actor_subject, ts DESC, id DESC)
    WHERE admin_action IS NOT NULL OR kind IS NOT NULL;
