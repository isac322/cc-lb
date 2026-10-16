-- Plain CREATE INDEX, not CONCURRENTLY: sqlx's database-wide migration
-- advisory lock serializes migrators and CONCURRENTLY deadlocks under it
-- (see 0085_quota_aggregate_covering_indexes.sql). One index per migration
-- file so each build's SHARE lock is released at its own commit instead of
-- being held across every index build in this set.
--
-- A startup CREATE INDEX holds a SHARE lock that blocks writes to
-- cache_keepalive_decisions for the entire build; lock_timeout bounds only lock
-- acquisition, not the held lock. On a large existing table an operator MUST
-- pre-create this index with CREATE INDEX CONCURRENTLY IF NOT EXISTS using the
-- identical definition and verify pg_index.indisvalid before deploying this
-- migration; IF NOT EXISTS then makes this statement a no-op.
SET LOCAL lock_timeout = '1s';

CREATE INDEX IF NOT EXISTS idx_cache_keepalive_decisions_ts
    ON cache_keepalive_decisions (ts);
