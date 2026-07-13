-- Plain CREATE INDEX, not CONCURRENTLY: sqlx's migration advisory lock is
-- database-wide (not per-schema/connection), so concurrent `migrate!()`
-- callers (multiple cc-lb-server instances on one DB, or this crate's
-- per-schema conformance tests run in parallel) serialize on it, and
-- CONCURRENTLY additionally waits out other transactions' snapshots -- the
-- two combine into a reproducible "deadlock detected" (see
-- golang-migrate/migrate#960). A plain CREATE INDEX briefly holds a SHARE
-- lock instead, acceptable for this purely additive index.
CREATE INDEX IF NOT EXISTS upstream_subscription_quota_checkpoints_slim_cover_idx
    ON upstream_subscription_quota_checkpoints_v1
    (upstream_id, "window", source, changed_at_unix_millis ASC, sample_id ASC)
    INCLUDE (utilization, status, resets_at_unix_secs);
