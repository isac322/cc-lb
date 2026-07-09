-- Drop the write-only contributors_json blob column from pool subscription quota history.
--
-- Historical pool utilization is now recomputable from the effective-dated
-- plan_tier_ratio_history_v1 / upstream_plan_tier_history_v1 catalog (see
-- docs/adr/0005-pool-quota-history-recompute-derivability.md), so the per-row
-- JSON blob is redundant.
--
-- On Postgres DROP COLUMN is metadata-only and instant; disk space is reclaimed
-- lazily by autovacuum (or VACUUM FULL / pg_repack for immediate reclamation).
ALTER TABLE pool_subscription_quota_history_v1 DROP COLUMN contributors_json;
