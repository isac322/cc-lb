-- Drop the legacy raw subscription-quota observations table and its indexes.
--
-- The subscription-quota feature moved from raw per-sample observations to
-- change-only checkpoints in upstream_subscription_quota_checkpoints_v1 (see
-- docs/adr/0007-subscription-quota-checkpoint-history.md). The raw table is no
-- longer written or read by any live path, so it is dropped here on any
-- database that still has it. Fresh databases briefly create it (migration
-- 0024) and drop it here; existing databases drop it on next startup.
--
-- DROP TABLE also removes the associated indexes; the explicit DROP INDEX
-- statements are defensive and harmless.
DROP INDEX IF EXISTS upstream_subscription_quota_obs_series_idx;
DROP INDEX IF EXISTS upstream_subscription_quota_obs_gc_idx;
DROP TABLE IF EXISTS upstream_subscription_quota_observations_v1;
