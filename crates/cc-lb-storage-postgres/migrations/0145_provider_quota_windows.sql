-- SQLx runs this migration in one transaction. Constraint installation uses
-- short AccessExclusive lock windows; validation runs with PostgreSQL's
-- ShareUpdateExclusive lock instead of holding an exclusive lock while it
-- scans existing rows. Existing rows must already satisfy the widened domain.
SET LOCAL lock_timeout = '5s';
-- Rehearsal evidence (PostgreSQL 15, 2026-11-06, local Docker):
-- 100 upstreams x 7 legacy windows x 2 sources x 720 hourly checkpoints
-- (1,008,000 rows), plus 1,400 latest rows and 5,040 pool rows:
-- ADD NOT VALID/VALIDATE elapsed per table was latest 10.8/7.4 ms,
-- checkpoints 18.1/1,830.3 ms, and pool 6.7/10.9 ms.
-- A 10x checkpoint fixture (10,080,000 rows; 50,400 pool rows) validated
-- in 1.43 s while 100 concurrent writer inserts completed with 32.4-47.5 ms
-- latency (35.9 ms average); ADD NOT VALID remained under 20 ms. The
-- scheduler polls OAuth usage every 60 s by default, but the checkpoint
-- writer's 0.001 utilization deadband suppresses unchanged samples; the
-- hourly fixture represents the effective changing-sample cadence used for
-- this rehearsal and the 10x fixture provides stress headroom.

ALTER TABLE upstream_subscription_quota_latest_v1
    DROP CONSTRAINT IF EXISTS upstream_subscription_quota_latest_v1_window_check;
ALTER TABLE upstream_subscription_quota_latest_v1
    ADD CONSTRAINT upstream_subscription_quota_latest_v1_window_check
    CHECK ("window" IN ('5h','7d','7d_sonnet','7d_opus','7d_fable','gemini_5h','gemini_7d','third_party_5h','third_party_7d','overage','unified'))
    NOT VALID;
ALTER TABLE upstream_subscription_quota_latest_v1
    VALIDATE CONSTRAINT upstream_subscription_quota_latest_v1_window_check;

ALTER TABLE upstream_subscription_quota_checkpoints_v1
    DROP CONSTRAINT IF EXISTS upstream_subscription_quota_checkpoints_v1_window_check;
ALTER TABLE upstream_subscription_quota_checkpoints_v1
    ADD CONSTRAINT upstream_subscription_quota_checkpoints_v1_window_check
    CHECK ("window" IN ('5h','7d','7d_sonnet','7d_opus','7d_fable','gemini_5h','gemini_7d','third_party_5h','third_party_7d','overage','unified'))
    NOT VALID;
ALTER TABLE upstream_subscription_quota_checkpoints_v1
    VALIDATE CONSTRAINT upstream_subscription_quota_checkpoints_v1_window_check;

ALTER TABLE pool_subscription_quota_history_v1
    DROP CONSTRAINT IF EXISTS pool_subscription_quota_history_v1_quota_window_check;
ALTER TABLE pool_subscription_quota_history_v1
    DROP CONSTRAINT IF EXISTS pool_subscription_quota_history_v1_quota_window_check_fable;
ALTER TABLE pool_subscription_quota_history_v1
    ADD CONSTRAINT pool_subscription_quota_history_v1_quota_window_check
    CHECK (quota_window IN ('5h','7d','7d_sonnet','7d_opus','7d_fable','gemini_5h','gemini_7d','third_party_5h','third_party_7d','overage','unified'))
    NOT VALID;
ALTER TABLE pool_subscription_quota_history_v1
    VALIDATE CONSTRAINT pool_subscription_quota_history_v1_quota_window_check;
