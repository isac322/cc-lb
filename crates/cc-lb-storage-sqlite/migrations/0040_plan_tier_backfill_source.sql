-- no-transaction
BEGIN TRANSACTION;

DROP TABLE IF EXISTS upstream_plan_tier_history_v1_new;

CREATE TABLE upstream_plan_tier_history_v1_new (
    upstream_id TEXT NOT NULL,
    organization_uuid TEXT,
    organization_type TEXT,
    rate_limit_tier TEXT,
    seat_tier TEXT,
    tier_key TEXT CHECK (tier_key IS NULL OR tier_key IN ('pro','team_standard','max_5x','team_premium','max_20x')),
    resolution_source TEXT NOT NULL CHECK (resolution_source IN ('override','builtin','backfill','unknown')),
    resolved_ratio_snapshot REAL CHECK (resolved_ratio_snapshot IS NULL OR (resolved_ratio_snapshot > 0.0 AND resolved_ratio_snapshot < 1.0e308)),
    observed_at_unix_millis INTEGER NOT NULL CHECK (observed_at_unix_millis >= 0),
    effective_from_unix_millis INTEGER NOT NULL CHECK (effective_from_unix_millis >= 0),
    effective_to_unix_millis INTEGER CHECK (effective_to_unix_millis IS NULL OR effective_to_unix_millis > effective_from_unix_millis),
    provenance TEXT NOT NULL CHECK (length(trim(provenance)) > 0),
    created_at_unix_millis INTEGER NOT NULL CHECK (created_at_unix_millis >= 0),
    PRIMARY KEY (upstream_id, effective_from_unix_millis),
    CHECK ((tier_key IS NULL) = (resolution_source = 'unknown')),
    CHECK (resolution_source <> 'unknown' OR resolved_ratio_snapshot IS NULL)
);

INSERT INTO upstream_plan_tier_history_v1_new (
    upstream_id,
    organization_uuid,
    organization_type,
    rate_limit_tier,
    seat_tier,
    tier_key,
    resolution_source,
    resolved_ratio_snapshot,
    observed_at_unix_millis,
    effective_from_unix_millis,
    effective_to_unix_millis,
    provenance,
    created_at_unix_millis
)
SELECT
    upstream_id,
    organization_uuid,
    organization_type,
    rate_limit_tier,
    seat_tier,
    tier_key,
    resolution_source,
    resolved_ratio_snapshot,
    observed_at_unix_millis,
    effective_from_unix_millis,
    effective_to_unix_millis,
    provenance,
    created_at_unix_millis
FROM upstream_plan_tier_history_v1;

DROP TABLE upstream_plan_tier_history_v1;
ALTER TABLE upstream_plan_tier_history_v1_new RENAME TO upstream_plan_tier_history_v1;

CREATE UNIQUE INDEX upstream_plan_tier_history_v1_one_open_idx
    ON upstream_plan_tier_history_v1 (upstream_id)
    WHERE effective_to_unix_millis IS NULL;

CREATE INDEX upstream_plan_tier_history_v1_asof_idx
    ON upstream_plan_tier_history_v1 (upstream_id, effective_from_unix_millis DESC, effective_to_unix_millis);

CREATE INDEX upstream_plan_tier_history_v1_tier_time_idx
    ON upstream_plan_tier_history_v1 (tier_key, effective_from_unix_millis DESC);

COMMIT;
