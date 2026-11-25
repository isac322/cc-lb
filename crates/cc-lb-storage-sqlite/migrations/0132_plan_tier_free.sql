-- no-transaction
PRAGMA foreign_keys = OFF;
BEGIN IMMEDIATE;

CREATE TEMP TABLE plan_tier_rebuild_before AS
SELECT (SELECT COUNT(*) FROM plan_tier_ratio_history_v1) AS ratio_rows,
       (SELECT COUNT(*) FROM metadata_tier_mapping_override_v1) AS override_rows,
       (SELECT COUNT(*) FROM upstream_plan_tier_history_v1) AS upstream_rows;

DROP TABLE IF EXISTS plan_tier_ratio_history_v1_new;
CREATE TABLE plan_tier_ratio_history_v1_new (
    tier_key TEXT NOT NULL CHECK (tier_key IN ('free','pro','team_standard','max_5x','team_premium','max_20x')),
    pro_relative_ratio REAL NOT NULL CHECK (pro_relative_ratio > 0.0 AND pro_relative_ratio < 1.0e308),
    effective_from_unix_millis INTEGER NOT NULL CHECK (effective_from_unix_millis >= 0),
    effective_to_unix_millis INTEGER CHECK (effective_to_unix_millis IS NULL OR effective_to_unix_millis > effective_from_unix_millis),
    provenance TEXT NOT NULL CHECK (length(trim(provenance)) > 0),
    created_at_unix_millis INTEGER NOT NULL CHECK (created_at_unix_millis >= 0),
    PRIMARY KEY (tier_key, effective_from_unix_millis)
);

INSERT INTO plan_tier_ratio_history_v1_new (
    tier_key,
    pro_relative_ratio,
    effective_from_unix_millis,
    effective_to_unix_millis,
    provenance,
    created_at_unix_millis
)
SELECT
    tier_key,
    pro_relative_ratio,
    effective_from_unix_millis,
    effective_to_unix_millis,
    provenance,
    created_at_unix_millis
FROM plan_tier_ratio_history_v1;

DROP TABLE plan_tier_ratio_history_v1;
ALTER TABLE plan_tier_ratio_history_v1_new RENAME TO plan_tier_ratio_history_v1;

CREATE UNIQUE INDEX plan_tier_ratio_history_v1_one_open_idx
    ON plan_tier_ratio_history_v1 (tier_key)
    WHERE effective_to_unix_millis IS NULL;

CREATE INDEX plan_tier_ratio_history_v1_asof_idx
    ON plan_tier_ratio_history_v1 (tier_key, effective_from_unix_millis DESC, effective_to_unix_millis);

DROP TABLE IF EXISTS metadata_tier_mapping_override_v1_new;
CREATE TABLE metadata_tier_mapping_override_v1_new (
    organization_type TEXT NOT NULL,
    rate_limit_tier TEXT NOT NULL,
    seat_tier TEXT NOT NULL,
    tier_key TEXT NOT NULL CHECK (tier_key IN ('free','pro','team_standard','max_5x','team_premium','max_20x')),
    effective_from_unix_millis INTEGER NOT NULL CHECK (effective_from_unix_millis >= 0),
    effective_to_unix_millis INTEGER CHECK (effective_to_unix_millis IS NULL OR effective_to_unix_millis > effective_from_unix_millis),
    provenance TEXT NOT NULL CHECK (length(trim(provenance)) > 0),
    created_at_unix_millis INTEGER NOT NULL CHECK (created_at_unix_millis >= 0),
    PRIMARY KEY (organization_type, rate_limit_tier, seat_tier, effective_from_unix_millis)
);

INSERT INTO metadata_tier_mapping_override_v1_new (
    organization_type,
    rate_limit_tier,
    seat_tier,
    tier_key,
    effective_from_unix_millis,
    effective_to_unix_millis,
    provenance,
    created_at_unix_millis
)
SELECT
    organization_type,
    rate_limit_tier,
    seat_tier,
    tier_key,
    effective_from_unix_millis,
    effective_to_unix_millis,
    provenance,
    created_at_unix_millis
FROM metadata_tier_mapping_override_v1;

DROP TABLE metadata_tier_mapping_override_v1;
ALTER TABLE metadata_tier_mapping_override_v1_new RENAME TO metadata_tier_mapping_override_v1;

CREATE UNIQUE INDEX metadata_tier_mapping_override_v1_one_open_idx
    ON metadata_tier_mapping_override_v1 (organization_type, rate_limit_tier, seat_tier)
    WHERE effective_to_unix_millis IS NULL;

DROP TABLE IF EXISTS upstream_plan_tier_history_v1_new;
CREATE TABLE upstream_plan_tier_history_v1_new (
    upstream_id TEXT NOT NULL,
    organization_uuid TEXT,
    organization_type TEXT,
    rate_limit_tier TEXT,
    seat_tier TEXT,
    tier_key TEXT CHECK (tier_key IS NULL OR tier_key IN ('free','pro','team_standard','max_5x','team_premium','max_20x')),
    resolution_source TEXT NOT NULL CHECK (resolution_source IN ('override','builtin','unknown')),
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

CREATE TEMP TABLE plan_tier_rebuild_guard (ok INTEGER NOT NULL CHECK (ok = 1));
INSERT INTO plan_tier_rebuild_guard (ok)
SELECT CASE
    WHEN (SELECT COUNT(*) FROM plan_tier_ratio_history_v1) = b.ratio_rows
     AND (SELECT COUNT(*) FROM metadata_tier_mapping_override_v1) = b.override_rows
     AND (SELECT COUNT(*) FROM upstream_plan_tier_history_v1) = b.upstream_rows
    THEN 1
    ELSE 0
END
FROM plan_tier_rebuild_before b;

DROP TABLE temp.plan_tier_rebuild_guard;
DROP TABLE temp.plan_tier_rebuild_before;

INSERT OR IGNORE INTO plan_tier_ratio_history_v1
    (tier_key, pro_relative_ratio, effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis)
VALUES
    ('free', 0.2, 0, NULL, 'migration:0132_plan_tier_free', CAST(strftime('%s','now') AS INTEGER) * 1000);

COMMIT;
PRAGMA foreign_keys = ON;
