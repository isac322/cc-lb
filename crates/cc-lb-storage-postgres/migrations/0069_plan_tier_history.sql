CREATE TABLE IF NOT EXISTS plan_tier_ratio_history_v1 (
    tier_key TEXT NOT NULL CHECK (tier_key IN ('pro','team_standard','max_5x','team_premium','max_20x')),
    pro_relative_ratio DOUBLE PRECISION NOT NULL CHECK (pro_relative_ratio > 0.0 AND pro_relative_ratio < 'infinity'::double precision),
    effective_from_unix_millis BIGINT NOT NULL CHECK (effective_from_unix_millis >= 0),
    effective_to_unix_millis BIGINT CHECK (effective_to_unix_millis IS NULL OR effective_to_unix_millis > effective_from_unix_millis),
    provenance TEXT NOT NULL CHECK (length(btrim(provenance)) > 0),
    created_at_unix_millis BIGINT NOT NULL CHECK (created_at_unix_millis >= 0),
    PRIMARY KEY (tier_key, effective_from_unix_millis)
);

CREATE UNIQUE INDEX IF NOT EXISTS plan_tier_ratio_history_v1_one_open_idx
    ON plan_tier_ratio_history_v1 (tier_key)
    WHERE effective_to_unix_millis IS NULL;

CREATE INDEX IF NOT EXISTS plan_tier_ratio_history_v1_asof_idx
    ON plan_tier_ratio_history_v1 (tier_key, effective_from_unix_millis DESC, effective_to_unix_millis);

CREATE TABLE IF NOT EXISTS metadata_tier_mapping_override_v1 (
    organization_type TEXT NOT NULL,
    rate_limit_tier TEXT NOT NULL,
    seat_tier TEXT NOT NULL,
    tier_key TEXT NOT NULL CHECK (tier_key IN ('pro','team_standard','max_5x','team_premium','max_20x')),
    effective_from_unix_millis BIGINT NOT NULL CHECK (effective_from_unix_millis >= 0),
    effective_to_unix_millis BIGINT CHECK (effective_to_unix_millis IS NULL OR effective_to_unix_millis > effective_from_unix_millis),
    provenance TEXT NOT NULL CHECK (length(btrim(provenance)) > 0),
    created_at_unix_millis BIGINT NOT NULL CHECK (created_at_unix_millis >= 0),
    PRIMARY KEY (organization_type, rate_limit_tier, seat_tier, effective_from_unix_millis)
);

CREATE UNIQUE INDEX IF NOT EXISTS metadata_tier_mapping_override_v1_one_open_idx
    ON metadata_tier_mapping_override_v1 (organization_type, rate_limit_tier, seat_tier)
    WHERE effective_to_unix_millis IS NULL;

CREATE TABLE IF NOT EXISTS upstream_plan_tier_history_v1 (
    upstream_id UUID NOT NULL,
    organization_uuid TEXT,
    organization_type TEXT,
    rate_limit_tier TEXT,
    seat_tier TEXT,
    tier_key TEXT CHECK (tier_key IS NULL OR tier_key IN ('pro','team_standard','max_5x','team_premium','max_20x')),
    resolution_source TEXT NOT NULL CHECK (resolution_source IN ('override','builtin','unknown')),
    resolved_ratio_snapshot DOUBLE PRECISION CHECK (resolved_ratio_snapshot IS NULL OR (resolved_ratio_snapshot > 0.0 AND resolved_ratio_snapshot < 'infinity'::double precision)),
    observed_at_unix_millis BIGINT NOT NULL CHECK (observed_at_unix_millis >= 0),
    effective_from_unix_millis BIGINT NOT NULL CHECK (effective_from_unix_millis >= 0),
    effective_to_unix_millis BIGINT CHECK (effective_to_unix_millis IS NULL OR effective_to_unix_millis > effective_from_unix_millis),
    provenance TEXT NOT NULL CHECK (length(btrim(provenance)) > 0),
    created_at_unix_millis BIGINT NOT NULL CHECK (created_at_unix_millis >= 0),
    PRIMARY KEY (upstream_id, effective_from_unix_millis),
    CHECK ((tier_key IS NULL) = (resolution_source = 'unknown')),
    CHECK (resolution_source <> 'unknown' OR resolved_ratio_snapshot IS NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS upstream_plan_tier_history_v1_one_open_idx
    ON upstream_plan_tier_history_v1 (upstream_id)
    WHERE effective_to_unix_millis IS NULL;

CREATE INDEX IF NOT EXISTS upstream_plan_tier_history_v1_asof_idx
    ON upstream_plan_tier_history_v1 (upstream_id, effective_from_unix_millis DESC, effective_to_unix_millis);

CREATE INDEX IF NOT EXISTS upstream_plan_tier_history_v1_tier_time_idx
    ON upstream_plan_tier_history_v1 (tier_key, effective_from_unix_millis DESC);

INSERT INTO plan_tier_ratio_history_v1
    (tier_key, pro_relative_ratio, effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis)
VALUES
    ('pro', 1.0, 0, NULL, 'migration:0069_plan_tier_history', (extract(epoch FROM now()) * 1000)::BIGINT),
    ('team_standard', 1.25, 0, NULL, 'migration:0069_plan_tier_history', (extract(epoch FROM now()) * 1000)::BIGINT),
    ('max_5x', 5.0, 0, NULL, 'migration:0069_plan_tier_history', (extract(epoch FROM now()) * 1000)::BIGINT),
    ('team_premium', 6.25, 0, NULL, 'migration:0069_plan_tier_history', (extract(epoch FROM now()) * 1000)::BIGINT),
    ('max_20x', 20.0, 0, NULL, 'migration:0069_plan_tier_history', (extract(epoch FROM now()) * 1000)::BIGINT)
ON CONFLICT DO NOTHING;
