SET LOCAL lock_timeout = '1s';

ALTER TABLE plan_tier_ratio_history_v1
    DROP CONSTRAINT IF EXISTS plan_tier_ratio_history_v1_tier_key_check,
    ADD CONSTRAINT plan_tier_ratio_history_v1_tier_key_check
    CHECK (tier_key IN ('free','pro','team_standard','max_5x','team_premium','max_20x'));

ALTER TABLE metadata_tier_mapping_override_v1
    DROP CONSTRAINT IF EXISTS metadata_tier_mapping_override_v1_tier_key_check,
    ADD CONSTRAINT metadata_tier_mapping_override_v1_tier_key_check
    CHECK (tier_key IN ('free','pro','team_standard','max_5x','team_premium','max_20x'));

ALTER TABLE upstream_plan_tier_history_v1
    DROP CONSTRAINT IF EXISTS upstream_plan_tier_history_v1_tier_key_check,
    ADD CONSTRAINT upstream_plan_tier_history_v1_tier_key_check
    CHECK (tier_key IS NULL OR tier_key IN ('free','pro','team_standard','max_5x','team_premium','max_20x'));

INSERT INTO plan_tier_ratio_history_v1
    (tier_key, pro_relative_ratio, effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis)
VALUES
    ('free', 0.2, 0, NULL, 'migration:0180_plan_tier_free', (extract(epoch FROM now()) * 1000)::BIGINT)
ON CONFLICT DO NOTHING;
