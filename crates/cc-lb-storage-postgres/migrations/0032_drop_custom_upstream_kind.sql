-- The 'custom' upstream kind has been removed from active code paths.
-- Drop it from the CHECK constraint on upstreams_v1.kind so the schema
-- matches the active StorageUpstreamKind enum (anthropic_api_key,
-- anthropic_oauth). The Postgres adapter's parse_kind already rejects
-- 'custom' with StorageError::Corrupted, so any pre-existing rows would
-- have failed at read time; this migration only tightens the schema.
ALTER TABLE upstreams_v1 DROP CONSTRAINT IF EXISTS upstreams_v1_kind_check;
ALTER TABLE upstreams_v1
    ADD CONSTRAINT upstreams_v1_kind_check
    CHECK (kind IN ('anthropic_api_key', 'anthropic_oauth'));
