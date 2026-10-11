-- Metadata-only nullable addition; no historical backfill.
ALTER TABLE prompt_cache_observations ADD COLUMN protection_expires_at BIGINT;
