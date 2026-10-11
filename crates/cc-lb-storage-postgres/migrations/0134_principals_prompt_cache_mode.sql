-- Principal prompt-cache policy. Metadata-only addition: a constant default on
-- a non-rewritten column, so PostgreSQL stores the value without touching
-- existing rows and no backfill is required. Existing rows read as
-- 'preserve_client', which is exactly today's behavior. The inline CHECK keeps
-- the stored value inside the enum the admin API and engine accept, and
-- PostgreSQL skips constraint validation because the default satisfies it.
ALTER TABLE principals_v1
    ADD COLUMN IF NOT EXISTS prompt_cache_mode TEXT NOT NULL DEFAULT 'preserve_client'
    CHECK (prompt_cache_mode IN ('preserve_client', 'one_hour_no_keepalive'));
