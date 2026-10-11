-- Principal prompt-cache policy. Metadata-only addition with a constant
-- default and no historical backfill: existing rows are preserve_client, which
-- is exactly today's behavior. The CHECK keeps the stored value inside the
-- enum the admin API and engine accept.
ALTER TABLE principals_v1 ADD COLUMN prompt_cache_mode TEXT NOT NULL DEFAULT 'preserve_client' CHECK (prompt_cache_mode IN ('preserve_client', 'one_hour_no_keepalive'));
