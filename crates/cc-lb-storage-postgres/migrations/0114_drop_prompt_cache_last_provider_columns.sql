SET LOCAL lock_timeout = '1s';

ALTER TABLE prompt_cache_observations
    DROP COLUMN IF EXISTS last_provider_cache_read_tokens,
    DROP COLUMN IF EXISTS last_provider_cache_creation_tokens;
