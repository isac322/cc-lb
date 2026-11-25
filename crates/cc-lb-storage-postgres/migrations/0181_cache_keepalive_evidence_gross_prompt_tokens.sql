SET LOCAL lock_timeout = '1s';

ALTER TABLE cache_keepalive_evidence
    ADD COLUMN IF NOT EXISTS gross_prompt_tokens BIGINT;
