SET LOCAL lock_timeout = '1s';

-- Widen the kind check for realtime call ownership. Every existing row already
-- satisfies the narrower check this replaces, so the new constraint is added
-- NOT VALID: new and updated rows are checked, and no table scan runs while
-- startup migrations hold the advisory lock.
ALTER TABLE upstream_affinity_v1
    DROP CONSTRAINT IF EXISTS upstream_affinity_v1_kind_check;

ALTER TABLE upstream_affinity_v1
    ADD CONSTRAINT upstream_affinity_v1_kind_check
    CHECK (
        kind IN (
            'anthropic_web_search_encrypted_content',
            'anthropic_file_id',
            'anthropic_container_id',
            'openai_response_id',
            'openai_conversation_id',
            'openai_realtime_call',
            'gemini_cached_content'
        )
    ) NOT VALID;
