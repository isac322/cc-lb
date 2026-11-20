SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '90s';

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
    );
