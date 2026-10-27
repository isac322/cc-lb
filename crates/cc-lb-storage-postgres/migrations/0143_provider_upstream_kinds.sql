-- Allow the provider credential kinds supported by the storage adapter while
-- continuing to reject unknown upstream kinds.
ALTER TABLE upstream_spec_v1
    DROP CONSTRAINT IF EXISTS upstream_spec_v1_kind_check;

ALTER TABLE upstream_spec_v1
    ADD CONSTRAINT upstream_spec_v1_kind_check
    CHECK (
        kind IN (
            'anthropic_api_key',
            'anthropic_oauth',
            'openai_api_key',
            'openai_oauth',
            'gemini_api_key',
            'gemini_oauth'
        )
    );
