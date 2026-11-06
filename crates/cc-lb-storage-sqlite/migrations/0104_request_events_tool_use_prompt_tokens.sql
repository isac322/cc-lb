ALTER TABLE request_events_v1
    ADD COLUMN tool_use_prompt_tokens INTEGER NULL
        CHECK (tool_use_prompt_tokens IS NULL OR tool_use_prompt_tokens >= 0);
