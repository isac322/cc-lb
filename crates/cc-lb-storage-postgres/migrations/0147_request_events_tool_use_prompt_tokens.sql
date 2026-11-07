ALTER TABLE request_events_v1
    ADD COLUMN IF NOT EXISTS tool_use_prompt_tokens BIGINT NULL
    CHECK (tool_use_prompt_tokens IS NULL OR tool_use_prompt_tokens >= 0);
