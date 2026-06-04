ALTER TABLE request_events_v1
    ADD COLUMN IF NOT EXISTS cache_breakpoints JSONB NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(cache_breakpoints) = 'array');
