ALTER TABLE cache_keepalive_turns
    ADD COLUMN IF NOT EXISTS display_reason TEXT NOT NULL DEFAULT '';
