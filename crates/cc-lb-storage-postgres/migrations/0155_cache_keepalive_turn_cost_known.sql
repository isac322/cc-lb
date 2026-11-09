-- Preserve every existing signed cost as known, including negative values.
-- Keep the default for callers that insert a cost without specifying availability.
ALTER TABLE cache_keepalive_turns
    ADD COLUMN IF NOT EXISTS cost_known BOOLEAN NOT NULL DEFAULT TRUE;
