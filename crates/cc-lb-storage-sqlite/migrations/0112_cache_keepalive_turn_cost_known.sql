-- Preserve every existing signed cost as known, including negative values.
-- Keep the default for callers that insert a cost without specifying availability.
ALTER TABLE cache_keepalive_turns
    ADD COLUMN cost_known INTEGER NOT NULL DEFAULT 1 CHECK (cost_known IN (0, 1));
