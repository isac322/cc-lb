-- Add router_terminal_strategy column to principals_v1.
-- This allows each principal to configure how the router plugin pipeline
-- selects among multiple router plugins (currently only 'first-pick' and 'random').
-- Using TEXT + CHECK constraint for forward compatibility with new strategies.
ALTER TABLE principals_v1
    ADD COLUMN IF NOT EXISTS router_terminal_strategy TEXT NOT NULL DEFAULT 'first-pick'
    CHECK (router_terminal_strategy IN ('random', 'first-pick'));
