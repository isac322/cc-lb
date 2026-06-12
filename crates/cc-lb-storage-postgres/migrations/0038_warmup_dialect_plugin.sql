-- Add warmup_dialect_plugin column to upstreams_v1 for dialect-specific plugin configuration.
-- This column stores JSONB configuration data for warmup plugins per upstream.
ALTER TABLE upstreams_v1 ADD COLUMN IF NOT EXISTS warmup_dialect_plugin jsonb NULL;
