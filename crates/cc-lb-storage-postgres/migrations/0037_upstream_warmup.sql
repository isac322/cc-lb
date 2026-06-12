-- Add warmup-related columns to upstreams_v1 for upstream connection warmup support.
-- These columns track warmup state, timing, and distributed locking for periodic
-- connection health checks.
ALTER TABLE upstreams_v1 ADD COLUMN warmup_enabled BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE upstreams_v1 ADD COLUMN next_warmup_at TIMESTAMPTZ NULL;
ALTER TABLE upstreams_v1 ADD COLUMN last_warmup_cycle_key BIGINT NULL;
ALTER TABLE upstreams_v1 ADD COLUMN warmup_lease_holder TEXT NULL;
ALTER TABLE upstreams_v1 ADD COLUMN warmup_lease_until_unix_secs BIGINT NULL;

-- Create partial index for efficient warmup-due queries.
-- This indexes only active, warmup-enabled upstreams that are not deleted.
CREATE INDEX IF NOT EXISTS upstreams_v1_warmup_due_idx
ON upstreams_v1 (next_warmup_at)
WHERE warmup_enabled = true AND deleted_at IS NULL;
