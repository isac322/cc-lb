ALTER TABLE upstream_status_v1
    DROP COLUMN IF EXISTS next_warmup_at,
    DROP COLUMN IF EXISTS last_warmup_cycle_key;
