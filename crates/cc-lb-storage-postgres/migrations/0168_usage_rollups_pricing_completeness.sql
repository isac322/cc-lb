SET LOCAL lock_timeout = '1s';

ALTER TABLE usage_rollups_v2
    ADD COLUMN IF NOT EXISTS priced_event_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS unpriced_event_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS unmetered_event_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS effective_cache_write_input_tokens BIGINT;

CREATE TABLE IF NOT EXISTS usage_rollup_pricing_backfill_stage_v1 (
    slot_id TEXT NOT NULL,
    resolution TEXT NOT NULL,
    bucket_start_unix_secs BIGINT NOT NULL,
    principal_id TEXT NOT NULL,
    upstream_id UUID NOT NULL,
    model TEXT NOT NULL,
    priced_event_count BIGINT NOT NULL DEFAULT 0,
    unpriced_event_count BIGINT NOT NULL DEFAULT 0,
    unmetered_event_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (slot_id, resolution, bucket_start_unix_secs, principal_id, upstream_id, model)
);
