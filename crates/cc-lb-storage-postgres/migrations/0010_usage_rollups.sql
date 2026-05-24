CREATE TABLE IF NOT EXISTS usage_rollups_v1 (
    resolution TEXT NOT NULL,
    bucket_start TIMESTAMPTZ NOT NULL,
    principal_id TEXT NOT NULL,
    upstream TEXT NOT NULL,
    model TEXT NOT NULL,
    value BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (resolution, bucket_start, principal_id, upstream, model)
);
CREATE TABLE IF NOT EXISTS usage_rollup_checkpoints_v1 (
    id TEXT PRIMARY KEY,
    value BIGINT NOT NULL
);
