CREATE TABLE IF NOT EXISTS usage_rollups_v1 (
    resolution TEXT NOT NULL,
    bucket_start TIMESTAMPTZ NOT NULL,
    principal_id TEXT NOT NULL,
    upstream TEXT NOT NULL,
    model TEXT NOT NULL,
    request_count BIGINT NOT NULL DEFAULT 0 CHECK (request_count >= 0),
    input_tokens BIGINT NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
    output_tokens BIGINT NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
    error_count BIGINT NOT NULL DEFAULT 0 CHECK (error_count >= 0),
    latency_count BIGINT NOT NULL DEFAULT 0 CHECK (latency_count >= 0),
    latency_ms_sum BIGINT NOT NULL DEFAULT 0 CHECK (latency_ms_sum >= 0),
    latency_ms_min BIGINT,
    latency_ms_max BIGINT,
    virtual_cost_micros BIGINT NOT NULL DEFAULT 0 CHECK (virtual_cost_micros >= 0),
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (resolution, bucket_start, principal_id, upstream, model)
);
CREATE TABLE IF NOT EXISTS usage_rollup_checkpoints_v1 (
    id TEXT PRIMARY KEY,
    value BIGINT NOT NULL
);
