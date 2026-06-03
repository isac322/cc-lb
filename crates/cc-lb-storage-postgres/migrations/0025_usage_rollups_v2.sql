ALTER TABLE request_events_v1
    ADD COLUMN IF NOT EXISTS upstream_id UUID;

CREATE INDEX IF NOT EXISTS request_events_v1_upstream_id_idx
    ON request_events_v1 (upstream_id);

DROP TABLE IF EXISTS usage_rollups_v1;

CREATE TABLE usage_rollups_v2 (
    resolution TEXT NOT NULL CHECK (resolution IN ('minute', 'hour')),
    bucket_start_unix_secs BIGINT NOT NULL CHECK (bucket_start_unix_secs >= 0),
    principal_id TEXT NOT NULL,
    upstream_id UUID NOT NULL,
    upstream_name TEXT NOT NULL,
    model TEXT NOT NULL,
    request_count BIGINT NOT NULL DEFAULT 0 CHECK (request_count >= 0),
    input_tokens BIGINT NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
    output_tokens BIGINT NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
    cache_creation_input_tokens BIGINT NOT NULL DEFAULT 0 CHECK (cache_creation_input_tokens >= 0),
    cache_read_input_tokens BIGINT NOT NULL DEFAULT 0 CHECK (cache_read_input_tokens >= 0),
    error_count BIGINT NOT NULL DEFAULT 0 CHECK (error_count >= 0),
    latency_count BIGINT NOT NULL DEFAULT 0 CHECK (latency_count >= 0),
    latency_ms_sum BIGINT NOT NULL DEFAULT 0 CHECK (latency_ms_sum >= 0),
    latency_ms_min BIGINT CHECK (latency_ms_min >= 0),
    latency_ms_max BIGINT CHECK (latency_ms_max >= 0),
    proxy_setup_ms_count BIGINT NOT NULL DEFAULT 0 CHECK (proxy_setup_ms_count >= 0),
    proxy_setup_ms_sum BIGINT NOT NULL DEFAULT 0 CHECK (proxy_setup_ms_sum >= 0),
    shape_ms_count BIGINT NOT NULL DEFAULT 0 CHECK (shape_ms_count >= 0),
    shape_ms_sum BIGINT NOT NULL DEFAULT 0 CHECK (shape_ms_sum >= 0),
    sign_ms_count BIGINT NOT NULL DEFAULT 0 CHECK (sign_ms_count >= 0),
    sign_ms_sum BIGINT NOT NULL DEFAULT 0 CHECK (sign_ms_sum >= 0),
    upstream_ttfb_ms_count BIGINT NOT NULL DEFAULT 0 CHECK (upstream_ttfb_ms_count >= 0),
    upstream_ttfb_ms_sum BIGINT NOT NULL DEFAULT 0 CHECK (upstream_ttfb_ms_sum >= 0),
    upstream_body_ms_count BIGINT NOT NULL DEFAULT 0 CHECK (upstream_body_ms_count >= 0),
    upstream_body_ms_sum BIGINT NOT NULL DEFAULT 0 CHECK (upstream_body_ms_sum >= 0),
    virtual_cost_micros BIGINT NOT NULL DEFAULT 0 CHECK (virtual_cost_micros >= 0),
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (resolution, bucket_start_unix_secs, principal_id, upstream_id, model)
);

CREATE INDEX usage_rollups_v2_upstream_resolution_bucket_idx
    ON usage_rollups_v2 (upstream_id, resolution, bucket_start_unix_secs);
