CREATE TABLE IF NOT EXISTS upstream_rate_limit_state_v1 (
    id UUID PRIMARY KEY,
    upstream_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('requests', 'input_tokens', 'output_tokens')),
    ts_ms BIGINT NOT NULL,
    remaining BIGINT NOT NULL,
    reset_at_ms BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS upstream_rate_limit_state_v1_upstream_kind_idx ON upstream_rate_limit_state_v1 (upstream_id, kind);
CREATE INDEX IF NOT EXISTS upstream_rate_limit_state_v1_ts_ms_idx ON upstream_rate_limit_state_v1 (ts_ms);
