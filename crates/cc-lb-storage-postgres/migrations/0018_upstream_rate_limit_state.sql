CREATE TABLE IF NOT EXISTS upstream_rate_limit_state_v1 (
    upstream_id UUID NOT NULL,
    "window" TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('requests', 'tokens', 'input_tokens', 'output_tokens')),
    limit_value BIGINT,
    remaining BIGINT,
    reset TEXT,
    observed_at_unix_secs BIGINT NOT NULL,
    PRIMARY KEY (upstream_id, "window", kind)
);

CREATE INDEX IF NOT EXISTS upstream_rate_limit_state_v1_upstream_kind_idx ON upstream_rate_limit_state_v1 (upstream_id, kind);
CREATE INDEX IF NOT EXISTS upstream_rate_limit_state_v1_observed_at_idx ON upstream_rate_limit_state_v1 (observed_at_unix_secs);
