CREATE TABLE IF NOT EXISTS audit_log_v1 (
    seq BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    request_id TEXT,
    principal_id TEXT,
    route TEXT,
    upstream TEXT,
    model TEXT,
    status INT,
    input_tokens BIGINT,
    output_tokens BIGINT,
    duration_ms BIGINT,
    agent_label TEXT,
    kind TEXT,
    payload BYTEA
);
CREATE INDEX IF NOT EXISTS audit_log_v1_principal_seq ON audit_log_v1 (principal_id, seq);
CREATE INDEX IF NOT EXISTS audit_log_v1_ts ON audit_log_v1 (ts);
