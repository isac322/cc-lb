CREATE TABLE IF NOT EXISTS request_events_v1 (
    seq BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    principal_id TEXT,
    payload BYTEA,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS request_events_v1_ts ON request_events_v1 (ts);
