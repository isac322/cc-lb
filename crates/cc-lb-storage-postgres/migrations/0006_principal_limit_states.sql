CREATE TABLE IF NOT EXISTS principal_limit_states_v1 (
    principal_id TEXT NOT NULL,
    identity_kind TEXT NOT NULL,
    identity_value TEXT NOT NULL,
    window TEXT NOT NULL,
    kind TEXT NOT NULL,
    snapshot JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (principal_id, identity_kind, identity_value, window, kind)
);
