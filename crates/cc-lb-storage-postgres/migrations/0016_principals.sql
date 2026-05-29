CREATE TABLE IF NOT EXISTS principals_v1 (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('machine', 'human', 'admin')),
    allowed_models JSONB NOT NULL CHECK (jsonb_typeof(allowed_models) = 'array'),
    default_limits JSONB NOT NULL CHECK (jsonb_typeof(default_limits) = 'array'),
    enabled BOOLEAN NOT NULL,
    last_apply_error TEXT,
    last_apply_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    revision BIGINT NOT NULL CHECK (revision >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS principals_v1_name_active_uniq ON principals_v1 (name) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS principals_v1_name_idx ON principals_v1 (name);
CREATE INDEX IF NOT EXISTS principals_v1_deleted_at_idx ON principals_v1 (deleted_at);
CREATE INDEX IF NOT EXISTS principals_v1_enabled_idx ON principals_v1 (enabled);
