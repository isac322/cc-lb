CREATE TABLE IF NOT EXISTS cache_keepalive_sessions (
    session_key_hash TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL,
    upstream_id UUID NOT NULL,
    generation BIGINT NOT NULL,
    refresh_count BIGINT NOT NULL,
    first_scheduled_at BIGINT NOT NULL,
    cache_anchor_at BIGINT NOT NULL,
    run_at BIGINT NOT NULL,
    ttl TEXT NOT NULL,
    status TEXT NOT NULL,
    enqueue_state TEXT NOT NULL,
    current_job_key TEXT NOT NULL,
    encrypted_payload BYTEA NOT NULL,
    terminal_reason TEXT,
    expires_at BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    CHECK (status IN ('active', 'terminal')),
    CHECK (enqueue_state IN ('pending', 'enqueued'))
);

CREATE INDEX IF NOT EXISTS idx_cache_keepalive_sessions_expiry
    ON cache_keepalive_sessions (expires_at);

CREATE INDEX IF NOT EXISTS idx_cache_keepalive_sessions_pending_updated
    ON cache_keepalive_sessions (status, enqueue_state, updated_at);

CREATE INDEX IF NOT EXISTS idx_cache_keepalive_sessions_run_at
    ON cache_keepalive_sessions (status, run_at);
