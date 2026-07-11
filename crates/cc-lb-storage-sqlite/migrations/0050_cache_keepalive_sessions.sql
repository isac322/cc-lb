CREATE TABLE cache_keepalive_sessions (
    session_key_hash TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL,
    upstream_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    refresh_count INTEGER NOT NULL,
    first_scheduled_at INTEGER NOT NULL,
    cache_anchor_at INTEGER NOT NULL,
    run_at INTEGER NOT NULL,
    ttl TEXT NOT NULL,
    status TEXT NOT NULL,
    enqueue_state TEXT NOT NULL,
    current_job_key TEXT NOT NULL,
    encrypted_payload BLOB NOT NULL,
    terminal_reason TEXT,
    expires_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    CHECK (status IN ('active', 'terminal')),
    CHECK (enqueue_state IN ('pending', 'enqueued'))
);

CREATE INDEX idx_cache_keepalive_sessions_expiry
    ON cache_keepalive_sessions (expires_at);

CREATE INDEX idx_cache_keepalive_sessions_pending_updated
    ON cache_keepalive_sessions (status, enqueue_state, updated_at);

CREATE INDEX idx_cache_keepalive_sessions_run_at
    ON cache_keepalive_sessions (status, run_at);
