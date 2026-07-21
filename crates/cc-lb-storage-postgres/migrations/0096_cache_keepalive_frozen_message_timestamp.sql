ALTER TABLE cache_keepalive_sessions
    ADD COLUMN last_message_at_ms BIGINT NOT NULL DEFAULT 0;

UPDATE cache_keepalive_sessions
SET last_message_at_ms = cache_anchor_at * 1000
WHERE last_message_at_ms = 0;

CREATE INDEX IF NOT EXISTS idx_cache_keepalive_sessions_principal_message
    ON cache_keepalive_sessions (principal_id, last_message_at_ms DESC, session_key_hash ASC);
