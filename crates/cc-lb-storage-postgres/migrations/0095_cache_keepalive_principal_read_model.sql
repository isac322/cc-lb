ALTER TABLE cache_keepalive_sessions
    ADD COLUMN display_reason TEXT NOT NULL DEFAULT '',
    ADD COLUMN error TEXT,
    ADD COLUMN config_snapshot TEXT;

ALTER TABLE cache_keepalive_decisions
    ADD COLUMN principal_id TEXT,
    ADD COLUMN session_key_hash TEXT,
    ADD COLUMN upstream_id UUID,
    ADD COLUMN error TEXT,
    ADD COLUMN ttl TEXT,
    ADD COLUMN config_snapshot TEXT,
    ADD COLUMN last_message_at_ms BIGINT;

UPDATE cache_keepalive_decisions decision_row
SET
    principal_id = turn_row.principal_id,
    session_key_hash = turn_row.session_key_hash,
    upstream_id = turn_row.upstream_id,
    ttl = session_row.ttl,
    last_message_at_ms = turn_row.ts * 1000
FROM cache_keepalive_turns turn_row
LEFT JOIN cache_keepalive_sessions session_row ON session_row.session_key_hash = turn_row.session_key_hash
WHERE decision_row.source_ref_id = turn_row.source_ref_id;

CREATE INDEX IF NOT EXISTS idx_cache_keepalive_sessions_principal_updated
    ON cache_keepalive_sessions (principal_id, updated_at DESC, session_key_hash ASC);

CREATE INDEX IF NOT EXISTS idx_cache_keepalive_turns_session_ts
    ON cache_keepalive_turns (principal_id, session_key_hash, ts DESC);

CREATE INDEX IF NOT EXISTS idx_cache_keepalive_decisions_principal_message
    ON cache_keepalive_decisions (principal_id, last_message_at_ms DESC, source_ref_id ASC);
