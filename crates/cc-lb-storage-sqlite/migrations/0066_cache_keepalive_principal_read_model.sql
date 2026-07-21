ALTER TABLE cache_keepalive_sessions ADD COLUMN display_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE cache_keepalive_sessions ADD COLUMN error TEXT;
ALTER TABLE cache_keepalive_sessions ADD COLUMN config_snapshot TEXT;

ALTER TABLE cache_keepalive_decisions ADD COLUMN principal_id TEXT;
ALTER TABLE cache_keepalive_decisions ADD COLUMN session_key_hash TEXT;
ALTER TABLE cache_keepalive_decisions ADD COLUMN upstream_id TEXT;
ALTER TABLE cache_keepalive_decisions ADD COLUMN error TEXT;
ALTER TABLE cache_keepalive_decisions ADD COLUMN ttl TEXT;
ALTER TABLE cache_keepalive_decisions ADD COLUMN config_snapshot TEXT;
ALTER TABLE cache_keepalive_decisions ADD COLUMN last_message_at_ms INTEGER;

UPDATE cache_keepalive_decisions
SET
    principal_id = (SELECT principal_id FROM cache_keepalive_turns WHERE source_ref_id = cache_keepalive_decisions.source_ref_id),
    session_key_hash = (SELECT session_key_hash FROM cache_keepalive_turns WHERE source_ref_id = cache_keepalive_decisions.source_ref_id),
    upstream_id = (SELECT upstream_id FROM cache_keepalive_turns WHERE source_ref_id = cache_keepalive_decisions.source_ref_id),
    ttl = (SELECT ttl FROM cache_keepalive_sessions WHERE session_key_hash = (SELECT session_key_hash FROM cache_keepalive_turns WHERE source_ref_id = cache_keepalive_decisions.source_ref_id)),
    last_message_at_ms = (SELECT ts * 1000 FROM cache_keepalive_turns WHERE source_ref_id = cache_keepalive_decisions.source_ref_id)
WHERE EXISTS (SELECT 1 FROM cache_keepalive_turns WHERE source_ref_id = cache_keepalive_decisions.source_ref_id);

CREATE INDEX idx_cache_keepalive_sessions_principal_updated
    ON cache_keepalive_sessions (principal_id, updated_at DESC, session_key_hash ASC);

CREATE INDEX idx_cache_keepalive_turns_session_ts
    ON cache_keepalive_turns (principal_id, session_key_hash, ts DESC);

CREATE INDEX idx_cache_keepalive_decisions_principal_message
    ON cache_keepalive_decisions (principal_id, last_message_at_ms DESC, source_ref_id ASC);
