CREATE INDEX IF NOT EXISTS idx_cache_keepalive_sessions_principal_effective_message
    ON cache_keepalive_sessions (
        principal_id,
        last_message_at_ms DESC,
        (('session:' || session_key_hash)) ASC
    );

CREATE INDEX IF NOT EXISTS idx_cache_keepalive_decisions_principal_effective_message
    ON cache_keepalive_decisions (
        principal_id,
        (COALESCE(last_message_at_ms, ts * 1000)) DESC,
        (('decision:' || source_ref_id)) ASC
    );
