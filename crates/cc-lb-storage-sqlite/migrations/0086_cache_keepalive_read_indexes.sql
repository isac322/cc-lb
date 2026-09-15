CREATE INDEX idx_cache_keepalive_sessions_principal_message_entry
    ON cache_keepalive_sessions (
        principal_id,
        last_message_at_ms DESC,
        ('session:' || session_key_hash) ASC
    );

CREATE INDEX idx_cache_keepalive_decisions_principal_message_entry
    ON cache_keepalive_decisions (
        principal_id,
        COALESCE(last_message_at_ms, ts * 1000) DESC,
        ('decision:' || source_ref_id) ASC
    );
