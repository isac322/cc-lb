ALTER TABLE cache_keepalive_evidence ADD COLUMN IF NOT EXISTS session_key_hash TEXT;

CREATE INDEX IF NOT EXISTS idx_cache_keepalive_evidence_follow_up_session
    ON cache_keepalive_evidence (principal_id, evidence_kind, session_key_hash, request_start_ms)
    WHERE session_key_hash IS NOT NULL;
