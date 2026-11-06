ALTER TABLE cache_keepalive_sessions ADD COLUMN IF NOT EXISTS cycle_id UUID;
ALTER TABLE cache_keepalive_turns ADD COLUMN IF NOT EXISTS cycle_id UUID;


CREATE TABLE IF NOT EXISTS cache_keepalive_evidence (
    evidence_id TEXT PRIMARY KEY,
    cycle_id UUID,
    evidence_kind TEXT NOT NULL,
    request_start_ms BIGINT NOT NULL,
    prefix_fingerprint TEXT,
    prefix_hash TEXT,
    prefix_policy_version TEXT,
    prefix_schema_version TEXT,
    principal_id TEXT NOT NULL,
    upstream_id UUID NOT NULL,
    model TEXT NOT NULL,
    ttl TEXT NOT NULL,
    hit_miss TEXT,
    cache_read_input_tokens BIGINT,
    cost_micros BIGINT,
    request_ref TEXT,
    event_ref TEXT,
    created_at_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cache_keepalive_evidence_cycle_start
    ON cache_keepalive_evidence (cycle_id, request_start_ms, evidence_id);
CREATE INDEX IF NOT EXISTS idx_cache_keepalive_evidence_principal_start
    ON cache_keepalive_evidence (principal_id, request_start_ms, evidence_id);

CREATE INDEX IF NOT EXISTS idx_cache_keepalive_evidence_principal_event_ref
    ON cache_keepalive_evidence (principal_id, event_ref)
    WHERE evidence_kind = 'renewal';
CREATE INDEX IF NOT EXISTS idx_cache_keepalive_evidence_follow_up_prefix
    ON cache_keepalive_evidence (principal_id, evidence_kind, prefix_hash, request_start_ms);
