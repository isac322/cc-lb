ALTER TABLE cache_keepalive_sessions ADD COLUMN cycle_id TEXT;
ALTER TABLE cache_keepalive_turns ADD COLUMN cycle_id TEXT;


CREATE TABLE cache_keepalive_evidence (
    evidence_id TEXT PRIMARY KEY,
    cycle_id TEXT,
    evidence_kind TEXT NOT NULL,
    request_start_ms INTEGER NOT NULL,
    prefix_fingerprint TEXT,
    prefix_hash TEXT,
    prefix_policy_version TEXT,
    prefix_schema_version TEXT,
    principal_id TEXT NOT NULL,
    upstream_id TEXT NOT NULL,
    model TEXT NOT NULL,
    ttl TEXT NOT NULL,
    hit_miss TEXT,
    cache_read_input_tokens INTEGER,
    cost_micros INTEGER,
    request_ref TEXT,
    event_ref TEXT,
    created_at_ms INTEGER NOT NULL
);

CREATE INDEX idx_cache_keepalive_evidence_cycle_start
    ON cache_keepalive_evidence (cycle_id, request_start_ms, evidence_id);
CREATE INDEX idx_cache_keepalive_evidence_principal_start
    ON cache_keepalive_evidence (principal_id, request_start_ms, evidence_id);
