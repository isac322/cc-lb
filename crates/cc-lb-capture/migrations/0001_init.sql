CREATE TABLE capture_v1 (
    event_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    ts_unix_ms INTEGER NOT NULL,
    canonical_model TEXT,
    chosen_upstream_id TEXT,
    disposition TEXT NOT NULL,
    attempt_num INTEGER,
    cache_read_input_tokens INTEGER,
    cache_creation_5m INTEGER,
    cache_creation_1h INTEGER,
    input_tokens INTEGER,
    output_tokens INTEGER,
    client_status INTEGER,
    upstream_status INTEGER,
    schema_version INTEGER NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE INDEX capture_v1_request_id_idx ON capture_v1 (request_id);
CREATE INDEX capture_v1_ts_unix_ms_idx ON capture_v1 (ts_unix_ms);
CREATE INDEX capture_v1_canonical_model_idx ON capture_v1 (canonical_model);
