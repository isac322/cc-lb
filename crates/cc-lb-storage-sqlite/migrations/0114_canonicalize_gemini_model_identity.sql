-- Defer historical Gemini model identity reconciliation to the off-request
-- rollup worker. Startup must only install durable, resumable state.
CREATE TABLE IF NOT EXISTS usage_rollup_reconciliations_v1 (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('pending', 'rewriting', 'rewriting_nested', 'complete')),
    cursor INTEGER NOT NULL DEFAULT 0 CHECK (cursor >= 0),
    event_upper_bound INTEGER NOT NULL DEFAULT 0 CHECK (event_upper_bound >= 0),
    rollup_upper_bound INTEGER NOT NULL DEFAULT 0 CHECK (rollup_upper_bound >= 0)
);

INSERT INTO usage_rollup_reconciliations_v1 (
    id, status, cursor, event_upper_bound
)
VALUES ('canonicalize_gemini_model_identity', 'pending', 0, 0)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS usage_rollup_reconciliation_markers_v1 (
    resolution TEXT NOT NULL,
    bucket_start_unix_secs INTEGER NOT NULL,
    principal_id TEXT NOT NULL,
    upstream_id TEXT NOT NULL,
    marker_model TEXT NOT NULL,
    PRIMARY KEY (
        resolution,
        bucket_start_unix_secs,
        principal_id,
        upstream_id,
        marker_model
    )
);
