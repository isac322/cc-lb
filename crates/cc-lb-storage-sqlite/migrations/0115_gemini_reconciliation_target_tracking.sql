-- Add canonical target progress tracking for databases that already applied 0114.
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
