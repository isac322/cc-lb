ALTER TABLE overview_excluded_error_rollups_v1 RENAME TO overview_excluded_error_legacy_v1;

CREATE TABLE overview_excluded_error_rollups_v1 (
    resolution TEXT NOT NULL CHECK (resolution IN ('minute', 'hour')),
    bucket_start_unix_secs INTEGER NOT NULL CHECK (bucket_start_unix_secs >= 0),
    upstream_id TEXT NOT NULL,
    error_count INTEGER NOT NULL CHECK (error_count >= 0),
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (resolution, bucket_start_unix_secs, upstream_id)
);

-- Initialization completes the bounded, independently committed attribution batches.
-- Empty upstream_id is reserved for historical global-only residual, never unknown UUID.
CREATE TABLE overview_excluded_error_attribution_v1 (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    phase TEXT NOT NULL CHECK (phase IN ('copy', 'events', 'complete')),
    cursor INTEGER NOT NULL,
    upper_bound INTEGER NOT NULL
);
INSERT INTO overview_excluded_error_attribution_v1 (id, phase, cursor, upper_bound)
SELECT 1, 'copy', 0, COALESCE(
    (SELECT value FROM usage_rollup_checkpoints_v1 WHERE id = 'high_water'), 0
);
