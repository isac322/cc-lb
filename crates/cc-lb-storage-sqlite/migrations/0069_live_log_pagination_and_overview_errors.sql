CREATE INDEX IF NOT EXISTS request_events_v1_thread_list_order_idx
    ON request_events_v1 (thread_id, list_ts_ms DESC, list_event_key DESC, id DESC);

CREATE INDEX IF NOT EXISTS request_events_v1_source_kind_list_order_idx
    ON request_events_v1 (source_kind, list_ts_ms DESC, list_event_key DESC, id DESC);

CREATE TABLE overview_excluded_error_rollups_v1 (
    resolution TEXT NOT NULL CHECK (resolution IN ('minute', 'hour')),
    bucket_start_unix_secs INTEGER NOT NULL CHECK (bucket_start_unix_secs >= 0),
    error_count INTEGER NOT NULL CHECK (error_count >= 0),
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (resolution, bucket_start_unix_secs)
);

INSERT INTO overview_excluded_error_rollups_v1 (
    resolution,
    bucket_start_unix_secs,
    error_count,
    updated_at
)
SELECT
    'minute',
    (list_ts_ms / 60000) * 60,
    COUNT(*),
    unixepoch()
FROM request_events_v1
WHERE id <= COALESCE(
    (SELECT value FROM usage_rollup_checkpoints_v1 WHERE id = 'high_water'),
    0
)
  AND list_status IN (401, 403, 404)
GROUP BY (list_ts_ms / 60000) * 60
UNION ALL
SELECT
    'hour',
    (list_ts_ms / 3600000) * 3600,
    COUNT(*),
    unixepoch()
FROM request_events_v1
WHERE id <= COALESCE(
    (SELECT value FROM usage_rollup_checkpoints_v1 WHERE id = 'high_water'),
    0
)
  AND list_status IN (401, 403, 404)
GROUP BY (list_ts_ms / 3600000) * 3600;
