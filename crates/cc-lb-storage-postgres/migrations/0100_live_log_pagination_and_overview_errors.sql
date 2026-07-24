CREATE INDEX IF NOT EXISTS request_events_v1_thread_list_order_idx
    ON request_events_v1 (thread_id, list_ts_ms DESC, list_event_key DESC);

CREATE INDEX IF NOT EXISTS request_events_v1_source_kind_list_order_idx
    ON request_events_v1 (source_kind, list_ts_ms DESC, list_event_key DESC);

CREATE TABLE overview_excluded_error_rollups_v1 (
    resolution TEXT NOT NULL CHECK (resolution IN ('minute', 'hour')),
    bucket_start_unix_secs BIGINT NOT NULL CHECK (bucket_start_unix_secs >= 0),
    error_count BIGINT NOT NULL CHECK (error_count >= 0),
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (resolution, bucket_start_unix_secs)
);

WITH excluded_events AS (
    SELECT r.list_ts_ms / 1000 AS ts_secs
    FROM request_events_v1 AS r
    JOIN usage_rollup_checkpoints_v1 AS checkpoint
      ON checkpoint.id = 'high_water'
     AND (r.tx_id, r.seq) <= (checkpoint.value_xid, checkpoint.value)
    WHERE r.list_status IN (401, 403, 404)
)
INSERT INTO overview_excluded_error_rollups_v1 (
    resolution,
    bucket_start_unix_secs,
    error_count,
    updated_at
)
SELECT
    'minute',
    (ts_secs / 60) * 60,
    COUNT(*),
    NOW()
FROM excluded_events
GROUP BY (ts_secs / 60) * 60
UNION ALL
SELECT
    'hour',
    (ts_secs / 3600) * 3600,
    COUNT(*),
    NOW()
FROM excluded_events
GROUP BY (ts_secs / 3600) * 3600;
