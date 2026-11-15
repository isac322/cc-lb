SET LOCAL lock_timeout = '1s';

ALTER TABLE overview_excluded_error_rollups_v1 RENAME TO overview_excluded_error_legacy_v1;
CREATE TABLE overview_excluded_error_rollups_v1 (
    resolution TEXT NOT NULL CHECK (resolution IN ('minute', 'hour')),
    bucket_start_unix_secs BIGINT NOT NULL CHECK (bucket_start_unix_secs >= 0),
    upstream_id TEXT NOT NULL,
    error_count BIGINT NOT NULL CHECK (error_count >= 0),
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT overview_excluded_error_attributed_pk
        PRIMARY KEY (resolution, bucket_start_unix_secs, upstream_id)
);

-- Metadata only: initialization runs bounded, independently committed batches.
-- Empty upstream_id is historical global-only residual, not the unknown UUID.
CREATE TABLE overview_excluded_error_attribution_v1 (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    phase TEXT NOT NULL CHECK (phase IN ('copy', 'events', 'complete')),
    cursor_xid xid8 NOT NULL,
    cursor_seq BIGINT NOT NULL,
    upper_xid xid8 NOT NULL,
    upper_seq BIGINT NOT NULL
);
INSERT INTO overview_excluded_error_attribution_v1
    (id, phase, cursor_xid, cursor_seq, upper_xid, upper_seq)
SELECT 1, CASE WHEN EXISTS(SELECT 1 FROM overview_excluded_error_legacy_v1 LIMIT 1)
               THEN 'copy' ELSE 'complete' END, '0'::xid8, 0,
       COALESCE((SELECT value_xid FROM usage_rollup_checkpoints_v1 WHERE id='high_water'), '0'::xid8),
       COALESCE((SELECT value FROM usage_rollup_checkpoints_v1 WHERE id='high_water'), 0);

DO $$
BEGIN
    IF (SELECT phase='complete' FROM overview_excluded_error_attribution_v1 WHERE id=1) THEN
        DROP TABLE overview_excluded_error_legacy_v1;
    END IF;
END $$;
