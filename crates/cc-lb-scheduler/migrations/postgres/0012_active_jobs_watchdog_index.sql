-- Partial index over active scheduler rows so key-status lookups and watchdog
-- prefix scans (using LIKE '<prefix>%' on text_pattern_ops) avoid decoding full
-- job payloads or scanning archived rows.
CREATE INDEX IF NOT EXISTS idx_apalis_jobs_active_type_key
    ON apalis.jobs (job_type, idempotency_key text_pattern_ops)
    WHERE status IN ('Pending', 'Queued', 'Running', 'Failed')
      AND idempotency_key IS NOT NULL;
