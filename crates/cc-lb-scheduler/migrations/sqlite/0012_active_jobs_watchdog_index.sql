-- Partial index over active scheduler rows so key-status lookups and watchdog
-- prefix scans avoid decoding full job payloads or scanning archived rows.
CREATE INDEX IF NOT EXISTS idx_jobs_active_type_key
    ON Jobs (job_type, idempotency_key)
    WHERE status IN ('Pending', 'Queued', 'Running', 'Failed')
      AND idempotency_key IS NOT NULL;
