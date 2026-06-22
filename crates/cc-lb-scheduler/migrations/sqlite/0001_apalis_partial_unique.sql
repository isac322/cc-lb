DROP INDEX IF EXISTS idx_jobs_idempotency_key;

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency_key
ON Jobs(job_type, idempotency_key)
WHERE status IN ('Pending','Running','Queued');
