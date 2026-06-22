DROP INDEX IF EXISTS apalis.idx_jobs_idempotency_key;

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency_key
ON apalis.jobs(job_type, idempotency_key)
WHERE status IN ('Pending','Running','Queued');
