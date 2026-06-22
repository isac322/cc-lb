UPDATE apalis.jobs SET job_type = 'adaptive' WHERE job_type = 'entity';
UPDATE apalis.jobs SET job_type = 'cron' WHERE job_type = 'singleton';

UPDATE apalis.jobs SET lock_by = 'adaptive' WHERE lock_by = 'entity';
UPDATE apalis.jobs SET lock_by = 'cron' WHERE lock_by = 'singleton';

UPDATE apalis.jobs SET status='Pending', lock_at=NULL, lock_by=NULL, last_result=NULL, attempts=0
WHERE status='Queued' AND last_result::TEXT LIKE '%heartbeat timeout%';

UPDATE apalis.jobs
SET idempotency_key = 'adaptive:' || substring(idempotency_key FROM 8)
WHERE idempotency_key LIKE 'entity:%';

UPDATE apalis.jobs
SET idempotency_key = 'cron:' || substring(idempotency_key FROM 11)
WHERE idempotency_key LIKE 'singleton:%';

DELETE FROM apalis.jobs WHERE job_type = 'scheduler_reconcile';
DELETE FROM apalis.workers WHERE worker_type IN ('entity', 'singleton', 'scheduler_reconcile');
