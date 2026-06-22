-- Rename queue (job_type) names + idempotency_key prefixes after refactor:
--   EntityJob   → AdaptiveJob  (queue "entity" → "adaptive")
--   SingletonJob → CronJob     (queue "singleton" → "cron")
-- Idempotency key prefix migrated in lockstep:
--   "entity:..."    → "adaptive:..."
--   "singleton:..." → "cron:..."

UPDATE Jobs SET job_type = 'adaptive' WHERE job_type = 'entity';
UPDATE Jobs SET job_type = 'cron' WHERE job_type = 'singleton';

UPDATE Jobs SET lock_by = 'adaptive' WHERE lock_by = 'entity';
UPDATE Jobs SET lock_by = 'cron' WHERE lock_by = 'singleton';

UPDATE Jobs SET status='Pending', lock_at=NULL, lock_by=NULL, last_result=NULL, attempts=0
WHERE status='Queued' AND last_result LIKE '%heartbeat timeout%';

UPDATE Jobs
SET idempotency_key = 'adaptive:' || substr(idempotency_key, 8)
WHERE idempotency_key LIKE 'entity:%';

UPDATE Jobs
SET idempotency_key = 'cron:' || substr(idempotency_key, 11)
WHERE idempotency_key LIKE 'singleton:%';

DELETE FROM Jobs WHERE job_type = 'scheduler_reconcile';
DELETE FROM Workers WHERE worker_type IN ('entity', 'singleton', 'scheduler_reconcile');
