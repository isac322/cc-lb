-- Queued payloads no current job variant deserializes: retired `quota_gc`
-- cron ticks (ADR 0005) and pre-rename `o_auth_*` type tags.
DELETE FROM apalis.jobs
WHERE job_type IN ('adaptive', 'cron')
  AND (
    idempotency_key LIKE 'cron:quota_gc%'
    OR position(convert_to('"type":"quota_gc"', 'UTF8') IN job) > 0
    OR position(convert_to('"type":"o_auth_', 'UTF8') IN job) > 0
  );
