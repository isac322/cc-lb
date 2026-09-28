-- Queued payloads no current job variant deserializes: retired `quota_gc`
-- cron ticks (ADR 0005) and pre-rename `o_auth_*` type tags.
DELETE FROM Jobs
WHERE job_type IN ('adaptive', 'cron')
  AND (
    idempotency_key LIKE 'cron:quota_gc%'
    OR instr(job, CAST('"type":"quota_gc"' AS BLOB)) > 0
    OR instr(job, CAST('"type":"o_auth_' AS BLOB)) > 0
  );
