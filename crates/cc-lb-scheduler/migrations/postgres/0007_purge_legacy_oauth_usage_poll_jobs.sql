DELETE FROM apalis.jobs
WHERE idempotency_key LIKE 'adaptive:oauth_usage_poll:%'
   OR idempotency_key LIKE 'entity:oauth_usage_poll:%'
   OR idempotency_key LIKE 'cron:oauth_usage_poll_watchdog:%'
   OR idempotency_key LIKE 'singleton:oauth_usage_poll_watchdog%';
