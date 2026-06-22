DELETE FROM apalis.jobs
WHERE id NOT IN (
    SELECT id
    FROM (
        SELECT
            id,
            ROW_NUMBER() OVER (
                PARTITION BY job_type, idempotency_key
                ORDER BY
                    CASE status
                        WHEN 'Pending' THEN 1
                        WHEN 'Queued' THEN 2
                        WHEN 'Running' THEN 3
                        WHEN 'Failed' THEN 4
                        WHEN 'Killed' THEN 5
                        WHEN 'Done' THEN 6
                        ELSE 7
                    END,
                    priority DESC,
                    id ASC
            ) AS rn
        FROM apalis.jobs
        WHERE idempotency_key IS NOT NULL
    ) ranked
    WHERE rn = 1
)
AND idempotency_key IS NOT NULL;

DROP INDEX IF EXISTS apalis.idx_jobs_idempotency_key;

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency_key
    ON apalis.jobs(job_type, idempotency_key);
