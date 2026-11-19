-- Validate the constraint added NOT VALID by 0171 in its own transaction.
SET LOCAL lock_timeout = '1s';

ALTER TABLE warmup_attempts_v1 VALIDATE CONSTRAINT warmup_attempts_v1_reason_check;
