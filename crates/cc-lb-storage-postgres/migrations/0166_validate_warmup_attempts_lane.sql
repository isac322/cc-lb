-- Validate the constraints added NOT VALID by 0165 in their own transaction.
-- VALIDATE CONSTRAINT takes SHARE UPDATE EXCLUSIVE, so reads and writes continue
-- during the scan.
SET LOCAL lock_timeout = '1s';

ALTER TABLE warmup_attempts_v1 VALIDATE CONSTRAINT warmup_attempts_v1_lane_check;
ALTER TABLE warmup_attempts_v1 VALIDATE CONSTRAINT warmup_attempts_v1_reason_check;
