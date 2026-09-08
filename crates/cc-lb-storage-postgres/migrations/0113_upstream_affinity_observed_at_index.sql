SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '90s';

CREATE INDEX upstream_affinity_v1_observed_at_idx
    ON upstream_affinity_v1 (observed_at_unix_secs);
