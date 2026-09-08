CREATE INDEX IF NOT EXISTS upstream_affinity_v1_observed_at_idx
    ON upstream_affinity_v1 (observed_at_unix_secs);
