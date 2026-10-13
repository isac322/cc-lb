-- Both indexes duplicate the primary key
-- (upstream_id, window, source, changed_at_unix_millis, sample_id), which
-- serves the ASC order directly and the DESC order scanned backward. Mirrors
-- PostgreSQL 0135.
DROP INDEX IF EXISTS upstream_subscription_quota_checkpoints_range_idx;
DROP INDEX IF EXISTS upstream_subscription_quota_checkpoints_anchor_idx;
