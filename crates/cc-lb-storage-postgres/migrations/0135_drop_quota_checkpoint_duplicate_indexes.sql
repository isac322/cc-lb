-- Both indexes (0074) duplicate the primary key
-- (upstream_id, "window", source, changed_at_unix_millis, sample_id): the PK
-- serves range_idx's ASC order directly and anchor_idx's DESC order when
-- scanned backward.
-- Plain DROP INDEX; CONCURRENTLY must not run through sqlx's advisory lock.
-- An operator MAY pre-drop with `DROP INDEX CONCURRENTLY IF EXISTS` through
-- the approved online path; this file is then a no-op.
SET LOCAL lock_timeout = '1s';

DROP INDEX IF EXISTS upstream_subscription_quota_checkpoints_range_idx;
DROP INDEX IF EXISTS upstream_subscription_quota_checkpoints_anchor_idx;
