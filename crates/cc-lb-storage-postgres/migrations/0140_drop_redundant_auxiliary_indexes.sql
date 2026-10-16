SET LOCAL lock_timeout = '1s';

DROP INDEX IF EXISTS usage_rollups_v2_interval_tokens_cover_idx;
DROP INDEX IF EXISTS audit_log_v1_actor_subject;
