-- request_events_v1_thread_ts (0027) is redundant: the thread filter and the
-- histogram are served by request_events_v1_thread_list_order_idx
-- (thread_id, list_ts_ms DESC, list_event_key DESC), so the old index only
-- adds write amplification on the busiest table.
-- Plain DROP INDEX; CONCURRENTLY must not run through sqlx's advisory lock.
-- An operator MAY pre-drop with `DROP INDEX CONCURRENTLY IF EXISTS` through
-- the approved online path; this file is then a no-op.
SET LOCAL lock_timeout = '1s';

DROP INDEX IF EXISTS request_events_v1_thread_ts;
