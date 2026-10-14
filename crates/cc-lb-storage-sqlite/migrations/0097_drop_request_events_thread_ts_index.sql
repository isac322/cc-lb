-- request_events_v1_thread_ts is redundant: the thread filter and the
-- histogram are served by request_events_v1_thread_list_order_idx. Mirrors
-- PostgreSQL 0134.
DROP INDEX IF EXISTS request_events_v1_thread_ts;
