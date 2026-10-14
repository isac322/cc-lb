-- request_events_v1_lower_model_list_order_idx (0105) is never chosen by the
-- planner for the request-log model filter: the needle's prefix arm and its
-- `claude-%c%` arm form a disjunction that the planner serves from the list
-- order index (tests/request_event_filter_index_plans.rs is the gate). It only
-- adds write amplification.
-- Plain DROP INDEX; CONCURRENTLY must not run through sqlx's advisory lock.
-- An operator MAY pre-drop with `DROP INDEX CONCURRENTLY IF EXISTS` through
-- the approved online path; this file is then a no-op.
SET LOCAL lock_timeout = '1s';

DROP INDEX IF EXISTS request_events_v1_lower_model_list_order_idx;
