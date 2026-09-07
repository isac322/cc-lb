-- Replace the existing principal list-order index with a covering form. The key
-- columns and ordering contract stay unchanged; included cost columns let
-- request_event_principal_costs avoid heap and TOAST reads.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '90s';

DROP INDEX IF EXISTS request_events_v1_principal_list_order_idx;

CREATE INDEX request_events_v1_principal_list_order_idx
    ON request_events_v1 (principal_id, list_ts_ms DESC, list_event_key DESC)
    INCLUDE (
        list_cost_usd_micros,
        list_cost_input_micros,
        list_cost_output_micros,
        list_cost_cache_creation_5m_micros,
        list_cost_cache_creation_1h_micros,
        list_cost_cache_read_micros
    );
