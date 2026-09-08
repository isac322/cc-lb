-- Replace the principal and upstream request-event indexes with covering forms.
-- Key prefixes retain existing lookup contracts while principal-cost queries
-- can read upstream and cost data without heap or TOAST access.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '90s';

DROP INDEX IF EXISTS request_events_v1_principal_list_order_idx;
DROP INDEX IF EXISTS request_events_v1_normalized_non_uuid_principal_cost_idx;
DROP INDEX IF EXISTS request_events_v1_upstream_id_idx;

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

CREATE INDEX request_events_v1_normalized_non_uuid_principal_cost_idx
    ON request_events_v1 (
        (LEFT(REGEXP_REPLACE(COALESCE(NULLIF(BTRIM(principal_id COLLATE "C", U&'\0009\000A\000B\000C\000D\0020\0085\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000'), ''), 'unknown'),
            '[^A-Za-z0-9_.:@-]', '_', 'g'), 64)),
        list_ts_ms DESC,
        list_event_key DESC
    )
    INCLUDE (
        principal_id,
        upstream_id,
        list_cost_usd_micros,
        list_cost_input_micros,
        list_cost_output_micros,
        list_cost_cache_creation_5m_micros,
        list_cost_cache_creation_1h_micros,
        list_cost_cache_read_micros
    )
    WHERE principal_id IS NULL
       OR principal_id !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

CREATE INDEX request_events_v1_upstream_id_idx
    ON request_events_v1 (
        upstream_id,
        principal_id,
        list_ts_ms DESC
    )
    INCLUDE (
        list_cost_usd_micros,
        list_cost_input_micros,
        list_cost_output_micros,
        list_cost_cache_creation_5m_micros,
        list_cost_cache_creation_1h_micros,
        list_cost_cache_read_micros
    );
