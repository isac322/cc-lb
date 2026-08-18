-- Canonical UUID principals use request_events_v1_principal_list_order_idx.
-- Keep a smaller normalization-aware index for legacy, missing, or malformed
-- principal IDs so dashboard cost enrichment preserves rollup semantics without
-- normalizing every canonical request-event row.
CREATE INDEX IF NOT EXISTS request_events_v1_normalized_non_uuid_principal_cost_idx
    ON request_events_v1 (
        (LEFT(REGEXP_REPLACE(COALESCE(NULLIF(BTRIM(principal_id COLLATE "C", U&'\0009\000A\000B\000C\000D\0020\0085\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000'), ''), 'unknown'),
            '[^A-Za-z0-9_.:@-]', '_', 'g'), 64)),
        list_ts_ms DESC,
        list_event_key DESC
    )
    WHERE principal_id IS NULL
       OR principal_id !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
