-- DashboardRange::SevenDays is the largest reachable dashboard window. Backfill
-- ten days so every reachable bucket is populated without decoding the full
-- historical payload table during startup.
WITH decoded AS MATERIALIZED (
    SELECT
        seq,
        convert_from(payload, 'UTF8')::jsonb AS payload_jsonb
    FROM request_events_v1
    WHERE payload IS NOT NULL
      AND NOT list_cost_components_materialized
      AND list_ts_ms >= (EXTRACT(EPOCH FROM now())::bigint - 10 * 86400) * 1000
)
UPDATE request_events_v1 AS events
SET list_cost_usd_micros = (decoded.payload_jsonb ->> 'cost_usd_micros')::bigint,
    list_cost_input_micros = (decoded.payload_jsonb ->> 'cost_input_micros')::bigint,
    list_cost_output_micros = (decoded.payload_jsonb ->> 'cost_output_micros')::bigint,
    list_cost_cache_creation_5m_micros =
        (decoded.payload_jsonb ->> 'cost_cache_creation_5m_micros')::bigint,
    list_cost_cache_creation_1h_micros =
        (decoded.payload_jsonb ->> 'cost_cache_creation_1h_micros')::bigint,
    list_cost_cache_read_micros =
        (decoded.payload_jsonb ->> 'cost_cache_read_micros')::bigint,
    list_cost_components_materialized = TRUE
FROM decoded
WHERE events.seq = decoded.seq;
