SELECT principal_id,
       ((list_ts_ms - $1) / $2)::bigint AS bucket_index,
       list_cost_usd_micros,
       list_cost_input_micros,
       list_cost_output_micros,
       list_cost_cache_creation_5m_micros,
       list_cost_cache_creation_1h_micros,
       list_cost_cache_read_micros
FROM request_events_v1
WHERE list_ts_ms >= $1
  AND list_ts_ms < $3
  AND principal_id = ANY($4::text[])
