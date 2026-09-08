SELECT LEFT(REGEXP_REPLACE(COALESCE(NULLIF(
           BTRIM(principal_id COLLATE "C", U&'\0009\000A\000B\000C\000D\0020\0085\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000'),
           ''), 'unknown'), '[^A-Za-z0-9_.:@-]', '_', 'g'), 64) AS principal_id,
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
  AND (principal_id IS NULL OR principal_id !~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
  AND LEFT(REGEXP_REPLACE(COALESCE(NULLIF(
      BTRIM(principal_id COLLATE "C", U&'\0009\000A\000B\000C\000D\0020\0085\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000'),
      ''), 'unknown'), '[^A-Za-z0-9_.:@-]', '_', 'g'), 64) = ANY($4::text[])
