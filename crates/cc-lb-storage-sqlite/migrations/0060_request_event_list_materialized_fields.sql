ALTER TABLE request_events_v1 ADD COLUMN list_ts_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE request_events_v1 ADD COLUMN list_event_key TEXT NOT NULL DEFAULT '';
ALTER TABLE request_events_v1 ADD COLUMN list_upstream TEXT NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_status INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_duration_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_auth_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_route_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_limit_reserve_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_bulkhead_wait_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_dns_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_connect_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_connection_reused INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_limit_reconcile_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_observability_post_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_proxy_setup_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_shape_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_sign_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_upstream_ttfb_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_upstream_body_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_stream_first_content_delta_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_stream_last_content_delta_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_inter_token_avg_ms INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_cost_usd_micros INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_cost_input_micros INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_cost_output_micros INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_cost_cache_creation_5m_micros INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_cost_cache_creation_1h_micros INTEGER NULL;
ALTER TABLE request_events_v1 ADD COLUMN list_cost_cache_read_micros INTEGER NULL;

UPDATE request_events_v1
   SET list_ts_ms = COALESCE(CAST(json_extract(payload, '$.ts_ms') AS INTEGER), ts * 1000),
       list_event_key = COALESCE(event_id, request_id),
       list_upstream = json_extract(payload, '$.upstream'),
       list_status = CAST(json_extract(payload, '$.status') AS INTEGER),
       list_duration_ms = CAST(json_extract(payload, '$.duration_ms') AS INTEGER),
       list_auth_ms = CAST(json_extract(payload, '$.auth_ms') AS INTEGER),
       list_route_ms = CAST(json_extract(payload, '$.route_ms') AS INTEGER),
       list_limit_reserve_ms = CAST(json_extract(payload, '$.limit_reserve_ms') AS INTEGER),
       list_bulkhead_wait_ms = CAST(json_extract(payload, '$.bulkhead_wait_ms') AS INTEGER),
       list_dns_ms = CAST(json_extract(payload, '$.dns_ms') AS INTEGER),
       list_connect_ms = CAST(json_extract(payload, '$.connect_ms') AS INTEGER),
       list_connection_reused = CASE json_extract(payload, '$.connection_reused')
           WHEN 1 THEN 1
           WHEN 0 THEN 0
           WHEN 'true' THEN 1
           WHEN 'false' THEN 0
           ELSE NULL
       END,
       list_limit_reconcile_ms = CAST(json_extract(payload, '$.limit_reconcile_ms') AS INTEGER),
       list_observability_post_ms = CAST(json_extract(payload, '$.observability_post_ms') AS INTEGER),
       list_proxy_setup_ms = CAST(json_extract(payload, '$.proxy_setup_ms') AS INTEGER),
       list_shape_ms = CAST(json_extract(payload, '$.shape_ms') AS INTEGER),
       list_sign_ms = CAST(json_extract(payload, '$.sign_ms') AS INTEGER),
       list_upstream_ttfb_ms = CAST(json_extract(payload, '$.upstream_ttfb_ms') AS INTEGER),
       list_upstream_body_ms = CAST(json_extract(payload, '$.upstream_body_ms') AS INTEGER),
       list_stream_first_content_delta_ms = CAST(json_extract(payload, '$.stream_first_content_delta_ms') AS INTEGER),
       list_stream_last_content_delta_ms = CAST(json_extract(payload, '$.stream_last_content_delta_ms') AS INTEGER),
       list_inter_token_avg_ms = CAST(json_extract(payload, '$.inter_token_avg_ms') AS INTEGER),
       list_cost_usd_micros = CAST(json_extract(payload, '$.cost_usd_micros') AS INTEGER),
       list_cost_input_micros = CAST(json_extract(payload, '$.cost_input_micros') AS INTEGER),
       list_cost_output_micros = CAST(json_extract(payload, '$.cost_output_micros') AS INTEGER),
       list_cost_cache_creation_5m_micros = CAST(json_extract(payload, '$.cost_cache_creation_5m_micros') AS INTEGER),
       list_cost_cache_creation_1h_micros = CAST(json_extract(payload, '$.cost_cache_creation_1h_micros') AS INTEGER),
       list_cost_cache_read_micros = CAST(json_extract(payload, '$.cost_cache_read_micros') AS INTEGER);

CREATE INDEX IF NOT EXISTS request_events_v1_list_order_idx
    ON request_events_v1 (list_ts_ms DESC, list_event_key DESC, id DESC);

CREATE INDEX IF NOT EXISTS request_events_v1_principal_list_order_idx
    ON request_events_v1 (principal_id, list_ts_ms DESC, list_event_key DESC, id DESC);

CREATE INDEX IF NOT EXISTS request_events_v1_model_list_order_idx
    ON request_events_v1 (model, list_ts_ms DESC, list_event_key DESC, id DESC);

CREATE INDEX IF NOT EXISTS request_events_v1_upstream_list_order_idx
    ON request_events_v1 (upstream_id, list_ts_ms DESC, list_event_key DESC, id DESC);

CREATE INDEX IF NOT EXISTS request_events_v1_principal_key_ts_idx
    ON request_events_v1 (principal_id, key_id, ts);

CREATE INDEX IF NOT EXISTS request_events_v1_principal_key_usage_idx
    ON request_events_v1 (principal_id, key_id, list_ts_ms);
