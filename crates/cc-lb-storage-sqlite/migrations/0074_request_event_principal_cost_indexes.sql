-- Canonical UUID principals use request_events_v1_principal_list_order_idx.
-- Legacy, missing, padded, or malformed principal IDs stay on a bounded
-- fallback index and are normalized by Rust after aggregation.
CREATE INDEX IF NOT EXISTS request_events_v1_non_uuid_principal_cost_idx
    ON request_events_v1 (list_ts_ms DESC, principal_id, list_event_key DESC, id DESC)
    WHERE principal_id IS NULL
       OR length(principal_id) <> 36
       OR length(replace(principal_id, '-', '')) <> 32
       OR substr(principal_id, 9, 1) <> '-'
       OR substr(principal_id, 14, 1) <> '-'
       OR substr(principal_id, 19, 1) <> '-'
       OR substr(principal_id, 24, 1) <> '-'
       OR lower(replace(principal_id, '-', '')) GLOB '*[^0-9a-f]*';
