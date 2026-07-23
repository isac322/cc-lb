ALTER TABLE request_events_v1
    ADD COLUMN list_ts_ms BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN list_event_key TEXT NOT NULL DEFAULT '',
    ADD COLUMN list_upstream TEXT NULL,
    ADD COLUMN list_status INTEGER NULL;

WITH decoded AS MATERIALIZED (
    SELECT
        seq,
        CASE
            WHEN payload IS NULL THEN NULL
            ELSE convert_from(payload, 'UTF8')::jsonb
        END AS payload_jsonb
    FROM request_events_v1
)
UPDATE request_events_v1 AS events
SET list_ts_ms = COALESCE(
        (decoded.payload_jsonb ->> 'ts_ms')::bigint,
        EXTRACT(EPOCH FROM events.ts)::bigint * 1000
    ),
    list_event_key = events.event_id,
    list_upstream = decoded.payload_jsonb ->> 'upstream',
    list_status = (decoded.payload_jsonb ->> 'status')::integer
FROM decoded
WHERE events.seq = decoded.seq;

CREATE INDEX request_events_v1_list_order_idx
    ON request_events_v1 (list_ts_ms DESC, list_event_key DESC);

CREATE INDEX request_events_v1_principal_list_order_idx
    ON request_events_v1 (principal_id, list_ts_ms DESC, list_event_key DESC);

CREATE INDEX request_events_v1_model_list_order_idx
    ON request_events_v1 (model, list_ts_ms DESC, list_event_key DESC);

CREATE INDEX request_events_v1_upstream_list_order_idx
    ON request_events_v1 (upstream_id, list_ts_ms DESC, list_event_key DESC);
