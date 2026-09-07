-- Match the list_cost_* columns already materialized by the SQLite backend.
-- Without these columns, Postgres request_event_principal_costs decodes the
-- payload TOAST value as jsonb for every matching row.
-- NULL means that cost components were not recorded for the event.
SET LOCAL lock_timeout = '1s';

ALTER TABLE request_events_v1
    ADD COLUMN list_cost_usd_micros BIGINT NULL,
    ADD COLUMN list_cost_input_micros BIGINT NULL,
    ADD COLUMN list_cost_output_micros BIGINT NULL,
    ADD COLUMN list_cost_cache_creation_5m_micros BIGINT NULL,
    ADD COLUMN list_cost_cache_creation_1h_micros BIGINT NULL,
    ADD COLUMN list_cost_cache_read_micros BIGINT NULL,
    ADD COLUMN list_cost_components_materialized BOOLEAN NOT NULL DEFAULT FALSE;

-- Old or rolled-back replicas do not send the marker. Materialize their
-- payloads in the database; current writers set the marker and skip the body.
CREATE FUNCTION request_events_v1_materialize_cost_components()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    payload_jsonb JSONB;
BEGIN
    IF NEW.payload IS NOT NULL THEN
        payload_jsonb := convert_from(NEW.payload, 'UTF8')::jsonb;
        NEW.list_cost_usd_micros :=
            (payload_jsonb ->> 'cost_usd_micros')::bigint;
        NEW.list_cost_input_micros :=
            (payload_jsonb ->> 'cost_input_micros')::bigint;
        NEW.list_cost_output_micros :=
            (payload_jsonb ->> 'cost_output_micros')::bigint;
        NEW.list_cost_cache_creation_5m_micros :=
            (payload_jsonb ->> 'cost_cache_creation_5m_micros')::bigint;
        NEW.list_cost_cache_creation_1h_micros :=
            (payload_jsonb ->> 'cost_cache_creation_1h_micros')::bigint;
        NEW.list_cost_cache_read_micros :=
            (payload_jsonb ->> 'cost_cache_read_micros')::bigint;
    END IF;
    NEW.list_cost_components_materialized := TRUE;
    RETURN NEW;
END;
$$;

CREATE TRIGGER request_events_v1_materialize_cost_components
BEFORE INSERT ON request_events_v1
FOR EACH ROW
WHEN (NOT NEW.list_cost_components_materialized)
EXECUTE FUNCTION request_events_v1_materialize_cost_components();
