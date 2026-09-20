-- Materialized endpoint classification for request_events_v1.
--
-- Nullable on purpose: rows written before this migration keep NULL and are
-- classified as `unclassified` at read time (renewal source_kind still wins).
-- No backfill — historical payloads carry no request path, so guessing would
-- fabricate metadata. New writes bind the classified kind.
ALTER TABLE request_events_v1 ADD COLUMN event_kind TEXT NULL;

-- Expression index so an explicit event_kind filter seeks straight into the
-- list ordering instead of scanning every row in the time window. The CASE
-- text must structurally match the effective-kind expression emitted by the
-- list/histogram WHERE clauses — SQLite only uses an expression index on an
-- exact match, and cannot index the `? IS NULL OR expr = ?` optional form.
CREATE INDEX IF NOT EXISTS request_events_v1_event_kind_list_order_idx
    ON request_events_v1 (
        (CASE WHEN source_kind = 'renewal' THEN 'renewal'
              ELSE COALESCE(event_kind, 'unclassified') END),
        list_ts_ms DESC,
        list_event_key DESC,
        id DESC
    );
