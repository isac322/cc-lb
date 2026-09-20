-- Nullable endpoint classification for request events. New writes bind the
-- exact endpoint kind; historical rows stay NULL and read as "unclassified"
-- (or "renewal" via the source_kind precedence), never inferred or backfilled.
ALTER TABLE request_events_v1 ADD COLUMN IF NOT EXISTS event_kind TEXT NULL;

-- Effective-kind list filter: the query predicate is the CASE expression
-- below (renewal source wins, NULL event_kind reads as 'unclassified'), so
-- the index must carry that exact expression for the planner to match it.
-- Trailing columns serve the existing list ordering and cursor tie-break.
CREATE INDEX IF NOT EXISTS request_events_v1_effective_kind_list_order_idx
    ON request_events_v1 (
        (CASE WHEN source_kind = 'renewal' THEN 'renewal'
              ELSE COALESCE(event_kind, 'unclassified') END),
        list_ts_ms DESC,
        list_event_key DESC
    );
