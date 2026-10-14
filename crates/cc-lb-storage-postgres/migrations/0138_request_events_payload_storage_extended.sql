-- Reverts 0136. STORAGE MAIN applied to every deployment, including those with
-- request_event_strip_diagnostics off, keeps full 2-8 KB compressed payloads
-- inline, which bloats the heap and slows every query that scans it. Restore
-- the default EXTENDED storage; operators switch to MAIN by hand only after
-- the strip is on in every pod (docs/runbook/request-diagnostics.md, step 4).
--
-- Catalog-only change: no table rewrite, a brief ACCESS EXCLUSIVE lock bounded
-- by lock_timeout. Existing values keep their current storage; values stored
-- inline under MAIN stay inline until they are rewritten.
SET LOCAL lock_timeout = '1s';

ALTER TABLE request_events_v1 ALTER COLUMN payload SET STORAGE EXTENDED;
