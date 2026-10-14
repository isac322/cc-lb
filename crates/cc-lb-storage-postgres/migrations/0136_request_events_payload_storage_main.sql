-- Diagnostics leave the payload (request_event_strip_diagnostics); keep the
-- remaining ~2 KB payloads inline so the TOAST relation can drain to empty.
--
-- Catalog-only change: no table rewrite, a brief ACCESS EXCLUSIVE lock bounded
-- by lock_timeout. Existing values keep their current storage. New and updated
-- payload values are compressed and kept inline, and move out of line only
-- when the tuple still exceeds TOAST_TUPLE_MAXIMUM (~8 KB) after compression.
SET LOCAL lock_timeout = '1s';

ALTER TABLE request_events_v1 ALTER COLUMN payload SET STORAGE MAIN;
