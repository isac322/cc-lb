-- Add `last_warmup_at` (unix seconds) to `upstream_status_v1`.
--
-- Distinct from `last_warmup_cycle_key`, which encodes the 5h-reset boundary
-- (or scheduled fire time) used for warmup idempotency. `last_warmup_at`
-- captures the wall-clock time the most recent successful warmup HTTP call
-- completed, so the admin UI can show "Last cycle ran <X> ago" instead of
-- "Last cycle ran <future 5h reset boundary>".
ALTER TABLE upstream_status_v1 ADD COLUMN last_warmup_at INTEGER;
