-- Family A (xid8 snapshot horizon) rollup cursor.
--
-- `seq` (BIGSERIAL) is assigned at INSERT time but only becomes visible at
-- COMMIT time, so a row with a lower seq can commit after a row with a higher
-- seq. A seq-ordered high-water-mark that advances past the higher seq would
-- then skip the lower one forever. Advancing the checkpoint in transaction-id
-- space instead -- the same space the MVCC visibility horizon
-- (pg_snapshot_xmin) lives in -- cannot skip a settled row, because xmin never
-- advances past an in-flight transaction. To resume across runs we must persist
-- both components of the composite (tx_id, seq) cursor.
ALTER TABLE usage_rollup_checkpoints_v1
    ADD COLUMN IF NOT EXISTS value_xid xid8 NOT NULL DEFAULT '0'::xid8;

-- The pre-existing checkpoint stored only a seq high-water-mark, which is not a
-- valid (tx_id, seq) cursor: pairing it with value_xid = 0 would re-scan every
-- real row (tx_id > 0) and double-count it into the additive rollups. Reset the
-- cursor to (0, 0) AND clear the additive aggregates in the same step, so the
-- first run after this migration rebuilds usage_rollups_v2 from a clean slate
-- instead of adding a full re-scan on top of existing totals. Safe because
-- Postgres is not deployed anywhere; fresh databases have no rows in either
-- table, so both deletes are no-ops there.
DELETE FROM usage_rollup_checkpoints_v1 WHERE id = 'high_water';
DELETE FROM usage_rollups_v2;

-- Support the composite cursor scan: range + ORDER BY on (tx_id, seq).
CREATE INDEX IF NOT EXISTS request_events_v1_tx_id_seq_idx
    ON request_events_v1 (tx_id, seq);
