-- Store the price-catalog snapshot as raw validated JSON text (matching the
-- SQLite backend) instead of jsonb. The payload is never queried structurally,
-- and text lets the adapter store/return the original bytes without a
-- serde_json Value serialize/deserialize round-trip.
ALTER TABLE price_catalog_snapshots_v1
    ALTER COLUMN payload TYPE text USING payload::text;
