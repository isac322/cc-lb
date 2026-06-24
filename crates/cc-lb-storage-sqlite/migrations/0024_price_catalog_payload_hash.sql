-- Safe to DROP: contents are a cache that LiteLlmLoader refetches on the next refresh tick.
DROP TABLE IF EXISTS price_catalog_snapshots_v1;

CREATE TABLE price_catalog_snapshots_v1 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    payload TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    fetched_at_ms INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX price_catalog_snapshots_v1_payload_hash_uidx
    ON price_catalog_snapshots_v1 (payload_hash);
