-- Safe to DROP: contents are a cache that LiteLlmLoader refetches on the next refresh tick.
DROP TABLE IF EXISTS price_catalog_snapshots_v1;

CREATE TABLE price_catalog_snapshots_v1 (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    fetched_at_ms BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX price_catalog_snapshots_v1_fetched_at_ms_desc_idx
    ON price_catalog_snapshots_v1 (fetched_at_ms DESC);

CREATE UNIQUE INDEX price_catalog_snapshots_v1_payload_hash_uidx
    ON price_catalog_snapshots_v1 (payload_hash);
