CREATE TABLE IF NOT EXISTS price_catalog_snapshots_v1 (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    fetched_at_ms BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS price_catalog_snapshots_v1_fetched_at_ms_desc_idx
    ON price_catalog_snapshots_v1 (fetched_at_ms DESC);
