CREATE TABLE IF NOT EXISTS wasm_blobs_v2 (
    sha256 BYTEA PRIMARY KEY CHECK (length(sha256) = 32),
    bytes BYTEA NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0 AND size_bytes <= 33554432),
    parse_validated_at TIMESTAMPTZ NOT NULL,
    refcount BIGINT NOT NULL DEFAULT 0 CHECK (refcount >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wasm_registry_v2 (
    id UUID PRIMARY KEY,
    sha256 BYTEA NOT NULL CHECK (length(sha256) = 32),
    name TEXT NOT NULL UNIQUE,
    original_filename TEXT NOT NULL,
    label TEXT,
    uploaded_at TIMESTAMPTZ NOT NULL,
    uploaded_by_admin_id UUID NOT NULL,
    revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0),
    CONSTRAINT wasm_registry_v2_sha_fk FOREIGN KEY (sha256)
        REFERENCES wasm_blobs_v2 (sha256) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX IF NOT EXISTS wasm_registry_v2_sha_idx ON wasm_registry_v2 (sha256);
CREATE INDEX IF NOT EXISTS wasm_registry_v2_name_idx ON wasm_registry_v2 (name);

CREATE TABLE IF NOT EXISTS plugin_chains_v2 (
    id UUID PRIMARY KEY,
    principal_id UUID NOT NULL,
    slot TEXT NOT NULL CHECK (slot IN ('router', 'observability_hook')),
    order_value BIGINT NOT NULL,
    wasm_registry_id UUID NOT NULL,
    config JSONB NOT NULL,
    sse_per_event BOOLEAN NOT NULL,
    batched_events_per_flush INTEGER NOT NULL CHECK (batched_events_per_flush >= 0),
    batched_flush_ms BIGINT NOT NULL CHECK (batched_flush_ms >= 0),
    revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0),
    CONSTRAINT plugin_chains_v2_wasm_registry_fk FOREIGN KEY (wasm_registry_id)
        REFERENCES wasm_registry_v2 (id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS plugin_chains_v2_principal_slot_order_idx
    ON plugin_chains_v2 (principal_id, slot, order_value);
CREATE INDEX IF NOT EXISTS plugin_chains_v2_wasm_registry_idx
    ON plugin_chains_v2 (wasm_registry_id);
