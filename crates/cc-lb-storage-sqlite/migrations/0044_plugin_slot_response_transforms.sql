PRAGMA foreign_keys = OFF;
PRAGMA defer_foreign_keys = ON;
PRAGMA legacy_alter_table = ON;

DROP INDEX IF EXISTS plugin_chains_v2_principal_slot_order_idx;
DROP INDEX IF EXISTS plugin_chains_v2_wasm_registry_idx;

ALTER TABLE plugin_chains_v2 RENAME TO plugin_chains_v2_old;

CREATE TABLE plugin_chains_v2 (
    id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL,
    slot TEXT NOT NULL CHECK (slot IN ('router', 'observability_hook', 'shape', 'transform_response', 'transform_sse_event')),
    wasm_registry_id BLOB NOT NULL REFERENCES wasm_registry_v2(id) ON DELETE RESTRICT,
    order_value INTEGER NOT NULL,
    config TEXT NOT NULL DEFAULT '{}',
    sse_per_event INTEGER NOT NULL DEFAULT 0 CHECK (sse_per_event IN (0, 1)),
    batched_events_per_flush INTEGER NOT NULL DEFAULT 1 CHECK (batched_events_per_flush >= 0),
    batched_flush_ms INTEGER NOT NULL DEFAULT 100 CHECK (batched_flush_ms >= 0),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision >= 0),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

INSERT INTO plugin_chains_v2 (
    id,
    principal_id,
    slot,
    wasm_registry_id,
    order_value,
    config,
    sse_per_event,
    batched_events_per_flush,
    batched_flush_ms,
    revision,
    created_at,
    updated_at
)
SELECT
    id,
    principal_id,
    slot,
    wasm_registry_id,
    order_value,
    config,
    sse_per_event,
    batched_events_per_flush,
    batched_flush_ms,
    revision,
    created_at,
    updated_at
FROM plugin_chains_v2_old;

CREATE INDEX IF NOT EXISTS plugin_chains_v2_principal_slot_order_idx
    ON plugin_chains_v2 (principal_id, slot, order_value, id);
CREATE INDEX IF NOT EXISTS plugin_chains_v2_wasm_registry_idx
    ON plugin_chains_v2 (wasm_registry_id);

DROP TABLE plugin_chains_v2_old;

PRAGMA legacy_alter_table = OFF;
PRAGMA foreign_keys = ON;
