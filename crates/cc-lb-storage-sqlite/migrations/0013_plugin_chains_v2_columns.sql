PRAGMA foreign_keys = OFF;
PRAGMA defer_foreign_keys = ON;
PRAGMA legacy_alter_table = ON;

DROP INDEX IF EXISTS plugin_chains_v2_principal_slot_order_idx;
DROP INDEX IF EXISTS plugin_chains_v2_wasm_sha256_idx;

ALTER TABLE plugin_chains_v2 RENAME TO plugin_chains_v2_old;

CREATE TABLE plugin_chains_v2 (
    id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL,
    slot TEXT NOT NULL CHECK (slot IN ('router', 'observability_hook', 'shape')),
    wasm_registry_id BLOB NOT NULL REFERENCES wasm_registry_v2(id) ON DELETE RESTRICT,
    order_value INTEGER NOT NULL,
    config TEXT NOT NULL DEFAULT '{}',
    sse_per_event INTEGER NOT NULL DEFAULT 0 CHECK (sse_per_event IN (0, 1)),
    batched_events_per_flush INTEGER NOT NULL DEFAULT 1 CHECK (batched_events_per_flush >= 0),
    batched_flush_ms INTEGER NOT NULL DEFAULT 100 CHECK (batched_flush_ms >= 0),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision >= 0),
    wire_version INTEGER CHECK (wire_version IS NULL OR (wire_version >= 1 AND wire_version <= 255)),
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
    wire_version,
    created_at,
    updated_at
)
SELECT
    old.id,
    old.principal_id,
    old.slot,
    COALESCE(
        json_extract(old.config, '$.wasm_registry_id'),
        (SELECT registry.id FROM wasm_registry_v2 registry WHERE registry.sha256 = old.wasm_sha256)
    ),
    old.order_index,
    COALESCE(json_extract(old.config, '$.config'), old.config, '{}'),
    COALESCE(CAST(json_extract(old.config, '$.sse_per_event') AS INTEGER), 0),
    COALESCE(CAST(json_extract(old.config, '$.batched_events_per_flush') AS INTEGER), 1),
    COALESCE(CAST(json_extract(old.config, '$.batched_flush_ms') AS INTEGER), 100),
    old.revision,
    CAST(json_extract(old.config, '$.wire_version') AS INTEGER),
    old.created_at,
    old.updated_at
FROM plugin_chains_v2_old old;

CREATE INDEX IF NOT EXISTS plugin_chains_v2_principal_slot_order_idx
    ON plugin_chains_v2 (principal_id, slot, order_value, id);
CREATE INDEX IF NOT EXISTS plugin_chains_v2_wasm_registry_idx
    ON plugin_chains_v2 (wasm_registry_id);

UPDATE wasm_blobs_v2
   SET refcount = (
       SELECT COUNT(*)
         FROM plugin_chains_v2
         JOIN wasm_registry_v2 ON wasm_registry_v2.id = plugin_chains_v2.wasm_registry_id
        WHERE wasm_registry_v2.sha256 = wasm_blobs_v2.sha256
   );

DROP TABLE plugin_chains_v2_old;
DROP TABLE wasm_registry_v2_old;
DROP TABLE wasm_blobs_v2_old;

PRAGMA legacy_alter_table = OFF;
