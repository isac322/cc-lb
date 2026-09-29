PRAGMA foreign_keys = OFF;
PRAGMA defer_foreign_keys = ON;
PRAGMA legacy_alter_table = ON;

DELETE FROM plugin_chains_v2
WHERE slot = 'observability_hook';

DROP INDEX IF EXISTS plugin_chains_v2_principal_slot_order_idx;
DROP INDEX IF EXISTS plugin_chains_v2_wasm_registry_idx;

ALTER TABLE plugin_chains_v2 RENAME TO plugin_chains_v2_old;

CREATE TABLE plugin_chains_v2 (
    id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL,
    slot TEXT NOT NULL CHECK (slot IN ('router', 'shape')),
    wasm_registry_id BLOB NOT NULL REFERENCES wasm_registry_v2(id) ON DELETE RESTRICT,
    order_value INTEGER NOT NULL,
    config TEXT NOT NULL DEFAULT '{}',
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
    revision,
    created_at,
    updated_at
FROM plugin_chains_v2_old;

CREATE INDEX IF NOT EXISTS plugin_chains_v2_principal_slot_order_idx
    ON plugin_chains_v2 (principal_id, slot, order_value, id);
CREATE INDEX IF NOT EXISTS plugin_chains_v2_wasm_registry_idx
    ON plugin_chains_v2 (wasm_registry_id);

DROP TABLE plugin_chains_v2_old;

UPDATE wasm_registry_v2
   SET supported_slots = (
       SELECT json_group_array(slot_value)
         FROM (
             SELECT value AS slot_value
               FROM json_each(wasm_registry_v2.supported_slots)
              WHERE value <> 'observability_hook'
              ORDER BY key
         )
   )
 WHERE EXISTS (
       SELECT 1
         FROM json_each(wasm_registry_v2.supported_slots)
        WHERE value = 'observability_hook'
   );

UPDATE wasm_registry_v2
   SET hook_metadata = json_remove(hook_metadata, '$.observe')
 WHERE json_type(hook_metadata, '$.observe') IS NOT NULL;

PRAGMA legacy_alter_table = OFF;
PRAGMA foreign_keys = ON;
