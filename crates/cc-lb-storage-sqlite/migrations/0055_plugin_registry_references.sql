ALTER TABLE wasm_registry_v2 RENAME COLUMN plugin_version TO original_filename;
ALTER TABLE wasm_registry_v2 ADD COLUMN plugin_version TEXT;

ALTER TABLE wasm_blobs_v2 DROP COLUMN refcount;

CREATE INDEX IF NOT EXISTS upstream_spec_v1_warmup_wasm_registry_id_idx
    ON upstream_spec_v1 (json_extract(warmup_dialect_plugin, '$.wasm_registry_id'))
    WHERE deleted_at IS NULL AND warmup_dialect_plugin IS NOT NULL;
