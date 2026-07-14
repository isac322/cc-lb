ALTER TABLE wasm_registry_v2 ADD COLUMN IF NOT EXISTS plugin_version TEXT NULL;

ALTER TABLE wasm_blobs_v2 DROP COLUMN IF EXISTS refcount;

CREATE INDEX IF NOT EXISTS upstream_spec_v1_warmup_wasm_registry_id_idx
    ON upstream_spec_v1 (((warmup_dialect_plugin->>'wasm_registry_id')::uuid))
    WHERE deleted_at IS NULL AND warmup_dialect_plugin IS NOT NULL;
