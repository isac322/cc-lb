ALTER TABLE wasm_registry_v2 ADD COLUMN description TEXT NOT NULL DEFAULT '';
ALTER TABLE wasm_registry_v2 ADD COLUMN usage TEXT NOT NULL DEFAULT '';
ALTER TABLE wasm_registry_v2 ADD COLUMN hook_metadata TEXT NOT NULL DEFAULT '{}';
ALTER TABLE wasm_registry_v2 DROP COLUMN wire_version;
ALTER TABLE plugin_chains_v2 DROP COLUMN wire_version;
