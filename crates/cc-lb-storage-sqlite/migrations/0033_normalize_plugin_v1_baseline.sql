UPDATE wasm_registry_v2
   SET wire_version = 1
 WHERE wire_version != 1;

UPDATE plugin_chains_v2
   SET wire_version = 1
 WHERE wire_version IS NOT NULL AND wire_version != 1;

DELETE FROM plugin_registry_marker_v1 WHERE key LIKE '%:wire_version';

ALTER TABLE wasm_registry_v2 DROP COLUMN abi_envelope;
ALTER TABLE wasm_registry_v2 DROP COLUMN augmented_metadata;
ALTER TABLE wasm_registry_v2 DROP COLUMN host_offer_hash;
ALTER TABLE wasm_registry_v2 DROP COLUMN handshake_schema_version;
ALTER TABLE wasm_registry_v2 DROP COLUMN last_handshake_at;
