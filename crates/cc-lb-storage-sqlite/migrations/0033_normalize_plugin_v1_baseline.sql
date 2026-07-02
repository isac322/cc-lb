UPDATE wasm_registry_v2
   SET wire_version = 1
 WHERE wire_version != 1;

UPDATE plugin_chains_v2
   SET wire_version = 1
 WHERE wire_version IS NOT NULL AND wire_version != 1;

DELETE FROM plugin_registry_marker_v1 WHERE key LIKE '%:wire_version';
