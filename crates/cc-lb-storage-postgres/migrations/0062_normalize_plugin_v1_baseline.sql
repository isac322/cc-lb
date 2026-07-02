UPDATE wasm_registry_v2
   SET wire_version = 1
 WHERE wire_version IS DISTINCT FROM 1;

UPDATE plugin_chains_v2
   SET wire_version = 1
 WHERE wire_version IS DISTINCT FROM 1;
