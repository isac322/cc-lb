DELETE FROM plugin_chains_v2 c
WHERE c.slot = 'router'
  AND c.wasm_registry_id <> '00000000-0000-0000-0000-000000000001'::uuid
  AND (c.wire_version IS NULL OR c.wire_version <> 3);
