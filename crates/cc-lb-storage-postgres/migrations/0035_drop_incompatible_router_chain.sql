DELETE FROM plugin_chains_v2 c
WHERE c.slot = 'router'
  AND c.wasm_registry_id <> '00000000-0000-0000-0000-000000000001'::uuid
  AND NOT EXISTS (
      SELECT 1
      FROM wasm_registry_v2 r
      WHERE r.id = c.wasm_registry_id
        AND r.kind = 'filter'
        AND r.wire_version = 3
  );
