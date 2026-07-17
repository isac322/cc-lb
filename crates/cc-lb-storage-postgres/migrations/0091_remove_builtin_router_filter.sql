DELETE FROM plugin_chains_v2
 WHERE wasm_registry_id = '00000000-0000-0000-0000-000000000001'::uuid;

DELETE FROM wasm_registry_v2
 WHERE id = '00000000-0000-0000-0000-000000000001'::uuid;

DELETE FROM wasm_blobs_v2
 WHERE sha256 = decode(repeat('00', 32), 'hex')
   AND NOT EXISTS (
       SELECT 1
         FROM wasm_registry_v2
        WHERE wasm_registry_v2.sha256 = wasm_blobs_v2.sha256
   );
