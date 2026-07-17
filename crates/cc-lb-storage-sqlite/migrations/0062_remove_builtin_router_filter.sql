DELETE FROM plugin_chains_v2
 WHERE wasm_registry_id = '00000000-0000-0000-0000-000000000001';

DELETE FROM wasm_registry_v2
 WHERE id = '00000000-0000-0000-0000-000000000001';

DELETE FROM wasm_blobs_v2
 WHERE sha256 = zeroblob(32)
   AND NOT EXISTS (
       SELECT 1
         FROM wasm_registry_v2
        WHERE wasm_registry_v2.sha256 = wasm_blobs_v2.sha256
   );

-- Registry markers for the builtin cache-affinity row: all-zero sha256 hex key
-- and the fixed builtin id 00000000-0000-0000-0000-000000000001.
DELETE FROM plugin_registry_marker_v1
 WHERE key LIKE 'wasm_registry:0000000000000000000000000000000000000000000000000000000000000000:%'
    OR key = 'wasm_registry_id:00000000-0000-0000-0000-000000000001:sha256';
