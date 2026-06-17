DELETE FROM plugin_chains_v2
WHERE slot = 'router'
  AND EXISTS (
      SELECT 1
      FROM wasm_blobs_v2 AS blob
      WHERE blob.sha256 = plugin_chains_v2.wasm_sha256
        AND length(blob.bytes) = 0
  )
  AND EXISTS (
      SELECT 1
      FROM plugin_registry_marker_v1 AS marker
      WHERE marker.key = 'wasm_registry:' || lower(hex(plugin_chains_v2.wasm_sha256)) || ':id'
        AND marker.value = '00000000-0000-0000-0000-000000000001'
  );
