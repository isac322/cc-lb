DELETE FROM plugin_chains_v2
WHERE slot = 'router'
  AND COALESCE(CAST(json_extract(config, '$.wire_version') AS INTEGER), -1) <> 3;
