DELETE FROM plugin_chains_v2
WHERE slot = 'router'
  AND wire_version IS DISTINCT FROM 3;
