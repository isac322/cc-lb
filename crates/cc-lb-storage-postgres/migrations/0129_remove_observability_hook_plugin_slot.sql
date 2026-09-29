DELETE FROM plugin_chains_v2
WHERE slot = 'observability_hook';

ALTER TABLE plugin_chains_v2 DROP CONSTRAINT IF EXISTS plugin_chains_v2_slot_check;
ALTER TABLE plugin_chains_v2 ADD CONSTRAINT plugin_chains_v2_slot_check
    CHECK (slot IN ('router', 'shape'));

ALTER TABLE plugin_chains_v2
    DROP COLUMN IF EXISTS sse_per_event,
    DROP COLUMN IF EXISTS batched_events_per_flush,
    DROP COLUMN IF EXISTS batched_flush_ms;

UPDATE wasm_registry_v2
   SET supported_slots = array_remove(supported_slots, 'observability_hook')
 WHERE 'observability_hook' = ANY(supported_slots);

UPDATE wasm_registry_v2
   SET hook_metadata = (hook_metadata::jsonb - 'observe')::text
 WHERE hook_metadata::jsonb ? 'observe';
