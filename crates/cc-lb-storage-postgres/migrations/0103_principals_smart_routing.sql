-- Smart Routing (the built-in `subscription-preference` router filter) becomes a
-- per-principal opt-out flag instead of an explicit plugin-chain entry.
--
-- Preserve current behaviour: only principals that already had the built-in
-- chain entry keep it enabled. New principals get the DB default (TRUE).
--
-- ADD COLUMN with a constant DEFAULT is metadata-only on PG 11+, so this does
-- not rewrite the table or hold a long ACCESS EXCLUSIVE lock.
ALTER TABLE principals_v1
    ADD COLUMN IF NOT EXISTS smart_routing_enabled BOOLEAN NOT NULL DEFAULT TRUE;

UPDATE principals_v1
   SET smart_routing_enabled = FALSE
 WHERE NOT EXISTS (
       SELECT 1
         FROM plugin_chains_v2
        WHERE plugin_chains_v2.principal_id = principals_v1.id
          AND plugin_chains_v2.slot = 'router'
          AND plugin_chains_v2.wasm_registry_id = '00000000-0000-0000-0000-000000000002'::uuid
   );

-- Drop the entry only where it is redundant: the flag always inserts the filter
-- at the head of the chain, so a leading entry is exactly what the flag now
-- reproduces. A mid-chain entry encodes a deliberate position among other
-- router plugins and MUST survive -- `apply_smart_routing_default` sees it and
-- skips the implicit insert, keeping the operator's ordering. Deleting it would
-- silently move the filter to the front.
--
-- "Leading" means no sibling router entry sorts at or before it, so an order
-- tie is treated as non-leading and left alone rather than guessed at.
--
-- The registry row and its blob stay: an explicit chain entry remains a valid
-- advanced override. Registry refcount is computed at query time from
-- plugin_chains_v2, and blob lifetime keys off wasm_registry_v2, so neither
-- needs a manual fixup here.
DELETE FROM plugin_chains_v2 AS builtin
 WHERE builtin.slot = 'router'
   AND builtin.wasm_registry_id = '00000000-0000-0000-0000-000000000002'::uuid
   AND NOT EXISTS (
       SELECT 1
         FROM plugin_chains_v2 AS sibling
        WHERE sibling.principal_id = builtin.principal_id
          AND sibling.slot = 'router'
          AND sibling.id <> builtin.id
          AND sibling.order_value <= builtin.order_value
   );
