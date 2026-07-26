-- Smart Routing (the built-in `subscription-preference` router filter) becomes a
-- per-principal opt-out flag instead of an explicit plugin-chain entry.
--
-- Preserve current behaviour: only principals that already had the built-in
-- chain entry keep it enabled. Everyone else stays off, matching what they see
-- today. New principals get the DB default (1).
ALTER TABLE principals_v1 ADD COLUMN smart_routing_enabled INTEGER NOT NULL DEFAULT 1;

UPDATE principals_v1
   SET smart_routing_enabled = 0
 WHERE NOT EXISTS (
       SELECT 1
         FROM plugin_chains_v2
        WHERE plugin_chains_v2.principal_id = principals_v1.id
          AND plugin_chains_v2.slot = 'router'
          AND plugin_chains_v2.wasm_registry_id = '00000000-0000-0000-0000-000000000002'
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
-- `wasm_registry_id` is declared BLOB but the adapter binds `Uuid::to_string()`,
-- so stored values are hyphenated lowercase UUID text. Precedent:
-- 0062_remove_builtin_router_filter.sql.
--
-- The registry row and its blob stay: an explicit chain entry remains a valid
-- advanced override. Registry refcount is computed at query time from
-- plugin_chains_v2, and blob lifetime keys off wasm_registry_v2, so neither
-- needs a manual fixup here.
DELETE FROM plugin_chains_v2
 WHERE slot = 'router'
   AND wasm_registry_id = '00000000-0000-0000-0000-000000000002'
   AND NOT EXISTS (
       SELECT 1
         FROM plugin_chains_v2 AS sibling
        WHERE sibling.principal_id = plugin_chains_v2.principal_id
          AND sibling.slot = 'router'
          AND sibling.id <> plugin_chains_v2.id
          AND sibling.order_value <= plugin_chains_v2.order_value
   );
