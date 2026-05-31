ALTER TABLE plugin_chains_v2 DROP CONSTRAINT IF EXISTS plugin_chains_v2_slot_check;
ALTER TABLE plugin_chains_v2 ADD CONSTRAINT plugin_chains_v2_slot_check
    CHECK (slot IN ('router', 'observability_hook', 'shape'));
