ALTER TABLE plugin_chains_v2
    ADD COLUMN IF NOT EXISTS wire_version SMALLINT;

ALTER TABLE plugin_chains_v2
    DROP CONSTRAINT IF EXISTS plugin_chains_v2_wire_version_check;

ALTER TABLE plugin_chains_v2
    ADD CONSTRAINT plugin_chains_v2_wire_version_check
    CHECK (wire_version IS NULL OR wire_version BETWEEN 1 AND 255);
