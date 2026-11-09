ALTER TABLE upstream_spec_v1
    ADD COLUMN owner_epoch INTEGER NOT NULL DEFAULT 0 CHECK (owner_epoch >= 0);

ALTER TABLE upstream_affinity_v1
    ADD COLUMN owner_epoch INTEGER NOT NULL DEFAULT 0 CHECK (owner_epoch >= 0);
