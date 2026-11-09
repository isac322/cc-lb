SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '90s';

ALTER TABLE upstream_spec_v1
    ADD COLUMN owner_epoch BIGINT NOT NULL DEFAULT 0
    CHECK (owner_epoch >= 0);

ALTER TABLE upstream_affinity_v1
    ADD COLUMN owner_epoch BIGINT NOT NULL DEFAULT 0
    CHECK (owner_epoch >= 0);
