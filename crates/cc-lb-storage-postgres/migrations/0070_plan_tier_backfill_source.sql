DO $$
DECLARE
    resolution_source_constraint text;
BEGIN
    SELECT conname
      INTO resolution_source_constraint
      FROM pg_constraint
     WHERE conrelid = 'upstream_plan_tier_history_v1'::regclass
       AND contype = 'c'
       AND pg_get_constraintdef(oid) LIKE '%resolution_source%'
       AND pg_get_constraintdef(oid) LIKE '%override%'
       AND pg_get_constraintdef(oid) LIKE '%builtin%'
       AND pg_get_constraintdef(oid) LIKE '%unknown%'
     LIMIT 1;

    IF resolution_source_constraint IS NOT NULL THEN
        EXECUTE format(
            'ALTER TABLE upstream_plan_tier_history_v1 DROP CONSTRAINT %I',
            resolution_source_constraint
        );
    END IF;
END $$;

ALTER TABLE upstream_plan_tier_history_v1
    ADD CONSTRAINT upstream_plan_tier_history_v1_resolution_source_check
    CHECK (resolution_source IN ('override','builtin','backfill','unknown'));
