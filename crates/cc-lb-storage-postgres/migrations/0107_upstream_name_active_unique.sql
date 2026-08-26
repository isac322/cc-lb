ALTER TABLE upstream_spec_v1
    DROP CONSTRAINT IF EXISTS upstream_spec_v1_name_key;

CREATE UNIQUE INDEX IF NOT EXISTS upstream_spec_v1_name_active_uniq
    ON upstream_spec_v1 (name)
    WHERE deleted_at IS NULL;

-- Fail loudly if any unconditional unique index on (name) survived under a
-- non-default constraint name; leaving one in place keeps soft-deleted names
-- reserved and silently preserves the bug this migration removes.
DO $$
DECLARE
    leftover TEXT;
BEGIN
    SELECT i.relname INTO leftover
    FROM pg_index x
    JOIN pg_class i ON i.oid = x.indexrelid
    JOIN pg_class t ON t.oid = x.indrelid
    WHERE t.relname = 'upstream_spec_v1'
      AND x.indisunique
      AND x.indpred IS NULL
      AND x.indnatts = 1
      AND x.indkey[0] = (
          SELECT attnum
          FROM pg_attribute
          WHERE attrelid = t.oid
            AND attname = 'name'
            AND NOT attisdropped
      )
    LIMIT 1;

    IF leftover IS NOT NULL THEN
        RAISE EXCEPTION
            'unconditional unique index % on upstream_spec_v1(name) still present',
            leftover;
    END IF;
END $$;
