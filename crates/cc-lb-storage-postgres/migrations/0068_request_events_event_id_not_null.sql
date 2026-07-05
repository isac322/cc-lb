UPDATE request_events_v1
SET event_id = 'legacy-' || seq
WHERE event_id IS NULL;

DROP INDEX IF EXISTS request_events_v1_event_id_idx;

ALTER TABLE request_events_v1 ALTER COLUMN event_id SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'request_events_v1_event_id_unique'
          AND conrelid = 'request_events_v1'::regclass
    ) THEN
        ALTER TABLE request_events_v1
            ADD CONSTRAINT request_events_v1_event_id_unique UNIQUE (event_id);
    END IF;
END $$;
