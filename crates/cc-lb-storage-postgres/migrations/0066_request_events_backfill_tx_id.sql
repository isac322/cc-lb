DO $$
DECLARE
    affected_rows BIGINT;
BEGIN
    LOOP
        UPDATE request_events_v1
        SET tx_id = '0'::xid8
        WHERE seq IN (
            SELECT seq FROM request_events_v1
            WHERE tx_id IS NULL
            LIMIT 5000
            FOR UPDATE SKIP LOCKED
        );
        GET DIAGNOSTICS affected_rows = ROW_COUNT;
        EXIT WHEN affected_rows = 0;
    END LOOP;
END $$;
