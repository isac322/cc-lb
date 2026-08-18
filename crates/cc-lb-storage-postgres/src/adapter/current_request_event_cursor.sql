SELECT seq FROM request_events_v1
WHERE tx_id < pg_snapshot_xmin(pg_current_snapshot())
ORDER BY seq DESC
LIMIT 1
