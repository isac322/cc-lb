ALTER TABLE oauth_usage_poll_cursors
    DROP COLUMN IF EXISTS last_window_start_unix_millis,
    DROP COLUMN IF EXISTS last_window_end_unix_millis,
    DROP COLUMN IF EXISTS last_throttle_at_unix_secs,
    DROP COLUMN IF EXISTS last_throttle_count,
    DROP COLUMN IF EXISTS recent_successes_unix_secs_json,
    DROP COLUMN IF EXISTS recent_throttles_unix_secs_json;
