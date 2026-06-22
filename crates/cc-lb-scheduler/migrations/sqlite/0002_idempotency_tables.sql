CREATE TABLE IF NOT EXISTS oauth_usage_poll_cursors (
    upstream_id BLOB PRIMARY KEY,
    last_window_start_unix_millis INTEGER CHECK (last_window_start_unix_millis IS NULL OR last_window_start_unix_millis >= 0),
    last_window_end_unix_millis INTEGER CHECK (last_window_end_unix_millis IS NULL OR last_window_end_unix_millis >= 0),
    last_throttle_at_unix_secs INTEGER CHECK (last_throttle_at_unix_secs IS NULL OR last_throttle_at_unix_secs >= 0),
    last_throttle_count INTEGER NOT NULL DEFAULT 0 CHECK (last_throttle_count >= 0),
    last_status INTEGER,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_observed_at_unix_secs INTEGER CHECK (last_observed_at_unix_secs IS NULL OR last_observed_at_unix_secs >= 0),
    recent_successes_unix_secs_json TEXT NOT NULL DEFAULT '[]',
    recent_throttles_unix_secs_json TEXT NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS anthropic_compat_etags (
    key TEXT PRIMARY KEY,
    etag TEXT,
    last_applied_at_unix_secs INTEGER NOT NULL CHECK (last_applied_at_unix_secs >= 0),
    last_value_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS price_catalog_versions (
    source TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    fetched_at_unix_secs INTEGER NOT NULL CHECK (fetched_at_unix_secs >= 0)
);
