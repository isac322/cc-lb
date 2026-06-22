DROP TABLE IF EXISTS oauth_usage_poll_cursors_new;

CREATE TABLE oauth_usage_poll_cursors_new (
    upstream_id BLOB PRIMARY KEY,
    last_observed_at_unix_secs INTEGER CHECK (last_observed_at_unix_secs IS NULL OR last_observed_at_unix_secs >= 0),
    last_status INTEGER,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0)
);

INSERT OR REPLACE INTO oauth_usage_poll_cursors_new (
    upstream_id,
    last_observed_at_unix_secs,
    last_status,
    attempt_count
)
SELECT
    upstream_id,
    last_observed_at_unix_secs,
    last_status,
    attempt_count
FROM oauth_usage_poll_cursors;

DROP TABLE oauth_usage_poll_cursors;
ALTER TABLE oauth_usage_poll_cursors_new RENAME TO oauth_usage_poll_cursors;
