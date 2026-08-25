CREATE TABLE oauth_refresh_leases_v1 (
    upstream_id TEXT PRIMARY KEY REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    holder TEXT NOT NULL,
    expected_generation INTEGER NOT NULL CHECK (expected_generation >= 0),
    lease_until_unix_secs INTEGER NOT NULL
);

CREATE TABLE oauth_refresh_terminal_failures_v1 (
    upstream_id TEXT PRIMARY KEY REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    expected_generation INTEGER NOT NULL CHECK (expected_generation >= 0),
    error_code TEXT NOT NULL,
    failed_at_unix_secs INTEGER NOT NULL
);
