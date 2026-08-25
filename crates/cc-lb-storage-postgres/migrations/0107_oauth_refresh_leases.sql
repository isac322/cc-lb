CREATE TABLE oauth_refresh_leases_v1 (
    upstream_id UUID PRIMARY KEY REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    holder UUID NOT NULL,
    expected_generation BIGINT NOT NULL CHECK (expected_generation >= 0),
    lease_until_unix_secs BIGINT NOT NULL
);

CREATE TABLE oauth_refresh_terminal_failures_v1 (
    upstream_id UUID PRIMARY KEY REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    expected_generation BIGINT NOT NULL CHECK (expected_generation >= 0),
    error_code TEXT NOT NULL,
    failed_at_unix_secs BIGINT NOT NULL
);
