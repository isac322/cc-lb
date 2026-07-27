CREATE TABLE api_key_usage_writers_v1 (
    writer_epoch TEXT PRIMARY KEY,
    lease_until_unix_secs INTEGER NOT NULL CHECK (lease_until_unix_secs >= 0),
    last_flush_id TEXT
);

INSERT INTO api_key_usage_writers_v1 (writer_epoch, lease_until_unix_secs)
VALUES ('00000000-0000-0000-0000-000000000000', 9223372036854775807);

CREATE TABLE api_key_usage_buckets_v1 (
    writer_epoch TEXT NOT NULL REFERENCES api_key_usage_writers_v1(writer_epoch) ON DELETE CASCADE,
    key_id TEXT NOT NULL,
    bucket_width_secs INTEGER NOT NULL CHECK (bucket_width_secs > 0),
    bucket_start_unix_secs INTEGER NOT NULL CHECK (bucket_start_unix_secs >= 0),
    requests INTEGER NOT NULL,
    input_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    cost_usd_micros INTEGER NOT NULL,
    PRIMARY KEY (writer_epoch, key_id, bucket_width_secs, bucket_start_unix_secs)
);

CREATE INDEX api_key_usage_buckets_v1_key_time_idx
    ON api_key_usage_buckets_v1 (key_id, bucket_width_secs, bucket_start_unix_secs);
CREATE INDEX api_key_usage_buckets_v1_expiry_idx
    ON api_key_usage_buckets_v1 (bucket_start_unix_secs + bucket_width_secs);
CREATE INDEX api_key_usage_writers_v1_lease_idx
    ON api_key_usage_writers_v1 (lease_until_unix_secs);
