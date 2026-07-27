CREATE TABLE api_key_usage_writers_v1 (
    writer_epoch UUID PRIMARY KEY,
    lease_until_unix_secs BIGINT NOT NULL CHECK (lease_until_unix_secs >= 0),
    last_flush_id UUID
);

INSERT INTO api_key_usage_writers_v1 (writer_epoch, lease_until_unix_secs)
VALUES ('00000000-0000-0000-0000-000000000000', 9223372036854775807);

CREATE TABLE api_key_usage_buckets_v1 (
    writer_epoch UUID NOT NULL REFERENCES api_key_usage_writers_v1(writer_epoch) ON DELETE CASCADE,
    key_id TEXT NOT NULL,
    bucket_width_secs BIGINT NOT NULL CHECK (bucket_width_secs > 0),
    bucket_start_unix_secs BIGINT NOT NULL CHECK (bucket_start_unix_secs >= 0),
    requests BIGINT NOT NULL,
    input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    cost_usd_micros BIGINT NOT NULL,
    PRIMARY KEY (writer_epoch, key_id, bucket_width_secs, bucket_start_unix_secs)
);

CREATE INDEX api_key_usage_buckets_v1_key_time_idx
    ON api_key_usage_buckets_v1 (key_id, bucket_width_secs, bucket_start_unix_secs);
CREATE INDEX api_key_usage_buckets_v1_expiry_idx
    ON api_key_usage_buckets_v1 ((bucket_start_unix_secs + bucket_width_secs));
CREATE INDEX api_key_usage_writers_v1_lease_idx
    ON api_key_usage_writers_v1 (lease_until_unix_secs);
