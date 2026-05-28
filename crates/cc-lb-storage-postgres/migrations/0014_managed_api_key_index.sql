CREATE TABLE IF NOT EXISTS managed_api_key_index_v1 (
    index_hash   BYTEA NOT NULL,
    principal_id TEXT  NOT NULL,
    key_id       TEXT  NOT NULL,
    PRIMARY KEY (index_hash),
    FOREIGN KEY (principal_id, key_id)
        REFERENCES managed_api_keys_v1 (principal_id, key_id)
        ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS managed_api_key_index_v1_composite_idx ON managed_api_key_index_v1 (principal_id, key_id);
