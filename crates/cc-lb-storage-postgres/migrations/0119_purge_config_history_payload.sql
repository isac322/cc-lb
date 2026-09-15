-- Config history is capped at 50 rows and now retains only revision and
-- application time metadata. Rebuild each JSON value so removed raw TOML,
-- including plaintext storage URLs, cannot remain in upgraded databases.
UPDATE config_history_v1
SET config = jsonb_build_object(
    'revision', revision,
    'applied_at_unix_secs', COALESCE(
        (config ->> 'applied_at_unix_secs')::BIGINT,
        EXTRACT(EPOCH FROM created_at)::BIGINT
    )
);
