-- Config history retains only revision and application time metadata. Rebuild
-- every JSON payload so removed raw TOML, including plaintext storage URLs,
-- cannot remain in upgraded databases.
UPDATE config_history_v1
SET payload = json_object(
    'revision', CAST(id AS INTEGER),
    'applied_at_unix_secs', COALESCE(
        CAST(json_extract(payload, '$.applied_at_unix_secs') AS INTEGER),
        created_at
    )
);
