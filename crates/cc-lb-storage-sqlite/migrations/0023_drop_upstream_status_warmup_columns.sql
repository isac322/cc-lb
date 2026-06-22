PRAGMA foreign_keys = OFF;

CREATE TABLE upstream_status_v1_new (
    upstream_id TEXT PRIMARY KEY REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    last_apply_error TEXT,
    last_apply_at INTEGER,
    observed_spec_revision INTEGER,
    observed_api_key_secret_revision INTEGER,
    observed_oauth_token_revision INTEGER,
    last_warmup_at INTEGER,
    updated_at INTEGER NOT NULL
);

INSERT INTO upstream_status_v1_new (
    upstream_id,
    last_apply_error,
    last_apply_at,
    observed_spec_revision,
    observed_api_key_secret_revision,
    observed_oauth_token_revision,
    last_warmup_at,
    updated_at
)
SELECT
    upstream_id,
    last_apply_error,
    last_apply_at,
    observed_spec_revision,
    observed_api_key_secret_revision,
    observed_oauth_token_revision,
    last_warmup_at,
    updated_at
FROM upstream_status_v1;

DROP TABLE upstream_status_v1;
ALTER TABLE upstream_status_v1_new RENAME TO upstream_status_v1;

PRAGMA foreign_keys = ON;
