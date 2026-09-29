DROP TABLE IF EXISTS anthropic_compat_etags_new;

CREATE TABLE anthropic_compat_etags_new (
    key TEXT PRIMARY KEY,
    last_applied_at_unix_secs INTEGER NOT NULL CHECK (last_applied_at_unix_secs >= 0),
    last_value_hash TEXT NOT NULL
);

INSERT OR REPLACE INTO anthropic_compat_etags_new (
    key,
    last_applied_at_unix_secs,
    last_value_hash
)
SELECT
    key,
    last_applied_at_unix_secs,
    last_value_hash
FROM anthropic_compat_etags;

DROP TABLE anthropic_compat_etags;
ALTER TABLE anthropic_compat_etags_new RENAME TO anthropic_compat_etags;
