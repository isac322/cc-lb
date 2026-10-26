-- SQLite requires a table rebuild to widen the kind CHECK constraint. The
-- affinity table is an FK child only, so foreign keys can stay enabled.
DROP TABLE IF EXISTS upstream_affinity_v1_new;

CREATE TABLE upstream_affinity_v1_new (
    principal_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN (
        'anthropic_web_search_encrypted_content',
        'anthropic_file_id',
        'anthropic_container_id',
        'openai_response_id',
        'openai_conversation_id',
        'gemini_cached_content'
    )),
    value_sha256 BLOB NOT NULL CHECK (length(value_sha256) = 32),
    upstream_id TEXT NOT NULL REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    observed_at_unix_secs INTEGER NOT NULL CHECK (observed_at_unix_secs >= 0),
    expires_at_unix_secs INTEGER CHECK (expires_at_unix_secs IS NULL OR expires_at_unix_secs >= 0),
    PRIMARY KEY (principal_id, provider, kind, value_sha256)
);

INSERT INTO upstream_affinity_v1_new (
    principal_id, provider, kind, value_sha256, upstream_id,
    observed_at_unix_secs, expires_at_unix_secs
)
SELECT
    principal_id, provider, kind, value_sha256, upstream_id,
    observed_at_unix_secs, expires_at_unix_secs
FROM upstream_affinity_v1;

DROP TABLE upstream_affinity_v1;
ALTER TABLE upstream_affinity_v1_new RENAME TO upstream_affinity_v1;

CREATE INDEX upstream_affinity_v1_upstream_id_idx
    ON upstream_affinity_v1 (upstream_id);

CREATE INDEX upstream_affinity_v1_expires_at_idx
    ON upstream_affinity_v1 (expires_at_unix_secs)
    WHERE expires_at_unix_secs IS NOT NULL;

CREATE INDEX upstream_affinity_v1_observed_at_idx
    ON upstream_affinity_v1 (observed_at_unix_secs);
