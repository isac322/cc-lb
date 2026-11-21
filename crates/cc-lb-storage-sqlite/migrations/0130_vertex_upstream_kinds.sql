-- no-transaction
PRAGMA foreign_keys = OFF;
BEGIN IMMEDIATE;

CREATE TEMP TABLE upstream_rebuild_before AS
SELECT (SELECT COUNT(*) FROM upstream_spec_v1) AS spec_rows,
       (SELECT COUNT(*) FROM upstream_api_key_secret_v1) AS api_key_rows,
       (SELECT COUNT(*) FROM upstream_oauth_token_v1) AS oauth_token_rows,
       (SELECT COUNT(*) FROM upstream_status_v1) AS status_rows,
       (SELECT COUNT(*) FROM warmup_attempts_v1) AS warmup_attempt_rows;

CREATE TABLE upstream_spec_v1_new (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('anthropic_api_key','anthropic_oauth','openai_api_key','openai_oauth','gemini_api_key','gemini_oauth','vertex_api_key','vertex_credential')),
    base_url TEXT,
    enabled INTEGER NOT NULL DEFAULT 1,
    warmup_enabled INTEGER NOT NULL DEFAULT 0,
    warmup_dialect_plugin TEXT,
    spec_revision INTEGER NOT NULL DEFAULT 1 CHECK (spec_revision >= 0),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    deleted_at INTEGER,
    workspace_id TEXT,
    owner_epoch INTEGER NOT NULL DEFAULT 0 CHECK (owner_epoch >= 0),
    vertex_project_id TEXT,
    vertex_location TEXT,
    vertex_api_version TEXT,
    vertex_express INTEGER,
    vertex_credential_type TEXT,
    vertex_quota_project_id TEXT,
    CHECK (kind NOT GLOB 'vertex_*' OR (vertex_api_version IS NOT NULL AND vertex_api_version IN ('v1','v1beta1') AND vertex_express IS NOT NULL)),
    CHECK (kind GLOB 'vertex_*' OR (vertex_project_id IS NULL AND vertex_location IS NULL AND vertex_api_version IS NULL AND vertex_express IS NULL AND vertex_credential_type IS NULL AND vertex_quota_project_id IS NULL)),
    CHECK (
        kind NOT GLOB 'vertex_*'
        OR (kind = 'vertex_api_key' AND ((vertex_express = 1 AND vertex_project_id IS NULL AND vertex_location IS NULL) OR (vertex_express = 0 AND vertex_project_id IS NOT NULL AND vertex_location IS NOT NULL)))
        OR (kind = 'vertex_credential' AND vertex_express = 0 AND vertex_project_id IS NOT NULL AND vertex_location IS NOT NULL)
    ),
    CHECK ((kind = 'vertex_credential' AND vertex_credential_type IS NOT NULL AND vertex_credential_type IN ('service_account','authorized_user','impersonated_service_account')) OR (kind <> 'vertex_credential' AND vertex_credential_type IS NULL)),
    CHECK (vertex_project_id IS NULL OR ((length(vertex_project_id) BETWEEN 6 AND 30 AND vertex_project_id NOT GLOB '*[^a-z0-9-]*' AND substr(vertex_project_id,1,1) GLOB '[a-z]' AND substr(vertex_project_id,-1,1) GLOB '[a-z0-9]') OR (length(vertex_project_id) BETWEEN 6 AND 20 AND vertex_project_id NOT GLOB '*[^0-9]*'))),
    CHECK (vertex_location IS NULL OR (length(vertex_location) BETWEEN 1 AND 32 AND vertex_location NOT GLOB '*[^a-z0-9-]*' AND substr(vertex_location,1,1) GLOB '[a-z]' AND substr(vertex_location,-1,1) GLOB '[a-z0-9]'))
);

INSERT INTO upstream_spec_v1_new (id,name,kind,base_url,enabled,warmup_enabled,warmup_dialect_plugin,spec_revision,created_at,updated_at,deleted_at,workspace_id,owner_epoch)
SELECT id,name,kind,base_url,enabled,warmup_enabled,warmup_dialect_plugin,spec_revision,created_at,updated_at,deleted_at,workspace_id,owner_epoch FROM upstream_spec_v1;
DROP TABLE upstream_spec_v1;
ALTER TABLE upstream_spec_v1_new RENAME TO upstream_spec_v1;
CREATE UNIQUE INDEX upstream_spec_v1_name_active_uniq ON upstream_spec_v1 (name) WHERE deleted_at IS NULL;
CREATE INDEX upstream_spec_v1_warmup_wasm_registry_id_idx ON upstream_spec_v1 (json_extract(warmup_dialect_plugin, '$.wasm_registry_id')) WHERE deleted_at IS NULL AND warmup_dialect_plugin IS NOT NULL;

CREATE TEMP TABLE upstream_rebuild_guard (ok INTEGER NOT NULL CHECK (ok = 1));
INSERT INTO upstream_rebuild_guard SELECT CASE WHEN (SELECT COUNT(*) FROM upstream_spec_v1)=spec_rows AND (SELECT COUNT(*) FROM upstream_api_key_secret_v1)=api_key_rows AND (SELECT COUNT(*) FROM upstream_oauth_token_v1)=oauth_token_rows AND (SELECT COUNT(*) FROM upstream_status_v1)=status_rows AND (SELECT COUNT(*) FROM warmup_attempts_v1)=warmup_attempt_rows THEN 1 ELSE 0 END FROM upstream_rebuild_before;
INSERT INTO upstream_rebuild_guard SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM upstream_api_key_secret_v1 c LEFT JOIN upstream_spec_v1 s ON s.id=c.upstream_id WHERE s.id IS NULL) AND NOT EXISTS (SELECT 1 FROM upstream_oauth_token_v1 c LEFT JOIN upstream_spec_v1 s ON s.id=c.upstream_id WHERE s.id IS NULL) AND NOT EXISTS (SELECT 1 FROM upstream_status_v1 c LEFT JOIN upstream_spec_v1 s ON s.id=c.upstream_id WHERE s.id IS NULL) AND NOT EXISTS (SELECT 1 FROM warmup_attempts_v1 c LEFT JOIN upstream_spec_v1 s ON s.id=c.upstream_id WHERE s.id IS NULL) THEN 1 ELSE 0 END;
DROP TABLE temp.upstream_rebuild_guard;
DROP TABLE temp.upstream_rebuild_before;
COMMIT;
PRAGMA foreign_keys = ON;
