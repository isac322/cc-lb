SET LOCAL lock_timeout = '1s';

ALTER TABLE upstream_spec_v1
    ADD COLUMN IF NOT EXISTS vertex_project_id TEXT,
    ADD COLUMN IF NOT EXISTS vertex_location TEXT,
    ADD COLUMN IF NOT EXISTS vertex_api_version TEXT,
    ADD COLUMN IF NOT EXISTS vertex_express BOOLEAN,
    ADD COLUMN IF NOT EXISTS vertex_credential_type TEXT,
    ADD COLUMN IF NOT EXISTS vertex_quota_project_id TEXT;

ALTER TABLE upstream_spec_v1 DROP CONSTRAINT IF EXISTS upstream_spec_v1_kind_check;
ALTER TABLE upstream_spec_v1 ADD CONSTRAINT upstream_spec_v1_kind_check CHECK (kind IN ('anthropic_api_key','anthropic_oauth','openai_api_key','openai_oauth','gemini_api_key','gemini_oauth','vertex_api_key','vertex_credential'));
ALTER TABLE upstream_spec_v1 ADD CONSTRAINT upstream_spec_v1_vertex_config_check CHECK (
    (kind NOT LIKE 'vertex_%' OR (vertex_api_version IS NOT NULL AND vertex_api_version IN ('v1','v1beta1') AND vertex_express IS NOT NULL))
    AND (kind NOT LIKE 'vertex_%' OR (kind = 'vertex_api_key' AND ((vertex_express = TRUE AND vertex_project_id IS NULL AND vertex_location IS NULL) OR (vertex_express = FALSE AND vertex_project_id IS NOT NULL AND vertex_location IS NOT NULL)) OR kind = 'vertex_credential' AND vertex_express = FALSE AND vertex_project_id IS NOT NULL AND vertex_location IS NOT NULL))
    AND ((kind = 'vertex_credential' AND vertex_credential_type IS NOT NULL AND vertex_credential_type IN ('service_account','authorized_user','impersonated_service_account')) OR (kind <> 'vertex_credential' AND vertex_credential_type IS NULL))
    AND (kind NOT LIKE 'vertex_%' OR (vertex_project_id IS NULL OR ((vertex_project_id ~ '^[a-z][a-z0-9-]{4,28}[a-z0-9]$') OR vertex_project_id ~ '^[0-9]{6,20}$')))
    AND (kind NOT LIKE 'vertex_%' OR (vertex_location IS NULL OR vertex_location ~ '^[a-z]([a-z0-9-]{0,30}[a-z0-9])?$'))
    AND (kind LIKE 'vertex_%' OR (vertex_project_id IS NULL AND vertex_location IS NULL AND vertex_api_version IS NULL AND vertex_express IS NULL AND vertex_credential_type IS NULL AND vertex_quota_project_id IS NULL))
);
