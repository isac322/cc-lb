-- Provider-neutral API-key project provenance and reporting state.
-- Existing Anthropic rows remain valid: every new column is nullable.
SET LOCAL lock_timeout = '1s';

ALTER TABLE upstream_api_key_details_v1
    ADD COLUMN IF NOT EXISTS provider TEXT;
ALTER TABLE upstream_api_key_details_v1
    ADD COLUMN IF NOT EXISTS project_id TEXT;
ALTER TABLE upstream_api_key_details_v1
    ADD COLUMN IF NOT EXISTS project_source TEXT CHECK (
        project_source IS NULL OR project_source IN ('configured', 'admin_resolved', 'unverified')
    );
ALTER TABLE upstream_api_key_details_v1
    ADD COLUMN IF NOT EXISTS provider_report_state TEXT CHECK (
        provider_report_state IS NULL OR provider_report_state IN (
            'reported',
            'estimated',
            'not_configured',
            'unavailable',
            'denied',
            'not_applicable',
            'stale',
            'failed'
        )
    );
-- Redacted, bounded metadata only; never plaintext credentials or auth headers.
ALTER TABLE upstream_api_key_details_v1
    ADD COLUMN IF NOT EXISTS provider_report_status_metadata_json TEXT;

-- One encrypted OpenAI organization/Admin credential per deployment. The
-- singleton scope prevents accidentally attaching the Admin credential to a
-- serving upstream or storing more than one active deployment credential.
CREATE TABLE IF NOT EXISTS openai_org_reporting_credential_v1 (
    scope TEXT PRIMARY KEY CHECK (scope = 'deployment'),
    encrypted_secret_ciphertext BYTEA NOT NULL,
    redacted_hint TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('configured', 'denied', 'unavailable', 'failed')),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision >= 1),
    created_at_unix_secs BIGINT NOT NULL CHECK (created_at_unix_secs >= 0),
    updated_at_unix_secs BIGINT NOT NULL CHECK (updated_at_unix_secs >= 0),
    poll_claim_lease_until_unix_secs BIGINT
);
