-- API-key upstreams: org-scoped workspace header + capability snapshot + reported usage.
-- Existing rows get no details row (they read as "not checked"); nothing is rewritten.
ALTER TABLE upstream_spec_v1 ADD COLUMN workspace_id TEXT;

CREATE TABLE upstream_api_key_details_v1 (
    upstream_id TEXT PRIMARY KEY REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    verified INTEGER NOT NULL CHECK (verified IN (0, 1)),
    key_class TEXT NOT NULL CHECK (key_class IN ('admin','enterprise','oauth_token','workspace_bound','org_scoped','unknown')),
    key_hint TEXT NOT NULL,
    api_key_id TEXT,
    needs_workspace INTEGER NOT NULL CHECK (needs_workspace IN (0, 1)),
    capabilities_json TEXT NOT NULL,
    remediation_json TEXT NOT NULL DEFAULT '[]',
    usage_report_state TEXT CHECK (usage_report_state IS NULL OR usage_report_state IN ('ok','denied','error','not_applicable')),
    last_error TEXT,
    checked_at_unix_secs INTEGER CHECK (checked_at_unix_secs IS NULL OR checked_at_unix_secs >= 0),
    last_attempt_at_unix_secs INTEGER CHECK (last_attempt_at_unix_secs IS NULL OR last_attempt_at_unix_secs >= 0),
    -- Bumped by every capability/access write; the poller's demotion CAS token.
    access_revision INTEGER NOT NULL DEFAULT 0 CHECK (access_revision >= 0),
    usage_poll_failures INTEGER NOT NULL DEFAULT 0 CHECK (usage_poll_failures >= 0),
    usage_next_poll_after_unix_secs INTEGER CHECK (usage_next_poll_after_unix_secs IS NULL OR usage_next_poll_after_unix_secs >= 0),
    usage_last_success_at_unix_secs INTEGER CHECK (usage_last_success_at_unix_secs IS NULL OR usage_last_success_at_unix_secs >= 0),
    usage_last_attempt_at_unix_secs INTEGER CHECK (usage_last_attempt_at_unix_secs IS NULL OR usage_last_attempt_at_unix_secs >= 0),
    -- Single-poll claim: a poller leases the row before its outbound calls
    -- and completes only while it still owns the token. Expired leases are
    -- reclaimable, so a crashed poller's row is taken over later.
    usage_poll_claim_token TEXT,
    usage_poll_claim_lease_until_unix_secs INTEGER CHECK (usage_poll_claim_lease_until_unix_secs IS NULL OR usage_poll_claim_lease_until_unix_secs >= 0),
    created_at_unix_secs INTEGER NOT NULL CHECK (created_at_unix_secs >= 0),
    updated_at_unix_secs INTEGER NOT NULL CHECK (updated_at_unix_secs >= 0)
);

CREATE TABLE upstream_api_usage_v1 (
    upstream_id TEXT NOT NULL REFERENCES upstream_spec_v1(id) ON DELETE CASCADE,
    attribution TEXT NOT NULL CHECK (attribution IN ('api_key','workspace')),
    subject_id TEXT NOT NULL,
    bucket_width_secs INTEGER NOT NULL CHECK (bucket_width_secs IN (3600, 86400)),
    bucket_start_unix_secs INTEGER NOT NULL CHECK (bucket_start_unix_secs >= 0),
    model TEXT NOT NULL,              -- '' when the provider reports no model
    uncached_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK (uncached_input_tokens >= 0),
    cache_creation_5m_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK (cache_creation_5m_input_tokens >= 0),
    cache_creation_1h_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK (cache_creation_1h_input_tokens >= 0),
    cache_read_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK (cache_read_input_tokens >= 0),
    output_tokens INTEGER NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
    list_cost_micros INTEGER NOT NULL DEFAULT 0 CHECK (list_cost_micros >= 0),
    observed_at_unix_secs INTEGER NOT NULL CHECK (observed_at_unix_secs >= 0),
    PRIMARY KEY (upstream_id, attribution, subject_id, bucket_width_secs, bucket_start_unix_secs, model)
);
CREATE INDEX upstream_api_usage_v1_bucket_start_idx ON upstream_api_usage_v1 (bucket_start_unix_secs);
