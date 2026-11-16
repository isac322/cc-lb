SELECT pg_advisory_xact_lock(410171);

CREATE TABLE IF NOT EXISTS account_owner_bindings_v1 (
    binding_id TEXT PRIMARY KEY CHECK (btrim(binding_id) <> ''),
    principal_id UUID NOT NULL REFERENCES principals_v1(id) ON DELETE RESTRICT,
    provider TEXT NOT NULL CHECK (btrim(provider) <> ''),
    owner_id TEXT NOT NULL CHECK (btrim(owner_id) <> ''),
    upstream_id UUID NOT NULL REFERENCES upstream_spec_v1(id),
    owner_generation BIGINT NOT NULL CHECK (owner_generation >= 1),
    local_user_id TEXT NOT NULL CHECK (btrim(local_user_id) <> ''),
    allowed_workspaces_json TEXT NOT NULL
        CHECK (jsonb_typeof(allowed_workspaces_json::jsonb) = 'array'),
    provider_workspace_id TEXT NOT NULL CHECK (btrim(provider_workspace_id) <> ''),
    outbound_region TEXT,
    revision BIGINT NOT NULL CHECK (revision >= 1),
    revoked_at_unix_secs BIGINT CHECK (revoked_at_unix_secs >= 0)
);

CREATE TABLE IF NOT EXISTS account_owner_workspaces_v1 (
    principal_id UUID NOT NULL REFERENCES principals_v1(id) ON DELETE RESTRICT,
    provider TEXT NOT NULL CHECK (btrim(provider) <> ''),
    workspace_id TEXT NOT NULL CHECK (btrim(workspace_id) <> ''),
    binding_id TEXT NOT NULL REFERENCES account_owner_bindings_v1(binding_id),
    PRIMARY KEY (principal_id, provider, workspace_id)
);

CREATE INDEX IF NOT EXISTS account_owner_bindings_v1_principal_idx
    ON account_owner_bindings_v1 (principal_id);
CREATE INDEX IF NOT EXISTS account_owner_bindings_v1_upstream_idx
    ON account_owner_bindings_v1 (upstream_id);
