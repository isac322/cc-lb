CREATE TABLE IF NOT EXISTS account_owner_bindings_v1 (
    binding_id TEXT PRIMARY KEY CHECK (length(trim(binding_id)) > 0),
    principal_id TEXT NOT NULL REFERENCES principals_v1(id) ON DELETE RESTRICT,
    provider TEXT NOT NULL CHECK (length(trim(provider)) > 0),
    owner_id TEXT NOT NULL CHECK (length(trim(owner_id)) > 0),
    upstream_id TEXT NOT NULL REFERENCES upstream_spec_v1(id) ON DELETE RESTRICT,
    owner_generation INTEGER NOT NULL CHECK (owner_generation > 0),
    local_user_id TEXT NOT NULL CHECK (length(trim(local_user_id)) > 0),
    allowed_workspaces_json TEXT NOT NULL CHECK (
        CASE WHEN json_valid(allowed_workspaces_json)
            THEN json_type(allowed_workspaces_json) = 'array'
                AND json_array_length(allowed_workspaces_json) > 0
            ELSE 0
        END
    ),
    provider_workspace_id TEXT NOT NULL CHECK (length(trim(provider_workspace_id)) > 0),
    outbound_region TEXT,
    revision INTEGER NOT NULL CHECK (revision > 0),
    revoked_at_unix_secs INTEGER CHECK (revoked_at_unix_secs IS NULL OR revoked_at_unix_secs >= 0)
);

CREATE INDEX IF NOT EXISTS account_owner_bindings_v1_principal_id
    ON account_owner_bindings_v1 (principal_id);
CREATE INDEX IF NOT EXISTS account_owner_bindings_v1_upstream_id
    ON account_owner_bindings_v1 (upstream_id);

CREATE TABLE IF NOT EXISTS account_owner_workspaces_v1 (
    principal_id TEXT NOT NULL REFERENCES principals_v1(id) ON DELETE RESTRICT,
    provider TEXT NOT NULL CHECK (length(trim(provider)) > 0),
    workspace_id TEXT NOT NULL CHECK (length(trim(workspace_id)) > 0),
    binding_id TEXT NOT NULL REFERENCES account_owner_bindings_v1(binding_id) ON DELETE RESTRICT,
    PRIMARY KEY (principal_id, provider, workspace_id)
);

CREATE INDEX IF NOT EXISTS account_owner_workspaces_v1_binding_id
    ON account_owner_workspaces_v1 (binding_id);
