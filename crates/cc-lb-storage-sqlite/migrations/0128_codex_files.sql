CREATE TABLE IF NOT EXISTS codex_files_v1 (
    file_id TEXT PRIMARY KEY CHECK (length(trim(file_id)) > 0),
    principal_id TEXT NOT NULL REFERENCES principals_v1(id) ON DELETE RESTRICT,
    provider TEXT NOT NULL CHECK (length(trim(provider)) > 0),
    kind TEXT NOT NULL CHECK (length(trim(kind)) > 0),
    owner_binding_id TEXT NOT NULL REFERENCES account_owner_bindings_v1(binding_id) ON DELETE RESTRICT,
    owner_id TEXT NOT NULL CHECK (length(trim(owner_id)) > 0),
    upstream_id TEXT NOT NULL REFERENCES upstream_spec_v1(id) ON DELETE RESTRICT,
    owner_generation INTEGER NOT NULL CHECK (owner_generation > 0),
    binding_revision INTEGER NOT NULL CHECK (binding_revision > 0),
    workspace_id TEXT NOT NULL CHECK (length(trim(workspace_id)) > 0),
    state TEXT NOT NULL CHECK (state IN ('created', 'finalized')),
    created_at_unix_secs INTEGER NOT NULL CHECK (created_at_unix_secs >= 0),
    expires_at_unix_secs INTEGER CHECK (expires_at_unix_secs IS NULL OR expires_at_unix_secs >= 0),
    reference TEXT
);
CREATE INDEX IF NOT EXISTS codex_files_v1_owner_idx
    ON codex_files_v1 (owner_binding_id, principal_id, provider, workspace_id);
