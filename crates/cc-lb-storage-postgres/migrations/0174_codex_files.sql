SELECT pg_advisory_xact_lock(410174);

CREATE TABLE IF NOT EXISTS codex_files_v1 (
    file_id TEXT NOT NULL CHECK (btrim(file_id) <> ''),
    principal_id UUID NOT NULL REFERENCES principals_v1(id) ON DELETE RESTRICT,
    provider TEXT NOT NULL CHECK (btrim(provider) <> ''),
    kind TEXT NOT NULL CHECK (btrim(kind) <> ''),
    owner_binding_id TEXT NOT NULL REFERENCES account_owner_bindings_v1(binding_id) ON DELETE RESTRICT,
    owner_id TEXT NOT NULL CHECK (btrim(owner_id) <> ''),
    upstream_id UUID NOT NULL REFERENCES upstream_spec_v1(id),
    owner_generation BIGINT NOT NULL CHECK (owner_generation >= 1),
    binding_revision BIGINT NOT NULL CHECK (binding_revision >= 1),
    workspace_id TEXT NOT NULL CHECK (btrim(workspace_id) <> ''),
    state TEXT NOT NULL CHECK (state IN ('created', 'finalized')),
    created_at_unix_secs BIGINT NOT NULL CHECK (created_at_unix_secs >= 0),
    expires_at_unix_secs BIGINT CHECK (expires_at_unix_secs >= 0),
    reference TEXT,
    PRIMARY KEY (principal_id, provider, kind, owner_binding_id, owner_generation, file_id)
);
CREATE INDEX IF NOT EXISTS codex_files_v1_owner_idx
    ON codex_files_v1 (owner_binding_id, principal_id, provider, workspace_id);
