CREATE TABLE IF NOT EXISTS wasm_blobs_v2 (
    sha256 BLOB PRIMARY KEY,
    bytes BLOB NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS wasm_registry_v2 (
    sha256 BLOB PRIMARY KEY REFERENCES wasm_blobs_v2(sha256) ON DELETE RESTRICT,
    plugin_name TEXT NOT NULL,
    plugin_version TEXT NOT NULL,
    abi_envelope INTEGER NOT NULL,
    augmented_metadata TEXT NOT NULL,
    host_offer_hash BLOB NOT NULL,
    handshake_schema_version INTEGER NOT NULL,
    last_handshake_at INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS plugin_chains_v2 (
    id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL,
    slot TEXT NOT NULL,
    wasm_sha256 BLOB NOT NULL REFERENCES wasm_registry_v2(sha256) ON DELETE RESTRICT,
    order_index INTEGER NOT NULL,
    config TEXT,
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision >= 0),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS plugin_registry_marker_v1 (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS wasm_registry_v2_status_idx
    ON wasm_registry_v2 (status, plugin_name, sha256);

CREATE INDEX IF NOT EXISTS plugin_chains_v2_principal_slot_order_idx
    ON plugin_chains_v2 (principal_id, slot, order_index, id);

CREATE INDEX IF NOT EXISTS plugin_chains_v2_wasm_sha256_idx
    ON plugin_chains_v2 (wasm_sha256);

-- Seed builtin cache affinity blob and registry entry
INSERT INTO wasm_blobs_v2 (sha256, bytes, created_at)
VALUES (zeroblob(32), zeroblob(0), 0)
ON CONFLICT(sha256) DO NOTHING;

INSERT INTO wasm_registry_v2 (
    sha256,
    plugin_name,
    plugin_version,
    abi_envelope,
    augmented_metadata,
    host_offer_hash,
    handshake_schema_version,
    last_handshake_at,
    status,
    created_at,
    updated_at
)
VALUES (
    zeroblob(32),
    'cache-affinity',
    'builtin://cache-affinity',
    3,
    '{"identity":{"magic":[204,27,112,16,0,1,0,0],"abi_envelope":1,"plugin_name":"cache-affinity","plugin_version":"builtin"},"negotiated_functions":{"cc_lb_route":3},"negotiated_capabilities":[],"handshake_completed_at":1,"self_check_passed":true,"self_check_completed_at":1,"expires_at":9223372036854775807}',
    zeroblob(32),
    1,
    0,
    'active',
    0,
    0
)
ON CONFLICT(sha256) DO NOTHING;

INSERT INTO plugin_registry_marker_v1 (key, value)
VALUES ('wasm_registry:0000000000000000000000000000000000000000000000000000000000000000:id', '00000000-0000-0000-0000-000000000001')
ON CONFLICT(key) DO NOTHING;

INSERT INTO plugin_registry_marker_v1 (key, value)
VALUES ('wasm_registry_id:00000000-0000-0000-0000-000000000001:sha256', '0000000000000000000000000000000000000000000000000000000000000000')
ON CONFLICT(key) DO NOTHING;

INSERT INTO plugin_registry_marker_v1 (key, value)
VALUES ('wasm_registry:0000000000000000000000000000000000000000000000000000000000000000:wire_version', '3')
ON CONFLICT(key) DO NOTHING;

INSERT INTO plugin_registry_marker_v1 (key, value)
VALUES ('wasm_registry:0000000000000000000000000000000000000000000000000000000000000000:supported_slots', '["router","observability_hook"]')
ON CONFLICT(key) DO NOTHING;
