PRAGMA defer_foreign_keys = ON;
PRAGMA legacy_alter_table = ON;

DROP INDEX IF EXISTS wasm_registry_v2_status_idx;

ALTER TABLE wasm_registry_v2 RENAME TO wasm_registry_v2_old;

CREATE TABLE wasm_registry_v2 (
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
    updated_at INTEGER NOT NULL,
    id BLOB,
    label TEXT,
    uploaded_by_admin_id BLOB,
    revision INTEGER NOT NULL DEFAULT 1,
    wire_version INTEGER NOT NULL DEFAULT 1,
    supported_slots TEXT NOT NULL DEFAULT '[]'
);

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
    updated_at,
    id,
    label,
    uploaded_by_admin_id,
    revision,
    wire_version,
    supported_slots
)
SELECT
    old.sha256,
    old.plugin_name,
    old.plugin_version,
    old.abi_envelope,
    old.augmented_metadata,
    old.host_offer_hash,
    old.handshake_schema_version,
    old.last_handshake_at,
    old.status,
    old.created_at,
    old.updated_at,
    COALESCE(
        (SELECT value FROM plugin_registry_marker_v1 marker WHERE marker.key = 'wasm_registry:' || lower(hex(old.sha256)) || ':id'),
        lower(hex(substr(old.sha256, 1, 4))) || '-' ||
        lower(hex(substr(old.sha256, 5, 2))) || '-' ||
        lower(hex(substr(old.sha256, 7, 2))) || '-' ||
        lower(hex(substr(old.sha256, 9, 2))) || '-' ||
        lower(hex(substr(old.sha256, 11, 6)))
    ),
    COALESCE(
        json_extract((SELECT value FROM plugin_registry_marker_v1 marker WHERE marker.key = 'wasm_registry:' || lower(hex(old.sha256)) || ':label'), '$'),
        CASE WHEN old.sha256 = zeroblob(32) THEN 'Built-in cache affinity filter' ELSE NULL END
    ),
    COALESCE(
        (SELECT value FROM plugin_registry_marker_v1 marker WHERE marker.key = 'wasm_registry:' || lower(hex(old.sha256)) || ':uploaded_by'),
        '00000000-0000-0000-0000-000000000000'
    ),
    COALESCE(
        CAST((SELECT value FROM plugin_registry_marker_v1 marker WHERE marker.key = 'wasm_registry:' || lower(hex(old.sha256)) || ':revision') AS INTEGER),
        0
    ),
    COALESCE(
        CAST((SELECT value FROM plugin_registry_marker_v1 marker WHERE marker.key = 'wasm_registry:' || lower(hex(old.sha256)) || ':wire_version') AS INTEGER),
        CASE WHEN old.sha256 = zeroblob(32) THEN 3 ELSE 1 END
    ),
    COALESCE(
        (SELECT value FROM plugin_registry_marker_v1 marker WHERE marker.key = 'wasm_registry:' || lower(hex(old.sha256)) || ':supported_slots'),
        CASE WHEN old.sha256 = zeroblob(32) THEN '["router"]' ELSE '[]' END
    )
FROM wasm_registry_v2_old old;

CREATE INDEX IF NOT EXISTS wasm_registry_v2_status_idx
    ON wasm_registry_v2 (status, plugin_name, sha256);
CREATE UNIQUE INDEX IF NOT EXISTS wasm_registry_v2_id_idx
    ON wasm_registry_v2 (id);
CREATE UNIQUE INDEX IF NOT EXISTS wasm_registry_v2_plugin_name_idx
    ON wasm_registry_v2 (plugin_name);
CREATE UNIQUE INDEX IF NOT EXISTS wasm_registry_v2_label_idx
    ON wasm_registry_v2 (label);

DELETE FROM plugin_registry_marker_v1
 WHERE key GLOB 'wasm_registry:*'
    OR key GLOB 'wasm_registry_id:*';

PRAGMA legacy_alter_table = OFF;
