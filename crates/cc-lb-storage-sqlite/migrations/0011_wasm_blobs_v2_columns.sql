PRAGMA defer_foreign_keys = ON;
PRAGMA legacy_alter_table = ON;

ALTER TABLE wasm_blobs_v2 RENAME TO wasm_blobs_v2_old;

CREATE TABLE wasm_blobs_v2 (
    sha256 BLOB PRIMARY KEY,
    bytes BLOB NOT NULL CHECK (length(bytes) <= 33554432),
    size_bytes INTEGER NOT NULL DEFAULT 0 CHECK (size_bytes >= 0 AND size_bytes <= 33554432),
    parse_validated_at INTEGER,
    refcount INTEGER NOT NULL DEFAULT 0 CHECK (refcount >= 0),
    created_at INTEGER NOT NULL
);

INSERT INTO wasm_blobs_v2 (
    sha256,
    bytes,
    size_bytes,
    parse_validated_at,
    refcount,
    created_at
)
SELECT
    sha256,
    bytes,
    length(bytes),
    NULL,
    (
        SELECT COUNT(*)
          FROM plugin_chains_v2
         WHERE plugin_chains_v2.wasm_sha256 = wasm_blobs_v2_old.sha256
    ),
    created_at
FROM wasm_blobs_v2_old;

PRAGMA legacy_alter_table = OFF;
