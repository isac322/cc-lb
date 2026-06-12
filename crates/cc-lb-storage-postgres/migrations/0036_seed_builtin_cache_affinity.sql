WITH builtin AS (
    SELECT
        decode(repeat('00', 32), 'hex') AS sha256,
        '00000000-0000-0000-0000-000000000001'::uuid AS id
)
INSERT INTO wasm_blobs_v2 (sha256, bytes, size_bytes, parse_validated_at, refcount, created_at)
SELECT sha256, ''::bytea, 0, to_timestamp(0), 0, to_timestamp(0)
FROM builtin
ON CONFLICT (sha256) DO NOTHING;

WITH builtin AS (
    SELECT
        decode(repeat('00', 32), 'hex') AS sha256,
        '00000000-0000-0000-0000-000000000001'::uuid AS id
)
INSERT INTO wasm_registry_v2 (
    id,
    sha256,
    name,
    original_filename,
    label,
    uploaded_at,
    uploaded_by_admin_id,
    revision
)
SELECT
    id,
    sha256,
    'cache-affinity',
    'builtin://cache-affinity',
    'Built-in cache affinity filter',
    to_timestamp(0),
    '00000000-0000-0000-0000-000000000000'::uuid,
    0
FROM builtin
ON CONFLICT (id) DO NOTHING;
