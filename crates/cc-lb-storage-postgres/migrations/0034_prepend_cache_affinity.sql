ALTER TABLE plugin_chains_v2
    DROP CONSTRAINT IF EXISTS plugin_chains_v2_wasm_registry_fk;

INSERT INTO plugin_chains_v2 (
    id,
    principal_id,
    slot,
    order_value,
    wasm_registry_id,
    config,
    sse_per_event,
    batched_events_per_flush,
    batched_flush_ms,
    revision,
    wire_version
)
SELECT
    (
        substr(md5(p.id::text || ':builtin/cache-affinity'), 1, 8) || '-' ||
        substr(md5(p.id::text || ':builtin/cache-affinity'), 9, 4) || '-' ||
        substr(md5(p.id::text || ':builtin/cache-affinity'), 13, 4) || '-' ||
        substr(md5(p.id::text || ':builtin/cache-affinity'), 17, 4) || '-' ||
        substr(md5(p.id::text || ':builtin/cache-affinity'), 21, 12)
    )::uuid,
    p.id,
    'router',
    COALESCE((
        SELECT MIN(c.order_value) - 1000
        FROM plugin_chains_v2 c
        WHERE c.principal_id = p.id AND c.slot = 'router'
    ), 1000),
    '00000000-0000-0000-0000-000000000001'::uuid,
    '{}'::jsonb,
    FALSE,
    1,
    100,
    0,
    3
FROM principals_v1 p
WHERE p.deleted_at IS NULL
  AND NOT EXISTS (
      SELECT 1
      FROM plugin_chains_v2 existing
      WHERE existing.principal_id = p.id
        AND existing.slot = 'router'
        AND existing.wasm_registry_id = '00000000-0000-0000-0000-000000000001'::uuid
  )
ON CONFLICT (id) DO NOTHING;
