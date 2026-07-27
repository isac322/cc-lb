-- Materialize the historical default subscription-preference behaviour as the explicit
-- subscription-preference Router entry. This migration stores no scalar state:
-- an entry's presence is the sole enabled signal.
--
-- Only principals with an empty Router chain receive the entry. Existing Router
-- chains are explicit operator configuration and must remain untouched.
INSERT INTO plugin_chains_v2 (
    id,
    principal_id,
    slot,
    wasm_registry_id,
    order_value,
    config,
    sse_per_event,
    batched_events_per_flush,
    batched_flush_ms,
    revision,
    created_at,
    updated_at
)
SELECT
    lower(hex(randomblob(4))) || '-' ||
        lower(hex(randomblob(2))) || '-' ||
        '4' || substr(lower(hex(randomblob(2))), 2) || '-' ||
        substr('89ab', (random() & 3) + 1, 1) ||
        substr(lower(hex(randomblob(2))), 2) || '-' ||
        lower(hex(randomblob(6))),
    principals_v1.id,
    'router',
    '00000000-0000-0000-0000-000000000002',
    0,
    '{}',
    0,
    1,
    100,
    0,
    CAST(strftime('%s', 'now') AS INTEGER),
    CAST(strftime('%s', 'now') AS INTEGER)
  FROM principals_v1
 WHERE deleted_at IS NULL
   AND NOT EXISTS (
       SELECT 1
         FROM plugin_chains_v2 AS existing
        WHERE existing.principal_id = principals_v1.id
          AND existing.slot = 'router'
   );
