INSERT INTO wasm_blobs_v2 (sha256, bytes, size_bytes, parse_validated_at, refcount, created_at)
VALUES (x'0202020202020202020202020202020202020202020202020202020202020202', zeroblob(0), 0, 0, 0, 0)
ON CONFLICT(sha256) DO NOTHING;

INSERT INTO wasm_registry_v2 (
    id,
    sha256,
    plugin_name,
    plugin_version,
    label,
    uploaded_by_admin_id,
    revision,
    wire_version,
    supported_slots,
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
    '00000000-0000-0000-0000-000000000002',
    x'0202020202020202020202020202020202020202020202020202020202020202',
    'subscription-preference',
    'builtin://subscription-preference',
    'Built-in subscription preference filter',
    '00000000-0000-0000-0000-000000000000',
    0,
    3,
    '["router"]',
    3,
    '{"identity":{"magic":[204,27,112,16,0,1,0,0],"abi_envelope":1,"plugin_name":"subscription-preference","plugin_version":"builtin"},"negotiated_functions":{"cc_lb_route":3},"negotiated_capabilities":[],"handshake_completed_at":1,"self_check_passed":true,"self_check_completed_at":1,"expires_at":9223372036854775807}',
    zeroblob(32),
    1,
    0,
    'active',
    0,
    0
)
ON CONFLICT(sha256) DO NOTHING;

INSERT INTO plugin_registry_marker_v1 (key, value)
VALUES ('wasm_registry:0202020202020202020202020202020202020202020202020202020202020202:id', '00000000-0000-0000-0000-000000000002')
ON CONFLICT(key) DO NOTHING;

INSERT INTO plugin_registry_marker_v1 (key, value)
VALUES ('wasm_registry_id:00000000-0000-0000-0000-000000000002:sha256', '0202020202020202020202020202020202020202020202020202020202020202')
ON CONFLICT(key) DO NOTHING;

INSERT INTO plugin_registry_marker_v1 (key, value)
VALUES ('wasm_registry:0202020202020202020202020202020202020202020202020202020202020202:wire_version', '3')
ON CONFLICT(key) DO NOTHING;

INSERT INTO plugin_registry_marker_v1 (key, value)
VALUES ('wasm_registry:0202020202020202020202020202020202020202020202020202020202020202:supported_slots', '["router"]')
ON CONFLICT(key) DO NOTHING;
