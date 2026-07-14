use cc_lb_plugin_wire::{HookKind, WireVersion, schema};

#[test]
fn root_schema_exports_describe_supported_hooks() {
    assert_eq!(WireVersion::V1.as_u8(), 1);
    assert_eq!(WireVersion::V2.as_u8(), 2);
    assert_eq!(WireVersion::from_u8(1), Some(WireVersion::V1));
    assert_eq!(WireVersion::from_u8(2), Some(WireVersion::V2));
    assert_eq!(WireVersion::V1.as_str(), "v1");
    assert_eq!(WireVersion::V2.as_str(), "v2");

    assert_eq!(HookKind::Filter.as_str(), "filter");
    assert_eq!(HookKind::Filter.export_name(), "cc_lb_filter");
    assert_eq!(HookKind::Filter.section_prefix(), "cc_lb.schema.filter");
    assert_eq!(HookKind::parse("shape"), Some(HookKind::Shape));
    assert_eq!(HookKind::TransformResponse.as_str(), "transform_response");
    assert_eq!(
        HookKind::TransformResponse.export_name(),
        "cc_lb_transform_response"
    );
    assert_eq!(
        HookKind::TransformResponse.section_prefix(),
        "cc_lb.schema.transform_response"
    );
    assert_eq!(
        HookKind::parse("transform_sse_event"),
        Some(HookKind::TransformSseEvent)
    );
    assert!(schema::host_supports(
        HookKind::TransformResponse,
        WireVersion::V1
    ));
    assert!(schema::host_supports(
        HookKind::TransformSseEvent,
        WireVersion::V1
    ));
    assert!(schema::host_supports(HookKind::Observe, WireVersion::V1));
    assert!(schema::host_supports(HookKind::Filter, WireVersion::V1));
    assert!(schema::host_supports(HookKind::Filter, WireVersion::V2));
    assert!(!schema::host_supports(HookKind::Shape, WireVersion::V2));
    assert!(!schema::host_supports(HookKind::Observe, WireVersion::V2));
}

#[cfg(feature = "std")]
#[test]
fn plugin_metadata_parse_validates_required_fields() {
    use cc_lb_plugin_wire::{MetadataError, PluginMetadata};

    let metadata = PluginMetadata::parse(
        br#"{
            "name":"cache-aware",
            "version":"0.1.0",
            "description":"Routes requests by cache state.",
            "usage":"Attach to filter chains.",
            "hooks":{
                "filter":{
                    "wire_version":1,
                    "description":"Filters upstream candidates.",
                    "usage":"Return accept/reject reasons."
                },
                "transform_response":{
                    "wire_version":1,
                    "description":"Transforms buffered responses.",
                    "usage":"Return unchanged or replacement response parts."
                },
                "transform_sse_event":{
                    "wire_version":1,
                    "description":"Transforms one SSE event.",
                    "usage":"Return unchanged, replacement events, or drop."
                }
            }
        }"#,
    )
    .expect("valid metadata parses");

    assert_eq!(metadata.name, "cache-aware");
    assert_eq!(metadata.hooks["filter"].wire_version, 1);
    assert_eq!(metadata.hooks["transform_response"].wire_version, 1);
    assert_eq!(metadata.hooks["transform_sse_event"].wire_version, 1);

    let error = PluginMetadata::parse(
        br#"{
            "name":"cache-aware",
            "version":"0.1.0",
            "description":"Routes requests by cache state.",
            "usage":"Attach to filter chains.",
            "hooks":{
                "sign":{
                    "wire_version":1,
                    "description":"Invalid hook.",
                    "usage":"Should fail."
                }
            }
        }"#,
    )
    .expect_err("unknown hook is rejected");

    match error {
        MetadataError::UnknownHook(hook) => assert_eq!(hook, "sign"),
        other => panic!("unexpected error: {other}"),
    }
}
