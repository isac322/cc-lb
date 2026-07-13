pub mod filter;
pub mod observe;
mod scratch;
pub mod shape;

pub use filter::WasmtimeFilterPlugin;
pub use observe::WasmtimeObservabilityHookPlugin;
pub(super) use scratch::{access_archived_scoped_or_copy, serialize_with_input_scratch};
pub use shape::WasmtimeUpstreamDialect;

#[cfg(test)]
mod tests {
    use std::mem::align_of;

    use cc_lb_plugin_wire::{ArchivedFilterResponse, FilterResponse, PerCandidateReason};
    use rkyv::{rancor::Error as RkyvError, util::AlignedVec};

    use super::access_archived_scoped_or_copy;

    #[test]
    fn access_archived_uses_aligned_guest_bytes_and_copies_misaligned_bytes() {
        // Given
        let response = FilterResponse {
            results: Box::new([PerCandidateReason {
                upstream_id: Box::from("11111111-1111-1111-1111-111111111111"),
                decision: Box::from("accept"),
                reason: Box::from("aligned-or-copy"),
            }]),
        };
        let aligned = rkyv::to_bytes::<RkyvError>(&response).expect("encode");
        let aligned_start = aligned.as_ptr() as usize;
        let aligned_end = aligned_start + aligned.len();

        // When
        let aligned_result =
            access_archived_scoped_or_copy::<ArchivedFilterResponse, _>(&aligned, |archived| {
                let archived_address = std::ptr::from_ref(archived).cast::<u8>() as usize;
                let reason: &str = &archived.results[0].reason;
                (
                    reason.to_owned(),
                    (aligned_start..aligned_end).contains(&archived_address),
                )
            })
            .expect("aligned access");

        let mut storage = AlignedVec::<16>::with_capacity(aligned.len() + 1);
        storage.push(0);
        storage.extend_from_slice(&aligned);
        let misaligned = &storage[1..];
        assert_ne!(
            misaligned.as_ptr() as usize % align_of::<ArchivedFilterResponse>(),
            0,
            "fixture must use an actually misaligned host pointer",
        );
        let misaligned_start = misaligned.as_ptr() as usize;
        let misaligned_end = misaligned_start + misaligned.len();
        let misaligned_result =
            access_archived_scoped_or_copy::<ArchivedFilterResponse, _>(misaligned, |archived| {
                let archived_address = std::ptr::from_ref(archived).cast::<u8>() as usize;
                let reason: &str = &archived.results[0].reason;
                (
                    reason.to_owned(),
                    !(misaligned_start..misaligned_end).contains(&archived_address),
                )
            })
            .expect("misaligned fallback access");

        // Then
        assert_eq!(aligned_result, ("aligned-or-copy".to_owned(), true));
        assert_eq!(misaligned_result, ("aligned-or-copy".to_owned(), true));
    }

    #[test]
    fn access_archived_rejects_invalid_bytes() {
        // Given: invalid bytes (all zeros)
        let invalid_bytes = vec![0xAA; 32];

        // When: we try to access them as ArchivedFilterResponse
        let result =
            access_archived_scoped_or_copy::<ArchivedFilterResponse, _>(&invalid_bytes, |_| {});

        // Then: it must return an error
        assert!(result.is_err());
    }

    use crate::wasm_host::WasmtimeFilterPlugin;
    use bytes::Bytes;
    use cc_lb_domain::{Principal, PrincipalKind, UpstreamCandidate, UpstreamKind};
    use cc_lb_routing::{FilterPlugin, RoutingContext};
    use cc_lb_runtime_wasmtime::policy::PluginWireBounds;
    use cc_lb_runtime_wasmtime::{
        HotEngineConfig, RuntimeSlotKey, WasmPluginWireDispatch, WasmtimeRuntime,
    };
    use http::{HeaderMap, HeaderValue, Method};
    use std::sync::Arc;
    use uuid::Uuid;

    fn append_custom_section(module: &mut Vec<u8>, name: &str, data: &[u8]) {
        let mut payload = Vec::new();
        encode_leb128(&mut payload, name.len() as u64);
        payload.extend_from_slice(name.as_bytes());
        payload.extend_from_slice(data);

        module.push(0);
        encode_leb128(module, payload.len() as u64);
        module.extend_from_slice(&payload);
    }

    fn encode_leb128(buf: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            buf.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    fn wat_with_custom_sections(
        wat: &str,
        metadata_json: &[u8],
        hook: cc_lb_plugin_wire::schema::HookKind,
    ) -> Vec<u8> {
        let mut module = wat::parse_str(wat).expect("valid wat");
        if matches!(hook, cc_lb_plugin_wire::schema::HookKind::Filter) {
            let fingerprint = <cc_lb_plugin_wire::v1::FilterRequest as cc_lb_plugin_wire::schema::WireSchema>::FINGERPRINT;
            append_custom_section(&mut module, "cc_lb.schema.filter.v1", &fingerprint);
        } else if matches!(hook, cc_lb_plugin_wire::schema::HookKind::Shape) {
            let fingerprint_shape = <cc_lb_plugin_wire::v1::ShapeRequest as cc_lb_plugin_wire::schema::WireSchema>::FINGERPRINT;
            let fingerprint_resp = <cc_lb_plugin_wire::v1::TransformResponseRequest as cc_lb_plugin_wire::schema::WireSchema>::FINGERPRINT;
            let fingerprint_sse = <cc_lb_plugin_wire::v1::TransformSseEventRequest as cc_lb_plugin_wire::schema::WireSchema>::FINGERPRINT;
            append_custom_section(&mut module, "cc_lb.schema.shape.v1", &fingerprint_shape);
            append_custom_section(
                &mut module,
                "cc_lb.schema.transform_response.v1",
                &fingerprint_resp,
            );
            append_custom_section(
                &mut module,
                "cc_lb.schema.transform_sse_event.v1",
                &fingerprint_sse,
            );
        }
        append_custom_section(&mut module, "cc_lb.plugin.v1", metadata_json);
        module
    }

    fn fixture_principal() -> Principal {
        Principal {
            id: "tenant-a".to_owned(),
            kind: PrincipalKind::ApiKey,
            claims: serde_json::Map::new(),
        }
    }

    fn fixture_routing_context() -> RoutingContext {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        RoutingContext {
            request_id: "req-123".to_owned(),
            thread_id: None,
            downstream_headers: headers,
            method: Method::POST,
            path: "/v1/messages".to_owned(),
            query: None,
            body_bytes: Bytes::from_static(b"{\"msg\":\"hi\"}"),
            canonical_model_id: "claude-fixture".to_owned(),
            cache_pricing: cc_lb_domain::CachePricingSummary::default(),
        }
    }

    fn fixture_candidates() -> Vec<UpstreamCandidate> {
        vec![UpstreamCandidate {
            upstream_id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
            name: "upstream-1".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            observed_rate_limits: Vec::new(),
            subscription_quotas: Vec::new(),
            observed_at_unix_secs: 0,
            cache_score: None,
            base_url: None,
            plan_capacity_ratio: None,
            organization_type: None,
            rate_limit_tier: None,
            seat_tier: None,
        }]
    }

    #[test]
    fn test_oversized_output_past_wire_bound() {
        let response = FilterResponse {
            results: Box::new([PerCandidateReason {
                upstream_id: Box::from("11111111-1111-1111-1111-111111111111"),
                decision: Box::from("accept"),
                reason: Box::from("aligned-or-copy"),
            }]),
        };
        let bytes = rkyv::to_bytes::<RkyvError>(&response).expect("encode");

        let packed = cc_lb_plugin_wire::schema::pack_ret(16, bytes.len() as u32);
        let mut data_str = String::new();
        for &b in bytes.as_slice() {
            data_str.push_str(&format!("\\{:02x}", b));
        }
        let wat = format!(
            r#"
            (module
                (memory (export "memory") 1)
                (data (i32.const 16) "{}")
                (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 1024)
                (func (export "cc_lb_free") (param i32 i32 i32))
                (func (export "cc_lb_filter") (param i32 i32) (result i64)
                    i64.const {}
                )
            )
            "#,
            data_str, packed as i64
        );
        let metadata = r#"{"name":"malicious-filter","version":"0.0.1","description":"malicious filter plugin","usage":"test usage","hooks":{"filter":{"wire_version":1,"description":"filter hook","usage":"call filter"}}}"#;
        let wasm_bytes = wat_with_custom_sections(
            &wat,
            metadata.as_bytes(),
            cc_lb_plugin_wire::schema::HookKind::Filter,
        );

        let config = HotEngineConfig {
            wire_bounds: PluginWireBounds {
                output_body_bytes: 10,
                ..PluginWireBounds::default()
            },
            ..HotEngineConfig::default()
        };
        let runtime = Arc::new(WasmtimeRuntime::new(config).expect("engine"));
        let slot = runtime
            .register_filter(
                RuntimeSlotKey::global("oversized"),
                "oversized",
                &wasm_bytes,
            )
            .expect("register");
        let dispatch = Arc::new(WasmPluginWireDispatch::from_slot(
            slot,
            runtime.config_arc(),
        ));
        let filter_plugin = WasmtimeFilterPlugin::new(dispatch, Uuid::new_v4(), "oversized");

        let ctx = fixture_routing_context();
        let principal = fixture_principal();
        let candidates = fixture_candidates();
        let result = filter_plugin.filter(&ctx, &principal, &candidates);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("exceeds wire_bounds.output_body_bytes"));
    }
}
