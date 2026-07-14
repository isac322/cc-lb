use super::*;

fn principal() -> Principal {
    let mut claims = serde_json::Map::new();
    claims.insert("scope".to_owned(), serde_json::Value::from("inference"));
    Principal {
        id: "tenant-a".to_owned(),
        kind: PrincipalKind::ApiKey,
        claims,
    }
}

fn context() -> RoutingContext {
    let mut headers = http::HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    headers.insert(
        http::header::AUTHORIZATION,
        http::HeaderValue::from_static("Bearer secret"),
    );
    RoutingContext {
        request_id: "req-123".to_owned(),
        thread_id: None,
        requested_service_tier: Some("priority".to_owned()),
        downstream_headers: headers,
        method: http::Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: bytes::Bytes::from_static(b"{\"msg\":\"hi\"}"),
        canonical_model_id: "claude-fixture".to_owned(),
        cache_pricing: cc_lb_domain::CachePricingSummary::default(),
    }
}

#[test]
fn v1_encoding_keeps_legacy_layout() {
    let principal = principal();
    let context = context();
    FilterWireRequest {
        wire_version: WireVersion::V1,
        ctx: &context,
        principal: &principal,
        candidates: &[],
        cookie_redaction: false,
    }
    .encode(|bytes| {
        let archived =
            rkyv::access::<cc_lb_plugin_wire::v1::ArchivedFilterRequest, RkyvError>(bytes)
                .expect("archived V1");
        assert_eq!(archived.headers.len(), 1);
        let header_name: &str = &archived.headers[0].name;
        assert_eq!(header_name, "content-type");
    })
    .expect("encode V1");
}

#[test]
fn v2_encoding_includes_requested_service_tier() {
    let principal = principal();
    let context = context();
    FilterWireRequest {
        wire_version: WireVersion::V2,
        ctx: &context,
        principal: &principal,
        candidates: &[],
        cookie_redaction: false,
    }
    .encode(|bytes| {
        let archived =
            rkyv::access::<cc_lb_plugin_wire::v2::ArchivedFilterRequest, RkyvError>(bytes)
                .expect("archived V2");
        let tier: Option<&str> = archived.service_tier.as_ref().map(|value| &**value);
        assert_eq!(tier, Some("priority"));
    })
    .expect("encode V2");
}
