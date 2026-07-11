use std::time::Duration;

use cc_lb_plugin_api::{
    CredentialStrategy, ObserveEvent, Principal, PrincipalKind, PrincipalQuotas, Upstream,
};
use cc_lb_routing::RoutingContext;
use cc_lb_upstream::DialectShapeContext;
use http::{HeaderMap, Method, StatusCode};

#[test]
fn upstream_variants_and_principal_serde_compile() {
    let variants = [Upstream::AnthropicDirect { base_url: None }];

    assert_eq!(variants.len(), 1);

    let principal = Principal {
        id: "alice".to_owned(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    };
    let json = serde_json::to_string(&principal).unwrap();
    let decoded: Principal = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, principal);
}

#[test]
fn remaining_public_types_compile() {
    let _strategy = CredentialStrategy::ApiKey;
    let shape_context = DialectShapeContext {
        request_id: "req-1".to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: bytes::Bytes::from_static(b"{}"),
    };
    assert_eq!(shape_context.method, Method::POST);

    let routing_context = RoutingContext {
        request_id: "req-1".to_owned(),
        thread_id: None,
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: bytes::Bytes::from_static(b"{}"),
        canonical_model_id: String::new(),
        cache_pricing: cc_lb_plugin_api::CachePricingSummary::default(),
    };
    assert_eq!(routing_context.method, Method::POST);

    let quotas = PrincipalQuotas {
        requests_per_window: 10,
        input_tokens_per_window: 100,
        output_tokens_per_window: 100,
        window: Duration::from_secs(60),
        allowed_models: vec!["claude-*".to_owned()],
    };
    assert_eq!(quotas.window, Duration::from_secs(60));

    let event = ObserveEvent::RequestFinished {
        status: StatusCode::OK,
        input_tokens: Some(1),
        output_tokens: Some(2),
        cache_creation_input_tokens: Some(3),
        cache_read_input_tokens: Some(4),
        duration_ms: 5,
    };
    assert_eq!(
        event,
        ObserveEvent::RequestFinished {
            status: StatusCode::OK,
            input_tokens: Some(1),
            output_tokens: Some(2),
            cache_creation_input_tokens: Some(3),
            cache_read_input_tokens: Some(4),
            duration_ms: 5,
        }
    );
}
