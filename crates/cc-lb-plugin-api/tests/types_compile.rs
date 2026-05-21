use std::time::Duration;

use bytes::Bytes;
use cc_lb_plugin_api::{
    AuthStrategy, ObserveEvent, Principal, PrincipalKind, PrincipalQuotas, RequestContext, Upstream,
};
use http::{HeaderMap, Method, StatusCode};

#[test]
fn upstream_variants_and_principal_serde_compile() {
    let variants = [
        Upstream::AnthropicDirect,
        Upstream::BedrockRuntime {
            region: "us-east-1".to_owned(),
        },
        Upstream::BedrockMantle {
            region: "us-west-2".to_owned(),
        },
        Upstream::Vertex {
            project: "project-a".to_owned(),
            region: "us-central1".to_owned(),
        },
        Upstream::CustomAnthropicSpec {
            base_url: "https://gateway.example.test".parse().unwrap(),
        },
    ];

    assert_eq!(variants.len(), 5);

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
    let _strategy = AuthStrategy::AwsSigV4;
    let ctx = RequestContext {
        request_id: "req-1".to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from_static(b"{}"),
    };
    assert_eq!(ctx.method, Method::POST);

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
        duration_ms: 3,
    };
    assert_eq!(
        event,
        ObserveEvent::RequestFinished {
            status: StatusCode::OK,
            input_tokens: Some(1),
            output_tokens: Some(2),
            duration_ms: 3,
        }
    );
}
