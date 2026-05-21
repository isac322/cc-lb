use std::sync::Arc;

use bytes::Bytes;
use cc_lb_core::{ErrorNormalizer, UpstreamKind};
use cc_lb_dialect_vertex::VertexDialect;
use http::StatusCode;
use serde_json::Value;

#[test]
fn vertex_invalid_argument_normalizes_to_anthropic_shape() {
    let normalizer =
        ErrorNormalizer::new().with_dialect(UpstreamKind::Vertex, Arc::new(VertexDialect));
    let body = Bytes::from_static(
        br#"{"error":{"code":400,"status":"INVALID_ARGUMENT","message":"bad"}}"#,
    );

    let normalized =
        normalizer.normalize_http_error(UpstreamKind::Vertex, StatusCode::BAD_REQUEST, &body);
    let value: Value = serde_json::from_slice(&normalized).expect("normalized JSON parses");

    assert_eq!(value.get("type").and_then(Value::as_str), Some("error"));
    assert_eq!(
        value
            .get("error")
            .and_then(|error| error.get("type"))
            .and_then(Value::as_str),
        Some("invalid_request_error")
    );
    assert_eq!(
        value
            .get("error")
            .and_then(|error| error.get("message"))
            .and_then(Value::as_str),
        Some("bad")
    );
}
