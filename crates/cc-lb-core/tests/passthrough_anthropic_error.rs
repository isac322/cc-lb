use bytes::Bytes;
use cc_lb_core::{ErrorNormalizer, UpstreamKind};
use http::StatusCode;

#[test]
fn anthropic_shape_error_passes_through_unchanged() {
    let normalizer = ErrorNormalizer::new();
    let body = Bytes::from_static(
        br#"{"type":"error","error":{"type":"authentication_error","message":"bad key"}}"#,
    );

    let normalized = normalizer.normalize_http_error(
        UpstreamKind::AnthropicDirect,
        StatusCode::UNAUTHORIZED,
        &body,
    );

    assert_eq!(normalized, body);
}
