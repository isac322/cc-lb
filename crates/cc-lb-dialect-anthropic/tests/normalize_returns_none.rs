use bytes::Bytes;
use cc_lb_dialect_anthropic::{AnthropicDirectDialect, CustomAnthropicSpecDialect};
use cc_lb_plugin_api::UpstreamDialect;
use http::StatusCode;

#[test]
fn direct_normalize_error_returns_none() {
    let body = Bytes::from_static(
        br#"{"type":"error","error":{"type":"authentication_error","message":"bad key"}}"#,
    );

    assert_eq!(
        AnthropicDirectDialect.normalize_error(StatusCode::UNAUTHORIZED, &body),
        None
    );
}

#[test]
fn custom_normalize_error_returns_none() {
    let body =
        Bytes::from_static(br#"{"type":"error","error":{"type":"api_error","message":"gateway"}}"#);

    assert_eq!(
        CustomAnthropicSpecDialect.normalize_error(StatusCode::BAD_GATEWAY, &body),
        None
    );
}
