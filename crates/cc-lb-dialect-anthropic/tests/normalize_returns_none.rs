use bytes::Bytes;
use cc_lb_dialect_anthropic::AnthropicDirectDialect;
use cc_lb_plugin_api::UpstreamDialect;
use http::StatusCode;

#[test]
fn direct_normalize_error_returns_none() {
    let body = Bytes::from_static(
        br#"{"type":"error","error":{"type":"authentication_error","message":"bad key"}}"#,
    );

    assert_eq!(
        AnthropicDirectDialect::default().normalize_error(StatusCode::UNAUTHORIZED, &body),
        None
    );
}
