use bytes::Bytes;
use cc_lb_dialect_bedrock::BedrockRuntimeDialect;
use cc_lb_plugin_api::UpstreamDialect;
use http::StatusCode;

#[test]
fn validation_exception_normalizes_to_anthropic_invalid_request() {
    let normalized = BedrockRuntimeDialect
        .normalize_error(
            StatusCode::BAD_REQUEST,
            &Bytes::from_static(br#"{"__type":"ValidationException","message":"bad request"}"#),
        )
        .expect("bedrock error normalizes");
    let body: serde_json::Value = serde_json::from_slice(&normalized).expect("json body");

    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "invalid_request_error");
    assert_eq!(body["error"]["message"], "bad request");
}

#[test]
fn server_error_without_bedrock_type_wraps_as_api_error() {
    let normalized = BedrockRuntimeDialect
        .normalize_error(
            StatusCode::BAD_GATEWAY,
            &Bytes::from_static(br#"{"oops":true}"#),
        )
        .expect("5xx normalizes");
    let body: serde_json::Value = serde_json::from_slice(&normalized).expect("json body");

    assert_eq!(body["error"]["type"], "api_error");
}
