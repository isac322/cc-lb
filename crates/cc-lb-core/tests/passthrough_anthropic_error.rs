use bytes::Bytes;
use cc_lb_core::{ErrorNormalizer, UpstreamKind};
use http::header::CONTENT_ENCODING;
use http::{HeaderMap, HeaderValue, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;

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

#[tokio::test]
async fn gzipped_anthropic_error_body_is_sent_as_readable_json() {
    let normalizer = ErrorNormalizer::new();
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let response = normalizer.build_http_error_response(
        UpstreamKind::AnthropicDirect,
        StatusCode::BAD_REQUEST,
        &gzipped_anthropic_error_body(),
        &headers,
    );

    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    let body: Value = serde_json::from_slice(&body).expect("body is readable JSON");

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(!headers.contains_key(CONTENT_ENCODING));
    assert_eq!(body["error"]["message"], "quota");
}

fn gzipped_anthropic_error_body() -> Bytes {
    Bytes::from_static(&[
        31, 139, 8, 0, 0, 0, 0, 0, 2, 255, 171, 86, 42, 169, 44, 72, 85, 178, 82, 74, 45, 42, 202,
        47, 82, 210, 129, 210, 86, 213, 48, 241, 204, 188, 178, 196, 156, 204, 148, 248, 162, 212,
        194, 210, 212, 226, 146, 120, 152, 186, 220, 212, 226, 226, 196, 116, 144, 138, 194, 210,
        252, 146, 68, 165, 218, 90, 0, 74, 93, 250, 125, 75, 0, 0, 0,
    ])
}
