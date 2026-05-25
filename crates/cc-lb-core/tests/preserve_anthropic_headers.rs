use http::HeaderMap;
use http::HeaderValue;

use cc_lb_core::strip_hop_by_hop;

#[test]
fn preserves_anthropic_passthrough_headers() {
    let mut headers = HeaderMap::new();
    headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
    headers.insert(
        "anthropic-beta",
        HeaderValue::from_static("tools-2024-01-01"),
    );
    headers.insert("user-agent", HeaderValue::from_static("cc-lb-test"));
    headers.insert("x-api-key", HeaderValue::from_static("sk-ant-test"));
    headers.insert(
        "authorization",
        HeaderValue::from_static("Bearer sk-ant-test"),
    );
    headers.insert("x-amz-date", HeaderValue::from_static("20260520T000000Z"));

    strip_hop_by_hop(&mut headers);

    assert_eq!(
        headers.get("anthropic-version").unwrap().to_str().unwrap(),
        "2023-06-01"
    );
    assert_eq!(
        headers.get("anthropic-beta").unwrap().to_str().unwrap(),
        "tools-2024-01-01"
    );
    assert_eq!(
        headers.get("user-agent").unwrap().to_str().unwrap(),
        "cc-lb-test"
    );
    assert_eq!(
        headers.get("x-api-key").unwrap().to_str().unwrap(),
        "sk-ant-test"
    );
    assert_eq!(
        headers.get("authorization").unwrap().to_str().unwrap(),
        "Bearer sk-ant-test"
    );
    assert_eq!(
        headers.get("x-amz-date").unwrap().to_str().unwrap(),
        "20260520T000000Z"
    );
}
