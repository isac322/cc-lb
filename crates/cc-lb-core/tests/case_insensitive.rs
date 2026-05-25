use http::HeaderMap;
use http::HeaderValue;

use cc_lb_core::strip_hop_by_hop;

#[test]
fn handles_mixed_case_hop_by_hop_headers() {
    let mut headers = HeaderMap::new();
    headers.insert("CONNECTION", HeaderValue::from_static("Keep-Alive, X-Foo"));
    headers.insert("Keep-Alive", HeaderValue::from_static("timeout=5"));
    headers.insert("X-Foo", HeaderValue::from_static("bar"));

    strip_hop_by_hop(&mut headers);

    assert!(!headers.contains_key("CONNECTION"));
    assert!(!headers.contains_key("Keep-Alive"));
    assert!(!headers.contains_key("X-Foo"));
}
