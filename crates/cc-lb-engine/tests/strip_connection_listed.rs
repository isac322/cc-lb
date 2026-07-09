use http::HeaderMap;
use http::HeaderValue;

use cc_lb_engine::strip_hop_by_hop;

#[test]
fn removes_connection_listed_headers() {
    let mut headers = HeaderMap::new();
    headers.insert("connection", HeaderValue::from_static("keep-alive, X-Foo"));
    headers.insert("keep-alive", HeaderValue::from_static("timeout=5"));
    headers.insert("x-foo", HeaderValue::from_static("bar"));

    strip_hop_by_hop(&mut headers);

    assert!(!headers.contains_key("connection"));
    assert!(!headers.contains_key("keep-alive"));
    assert!(!headers.contains_key("x-foo"));
}
