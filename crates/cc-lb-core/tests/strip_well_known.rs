use http::HeaderMap;
use http::HeaderValue;

use cc_lb_core::strip_hop_by_hop;

#[test]
fn removes_standard_hop_by_hop_headers() {
    let mut headers = HeaderMap::new();
    headers.insert("connection", HeaderValue::from_static("close"));
    headers.insert("keep-alive", HeaderValue::from_static("timeout=5"));
    headers.insert("proxy-authenticate", HeaderValue::from_static("Basic"));
    headers.insert("proxy-authorization", HeaderValue::from_static("Basic abc"));
    headers.insert("te", HeaderValue::from_static("trailers"));
    headers.insert("trailer", HeaderValue::from_static("x-trailer"));
    headers.insert("transfer-encoding", HeaderValue::from_static("chunked"));
    headers.insert("upgrade", HeaderValue::from_static("h2c"));
    headers.insert("proxy-connection", HeaderValue::from_static("keep-alive"));

    strip_hop_by_hop(&mut headers);

    assert!(headers.is_empty());
}
