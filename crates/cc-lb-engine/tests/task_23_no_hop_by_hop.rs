use std::collections::BTreeSet;

use http::HeaderMap;
use http::HeaderValue;

use cc_lb_engine::strip_hop_by_hop;

#[test]
fn writes_hop_by_hop_snapshot() {
    let mut headers = HeaderMap::new();
    headers.insert("connection", HeaderValue::from_static("keep-alive, X-Foo"));
    headers.insert("keep-alive", HeaderValue::from_static("timeout=5"));
    headers.insert("x-foo", HeaderValue::from_static("bar"));
    headers.insert("proxy-authenticate", HeaderValue::from_static("Basic"));
    headers.insert("proxy-authorization", HeaderValue::from_static("Basic abc"));
    headers.insert("te", HeaderValue::from_static("trailers"));
    headers.insert("trailer", HeaderValue::from_static("x-trailer"));
    headers.insert("transfer-encoding", HeaderValue::from_static("chunked"));
    headers.insert("upgrade", HeaderValue::from_static("h2c"));
    headers.insert("proxy-connection", HeaderValue::from_static("keep-alive"));
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

    let before = header_names(&headers);
    let mut stripped = headers.clone();
    strip_hop_by_hop(&mut stripped);
    let after = header_names(&stripped);
    let removed = before.difference(&after).cloned().collect::<BTreeSet<_>>();

    let expected_removed = [
        "connection".to_owned(),
        "keep-alive".to_owned(),
        "proxy-authenticate".to_owned(),
        "proxy-authorization".to_owned(),
        "te".to_owned(),
        "trailer".to_owned(),
        "transfer-encoding".to_owned(),
        "upgrade".to_owned(),
        "proxy-connection".to_owned(),
        "x-foo".to_owned(),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();

    assert_eq!(removed, expected_removed);
}

fn header_names(headers: &HeaderMap) -> BTreeSet<String> {
    headers
        .keys()
        .map(|name| name.as_str().to_owned())
        .collect()
}
