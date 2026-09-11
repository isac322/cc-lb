use std::collections::BTreeSet;

use http::HeaderMap;
use http::HeaderValue;

use cc_lb_engine::strip_hop_by_hop;

#[test]
fn removes_standard_hop_by_hop_headers() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "connection",
        HeaderValue::from_static("close, keep-alive, X-Foo"),
    );
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
    strip_hop_by_hop(&mut headers);
    let after = header_names(&headers);
    let removed = before.difference(&after).cloned().collect::<BTreeSet<_>>();

    let expected_removed = [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "proxy-connection",
        "x-foo",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    let expected_remaining = [
        "anthropic-version",
        "anthropic-beta",
        "user-agent",
        "x-api-key",
        "authorization",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();

    assert_eq!(removed, expected_removed);
    assert_eq!(after, expected_remaining);
}

fn header_names(headers: &HeaderMap) -> BTreeSet<String> {
    headers
        .keys()
        .map(|name| name.as_str().to_owned())
        .collect()
}
