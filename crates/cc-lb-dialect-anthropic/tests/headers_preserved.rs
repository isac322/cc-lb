mod common;

use bytes::Bytes;
use cc_lb_dialect_anthropic::AnthropicDirectDialect;
use cc_lb_plugin_api::{shape_request, Upstream};
use http::{HeaderMap, HeaderValue, Method};

#[test]
fn anthropic_and_custom_headers_are_preserved_exactly() {
    let mut headers = common::anthropic_headers();
    headers.append("x-repeat", HeaderValue::from_static("one"));
    headers.append("x-repeat", HeaderValue::from_static("two"));
    let ctx = common::request_context(
        Method::POST,
        "/v1/messages",
        None,
        Bytes::from_static(b"{}"),
        headers.clone(),
    );

    let shaped = shape_request(
        &AnthropicDirectDialect,
        &ctx,
        &Upstream::AnthropicDirect,
        &common::principal(),
    )
    .expect("direct shape should succeed");

    assert_eq!(shaped.headers(), &headers);
    assert_header(shaped.headers(), "anthropic-version", "2023-06-01");
    assert_header(
        shaped.headers(),
        "anthropic-beta",
        "oauth-2025-04-20,files-api-2025-04-14",
    );
    assert_header(shaped.headers(), "content-type", "application/json");
    assert_header(shaped.headers(), "user-agent", "claude-cli/2.1.75");
    assert_header(
        shaped.headers(),
        "anthropic-dangerous-direct-browser-access",
        "true",
    );
    assert_header(shaped.headers(), "x-api-key", "sk-ant-test");
    assert_header(
        shaped.headers(),
        "authorization",
        "Bearer sk-ant-oat01-test",
    );
    assert_header(shaped.headers(), "x-foo", "bar");

    let repeated = shaped
        .headers()
        .get_all("x-repeat")
        .iter()
        .map(|value| value.to_str().expect("test header is utf8"))
        .collect::<Vec<_>>();
    assert_eq!(repeated, vec!["one", "two"]);
}

fn assert_header(headers: &HeaderMap, name: &str, expected: &str) {
    let actual = headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .expect("header should be present");
    assert_eq!(actual, expected);
}
