mod common;

use bytes::Bytes;
use cc_lb_dialect_anthropic::CustomAnthropicSpecDialect;
use cc_lb_plugin_api::{shape_request, Upstream};
use http::Method;

#[test]
fn custom_base_path_and_downstream_path_are_joined_without_double_slashes() {
    let ctx = common::request_context(
        Method::POST,
        "/v1/messages",
        None,
        Bytes::from_static(b"{}"),
        common::anthropic_headers(),
    );
    let upstream = Upstream::CustomAnthropicSpec {
        base_url: "https://gw.example/api".parse().expect("valid test url"),
    };

    let shaped = shape_request(
        &CustomAnthropicSpecDialect,
        &ctx,
        &upstream,
        &common::principal(),
    )
    .expect("custom shape should succeed");

    println!("custom_composed_url={}", shaped.url());
    assert_eq!(shaped.url().as_str(), "https://gw.example/api/v1/messages");
}

#[test]
fn custom_url_preserves_base_prefix_and_downstream_query() {
    let ctx = common::request_context(
        Method::POST,
        "v1/messages",
        Some("stream=true&request_id=req_123"),
        Bytes::from_static(b"{}"),
        common::anthropic_headers(),
    );
    let upstream = Upstream::CustomAnthropicSpec {
        base_url: "https://gw.example/api/".parse().expect("valid test url"),
    };

    let shaped = shape_request(
        &CustomAnthropicSpecDialect,
        &ctx,
        &upstream,
        &common::principal(),
    )
    .expect("custom shape should succeed");

    println!("custom_query_url={}", shaped.url());
    assert_eq!(
        shaped.url().as_str(),
        "https://gw.example/api/v1/messages?stream=true&request_id=req_123"
    );
}

#[test]
fn custom_dialect_rejects_wrong_upstream_variant() {
    let ctx = common::request_context(
        Method::POST,
        "/v1/messages",
        None,
        Bytes::from_static(b"{}"),
        common::anthropic_headers(),
    );

    let err = shape_request(
        &CustomAnthropicSpecDialect,
        &ctx,
        &Upstream::AnthropicDirect,
        &common::principal(),
    )
    .expect_err("wrong upstream must be rejected");

    assert!(err.to_string().contains("CustomAnthropicSpec"));
}
