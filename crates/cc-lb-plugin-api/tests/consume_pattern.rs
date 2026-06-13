use std::path::PathBuf;
use std::process::Command;
use std::{env, fs};

use bytes::Bytes;
use cc_lb_plugin_api::{
    DialectError, Principal, PrincipalKind, RequestContext, ShapedRequest, ShapedRequestBuilder,
    SignedRequest, Signer, SignerError, SigningCapability, Upstream, UpstreamDialect,
    shape_request, sign_request,
};
use http::{HeaderMap, Method};

struct DummyDialect;
struct DummySigner;

impl UpstreamDialect for DummyDialect {
    fn shape(
        &self,
        _ctx: &RequestContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        Ok(builder.shaped_request(
            "https://api.anthropic.com/v1/messages".parse().unwrap(),
            Method::POST,
            HeaderMap::new(),
            Bytes::from_static(b"{}"),
        ))
    }

    fn normalize_error(&self, _status: http::StatusCode, _body: &Bytes) -> Option<Bytes> {
        None
    }
}

#[async_trait::async_trait]
impl Signer for DummySigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        shaped.headers_mut().insert(
            "x-api-key",
            http::HeaderValue::from_static("redacted-test-key"),
        );
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(
        &self,
        _err: &cc_lb_plugin_api::UpstreamError,
    ) -> cc_lb_plugin_api::RetryDecision {
        cc_lb_plugin_api::RetryDecision::Fail
    }
}

#[test]
fn signer_seals_by_consuming_shaped_request() {
    let ctx = RequestContext {
        request_id: "req-1".to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from_static(b"{}"),
        cache_breakpoints: Vec::new(),
        canonical_model_id: String::new(),
    };
    let principal = Principal {
        id: "alice".to_owned(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    };
    let shaped = shape_request(
        &DummyDialect,
        &ctx,
        &Upstream::AnthropicDirect { base_url: None },
        &principal,
    )
    .unwrap();

    let signer = DummySigner;
    let future = sign_request(&signer, shaped);
    drop(future);

    let shaped = shape_request(
        &DummyDialect,
        &ctx,
        &Upstream::AnthropicDirect { base_url: None },
        &principal,
    )
    .unwrap();
    assert_eq!(shaped.method(), Method::POST);
    assert_eq!(shaped.body(), &Bytes::from_static(b"{}"));
}

#[test]
fn controlled_builder_cannot_be_fabricated_by_normal_callers() {
    struct DirectDialect;

    impl UpstreamDialect for DirectDialect {
        fn shape(
            &self,
            _ctx: &RequestContext,
            _upstream: &Upstream,
            _principal: &Principal,
            builder: &mut ShapedRequestBuilder,
        ) -> Result<ShapedRequest, DialectError> {
            Ok(builder.shaped_request(
                "https://api.anthropic.com/v1/messages".parse().unwrap(),
                Method::POST,
                HeaderMap::new(),
                Bytes::new(),
            ))
        }

        fn normalize_error(&self, _status: http::StatusCode, _body: &Bytes) -> Option<Bytes> {
            None
        }
    }

    let ctx = RequestContext {
        request_id: "req-2".to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::new(),
        cache_breakpoints: Vec::new(),
        canonical_model_id: String::new(),
    };
    let principal = Principal {
        id: "alice".to_owned(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    };

    let shaped = shape_request(
        &DirectDialect,
        &ctx,
        &Upstream::AnthropicDirect { base_url: None },
        &principal,
    )
    .unwrap();
    assert_eq!(
        shaped.url().as_str(),
        "https://api.anthropic.com/v1/messages"
    );
}

#[test]
fn external_struct_literals_are_rejected() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let target_dir = env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| manifest_dir.join("../..").join("target"));
    let test_dir = env::temp_dir().join(format!("cc-lb-construct-test-{}", std::process::id()));
    let src_dir = test_dir.join("src");
    if test_dir.exists() {
        fs::remove_dir_all(&test_dir).unwrap();
    }
    fs::create_dir_all(&src_dir).unwrap();

    fs::write(
        test_dir.join("Cargo.toml"),
        format!(
            r#"[package]
name = "cc-lb-construct-test"
version = "0.0.0"
edition = "2021"

[dependencies]
cc-lb-plugin-api = {{ path = "{}" }}
"#,
            manifest_dir.display()
        ),
    )
    .unwrap();
    fs::write(
        src_dir.join("main.rs"),
        r#"use cc_lb_plugin_api::{ShapedRequest, ShapedRequestBuilder, SignedRequest, SigningCapability};

fn main() {
    let _ = ShapedRequest {
        url: unreachable!(),
        method: unreachable!(),
        headers: unreachable!(),
        body: unreachable!(),
    };

    let _ = SignedRequest {
        url: unreachable!(),
        method: unreachable!(),
        headers: unreachable!(),
        body: unreachable!(),
    };

    let _ = ShapedRequest::new;

    let mut builder = ShapedRequestBuilder {};

    let _ = builder.shaped_request(
        unreachable!(),
        unreachable!(),
        unreachable!(),
        unreachable!(),
    );

    let mut capability = SigningCapability {};

    let _ = SignedRequest::from_shaped(unreachable!(), &mut capability);
}
"#,
    )
    .unwrap();

    let output = Command::new("cargo")
        .arg("check")
        .arg("--quiet")
        .env("CARGO_TARGET_DIR", target_dir)
        .current_dir(&test_dir)
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "external struct literal unexpectedly compiled"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("private") || stderr.contains("E0451") || stderr.contains("E0639"),
        "unexpected compiler error: {stderr}"
    );
}
