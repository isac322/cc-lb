#![allow(dead_code)]

use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;
use cc_lb_plugin_api::{
    shape_request, Principal, PrincipalKind, RequestContext, ShapedRequest, ShapedRequestBuilder,
    Upstream, UpstreamDialect,
};
use cc_lb_signer_aws::{AwsSigV4Signer, Clock, StaticCredentialsProvider};
use http::{HeaderMap, Method, StatusCode};
use ring::digest::{digest, SHA256};
use url::Url;

pub const ACCESS_KEY: &str = "AKIATEST";
pub const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
pub const SIGNING_EPOCH_SECS: u64 = 1_577_836_800;

#[derive(Debug)]
pub struct FixedClock {
    now: SystemTime,
}

impl FixedClock {
    pub fn new(now: SystemTime) -> Self {
        Self { now }
    }
}

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.now
    }
}

pub fn signer() -> AwsSigV4Signer {
    signer_with_session(None)
}

pub fn signer_with_session(session_token: Option<String>) -> AwsSigV4Signer {
    AwsSigV4Signer::with_clock(
        "us-east-1",
        "bedrock",
        Arc::new(StaticCredentialsProvider::new(
            ACCESS_KEY,
            SECRET_KEY,
            session_token,
        )),
        Arc::new(FixedClock::new(
            SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(SIGNING_EPOCH_SECS),
        )),
    )
}

pub fn shaped_request(url: &str, headers: HeaderMap, body: Bytes) -> ShapedRequest {
    let ctx = RequestContext {
        request_id: "req-aws-sigv4".to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: body.clone(),
    };
    let principal = Principal {
        id: "alice".to_owned(),
        kind: PrincipalKind::WorkloadIdentity,
        claims: serde_json::Map::new(),
    };
    let dialect = TestDialect {
        url: Url::parse(url).expect("test URL parses"),
        headers,
        body,
    };
    shape_request(
        &dialect,
        &ctx,
        &Upstream::BedrockRuntime {
            region: "us-east-1".to_owned(),
        },
        &principal,
    )
    .expect("shape request")
}

pub fn auth_header(headers: &HeaderMap) -> &str {
    headers
        .get(http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .expect("authorization header")
}

pub fn signature_component(authorization: &str) -> &str {
    authorization
        .split(',')
        .map(str::trim)
        .find_map(|part| part.strip_prefix("Signature="))
        .expect("signature component")
}

pub fn hex_sha256(body: &[u8]) -> String {
    let digest = digest(&SHA256, body);
    let mut output = String::with_capacity(digest.as_ref().len() * 2);
    for byte in digest.as_ref() {
        use std::fmt::Write;
        write!(&mut output, "{byte:02x}").expect("write hex");
    }
    output
}

pub fn request_uri(url: &Url) -> String {
    let mut uri = url.path().to_owned();
    if let Some(query) = url.query() {
        uri.push('?');
        uri.push_str(query);
    }
    uri
}

struct TestDialect {
    url: Url,
    headers: HeaderMap,
    body: Bytes,
}

impl UpstreamDialect for TestDialect {
    fn shape(
        &self,
        _ctx: &RequestContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, cc_lb_plugin_api::DialectError> {
        Ok(builder.shaped_request(
            self.url.clone(),
            Method::POST,
            self.headers.clone(),
            self.body.clone(),
        ))
    }

    fn normalize_error(&self, _status: StatusCode, _body: &Bytes) -> Option<Bytes> {
        None
    }
}
