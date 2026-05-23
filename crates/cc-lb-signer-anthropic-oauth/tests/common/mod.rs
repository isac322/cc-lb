#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_plugin_api::{
    Principal, PrincipalKind, RequestContext, ShapedRequest, ShapedRequestBuilder, Upstream,
    UpstreamDialect, shape_request,
};
use cc_lb_signer_anthropic_oauth::{
    AnthropicOAuthSigner, OAuthHttpClient, OAuthHttpError, OAuthTokenRequest, OAuthTokenResponse,
};
use cc_lb_storage_redb::{OAuthCredentials, RedbStorage};
use http::header::USER_AGENT;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use oauth2::ClientId;
use secrecy::ExposeSecret;
use tempfile::TempDir;
use url::Url;

#[derive(Debug)]
pub struct FakeOAuthClient {
    pub calls: AtomicU32,
    responses: Mutex<VecDeque<OAuthTokenResponse>>,
    bodies: Mutex<Vec<String>>,
}

impl FakeOAuthClient {
    pub fn new(responses: Vec<OAuthTokenResponse>) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicU32::new(0),
            responses: Mutex::new(VecDeque::from(responses)),
            bodies: Mutex::new(Vec::new()),
        })
    }

    pub fn call_count(&self) -> u32 {
        self.calls.load(Ordering::Relaxed)
    }

    pub fn bodies(&self) -> Vec<String> {
        self.bodies.lock().expect("bodies lock").clone()
    }
}

#[async_trait]
impl OAuthHttpClient for FakeOAuthClient {
    async fn post_token(
        &self,
        request: OAuthTokenRequest,
    ) -> Result<OAuthTokenResponse, OAuthHttpError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.bodies
            .lock()
            .expect("bodies lock")
            .push(request.form_body.expose_secret().to_owned());
        self.responses
            .lock()
            .expect("responses lock")
            .pop_front()
            .ok_or_else(|| OAuthHttpError::Request {
                reason: "fake response queue empty".to_owned(),
            })
    }
}

pub struct TestStorage {
    pub _dir: TempDir,
    pub storage: Arc<RedbStorage>,
}

pub fn storage() -> TestStorage {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("storage.redb");
    let storage = Arc::new(RedbStorage::open(&path).expect("storage open"));
    TestStorage { _dir: dir, storage }
}

pub fn signer(storage: Arc<RedbStorage>, http: Arc<dyn OAuthHttpClient>) -> AnthropicOAuthSigner {
    AnthropicOAuthSigner::with_http(
        "alice",
        "anthropic_oauth",
        storage,
        Url::parse("https://platform.claude.com/v1/oauth/token").expect("token url"),
        ClientId::new("client-test".to_owned()),
        http,
    )
}

pub fn creds(access_token: &str, refresh_token: &str, expires_at: u64) -> OAuthCredentials {
    OAuthCredentials {
        access_token: access_token.to_owned(),
        refresh_token: refresh_token.to_owned(),
        expires_at,
        scopes: vec!["messages".to_owned()],
    }
}

pub fn success_response(
    access_token: &str,
    refresh_token: Option<&str>,
    expires_in: u64,
) -> OAuthTokenResponse {
    let mut fields = serde_json::Map::new();
    fields.insert(
        "access_token".to_owned(),
        serde_json::Value::String(access_token.to_owned()),
    );
    if let Some(refresh_token) = refresh_token {
        fields.insert(
            "refresh_token".to_owned(),
            serde_json::Value::String(refresh_token.to_owned()),
        );
    }
    fields.insert(
        "expires_in".to_owned(),
        serde_json::Value::Number(serde_json::Number::from(expires_in)),
    );
    fields.insert(
        "scope".to_owned(),
        serde_json::Value::String("messages files".to_owned()),
    );
    OAuthTokenResponse {
        status: StatusCode::OK,
        body: Bytes::from(serde_json::Value::Object(fields).to_string()),
    }
}

pub fn failure_response() -> OAuthTokenResponse {
    OAuthTokenResponse {
        status: StatusCode::BAD_REQUEST,
        body: Bytes::from_static(br#"{"error":"invalid_grant"}"#),
    }
}

pub fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_secs()
}

pub fn shaped_request() -> ShapedRequest {
    let ctx = RequestContext {
        request_id: "req-1".to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from_static(b"{}"),
    };
    let principal = Principal {
        id: "alice".to_owned(),
        kind: PrincipalKind::OAuthSubject,
        claims: serde_json::Map::new(),
    };
    shape_request(&DirectDialect, &ctx, &Upstream::AnthropicDirect, &principal)
        .expect("shape request")
}

struct DirectDialect;

impl UpstreamDialect for DirectDialect {
    fn shape(
        &self,
        _ctx: &RequestContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, cc_lb_plugin_api::DialectError> {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static("test-agent"));
        Ok(builder.shaped_request(
            Url::parse("https://api.anthropic.com/v1/messages").expect("request url"),
            Method::POST,
            headers,
            Bytes::from_static(br#"{"model":"claude-test"}"#),
        ))
    }

    fn normalize_error(&self, _status: StatusCode, _body: &Bytes) -> Option<Bytes> {
        None
    }
}

pub fn unauthorized_error() -> cc_lb_plugin_api::UpstreamError {
    cc_lb_plugin_api::UpstreamError::Unauthorized {
        status: StatusCode::UNAUTHORIZED,
        body: None,
    }
}
