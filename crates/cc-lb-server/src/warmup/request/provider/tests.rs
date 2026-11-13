use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::{Body, to_bytes};
use cc_lb_aead::{EncryptedOAuthTokens, GeminiOAuthProfile, OAuthTokenBundle};
use cc_lb_clock::TestClock;
use cc_lb_storage_api::SubscriptionQuotaWindow;
use futures_util::StreamExt;
use http::{Response, StatusCode};
use hyper_util::rt::TokioExecutor;
use serde_json::Value;
use tokio::sync::{Notify, mpsc};
use url::Url;

use crate::warmup::request::dispatch_http_request_with_timeout;

use super::*;

const NOW: u64 = 1_700_000_000;

struct CapturedRequest {
    method: Method,
    uri: http::Uri,
    headers: HeaderMap,
    body: Value,
}

struct MockServer {
    base_url: Url,
    requests: mpsc::Receiver<CapturedRequest>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl MockServer {
    async fn start(
        status: StatusCode,
        headers: HeaderMap,
        response_body: &'static str,
        stream_gate: Option<Arc<Notify>>,
    ) -> Self {
        let (tx, requests) = mpsc::channel(1);
        let app = Router::new().fallback(move |request: Request<Body>| {
            let tx = tx.clone();
            let headers = headers.clone();
            let stream_gate = stream_gate.clone();
            async move {
                let (parts, body) = request.into_parts();
                let bytes = to_bytes(body, 64 * 1024).await.expect("request body");
                tx.send(CapturedRequest {
                    method: parts.method,
                    uri: parts.uri,
                    headers: parts.headers,
                    body: serde_json::from_slice(&bytes).expect("JSON request"),
                })
                .await
                .expect("capture request");
                let body = match stream_gate {
                    Some(gate) => {
                        let first = futures_util::stream::once(async {
                            Ok::<_, Infallible>(Bytes::from_static(
                                b"data: {\"type\":\"response.created\"}\n\n",
                            ))
                        });
                        let last = futures_util::stream::once(async move {
                            gate.notified().await;
                            Ok::<_, Infallible>(Bytes::from_static(response_body.as_bytes()))
                        });
                        Body::from_stream(first.chain(last))
                    }
                    None => Body::from(response_body),
                };
                let mut response = Response::new(body);
                *response.status_mut() = status;
                *response.headers_mut() = headers;
                response
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock");
        let base_url = format!("http://{}/", listener.local_addr().expect("mock address"))
            .parse()
            .expect("mock URL");
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve mock");
        });
        Self {
            base_url,
            requests,
            task,
        }
    }

    async fn request(&mut self) -> CapturedRequest {
        tokio::time::timeout(Duration::from_secs(5), self.requests.recv())
            .await
            .expect("request arrives")
            .expect("captured request")
    }
}

fn http_client() -> WarmupHttpClient {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build(connector)
}

#[tokio::test]
async fn warmup_response_body_timeout_cancels_never_ending_stream() {
    let gate = Arc::new(Notify::new());
    let mut server = MockServer::start(StatusCode::OK, HeaderMap::new(), "", Some(gate)).await;
    let request = Request::builder()
        .method(Method::POST)
        .uri(server.base_url.as_str())
        .body(Full::new(Bytes::from_static(b"{}")))
        .expect("warmup request builds");
    let attempt = dispatch_http_request_with_timeout(
        &http_client(),
        request,
        |_| Vec::new(),
        None,
        Duration::from_millis(25),
    )
    .await;
    assert!(matches!(
        attempt,
        WarmupRequestAttempt::NetworkError { error } if error == "warmup response timed out"
    ));
    let _ = server.request().await;
}

fn upstream(
    kind: UpstreamKind,
    profile: GeminiOAuthProfile,
    base_url: Url,
    aead: &AeadService,
) -> UpstreamRecord {
    let id = Uuid::new_v4();
    let bundle = OAuthTokenBundle {
        access_token: "mock-secret-access-token".to_owned(),
        refresh_token: "mock-refresh-token".to_owned(),
        expires_at_unix_secs: NOW + 3600,
        refresh_token_expires_at_unix_secs: None,
        account_id: Some("mock-account".to_owned()),
        project_id: Some("mock-project".to_owned()),
        oauth_profile: profile,
        scopes: Vec::new(),
        never_refresh: false,
    };
    UpstreamRecord {
        id,
        name: "warmup-provider-contract".to_owned(),
        kind,
        base_url: Some(base_url),
        oauth_credentials: Some(
            EncryptedOAuthTokens::encrypt(aead, &bundle, id.as_bytes())
                .expect("encrypt credentials"),
        ),
        ..Default::default()
    }
}

fn params<'a>(
    http: &'a WarmupHttpClient,
    upstream: &'a UpstreamRecord,
    aead: &'a AeadService,
    config: &'a Config,
) -> ProviderWarmupDispatchParams<'a> {
    ProviderWarmupDispatchParams {
        http,
        upstream,
        lane: WarmupLane::Primary,
        aead,
        config,
        clock: Arc::new(TestClock::new_at_secs(NOW)),
    }
}

#[tokio::test]
async fn openai_uses_codex_base_path_signer_and_drains_sse_before_parsing_quota() {
    let gate = Arc::new(Notify::new());
    let mut response_headers = HeaderMap::new();
    for (name, value) in [
        ("content-type", "text/event-stream"),
        ("x-codex-primary-used-percent", "12"),
        ("x-codex-primary-window-minutes", "300"),
        ("x-codex-primary-reset-at", "1700018000"),
        ("x-codex-secondary-used-percent", "1"),
        ("x-codex-secondary-window-minutes", "10080"),
        ("x-codex-secondary-reset-at", "1700604800"),
    ] {
        response_headers.insert(name, HeaderValue::from_static(value));
    }
    let mut server = MockServer::start(
        StatusCode::OK,
        response_headers,
        "data: {\"type\":\"response.completed\"}\n\ndata: [DONE]\n\n",
        Some(gate.clone()),
    )
    .await;
    let aead = AeadService::from_master_key([9; 32]);
    let record = upstream(
        UpstreamKind::OpenaiOauth,
        GeminiOAuthProfile::GeminiCli,
        server
            .base_url
            .join("backend-api/codex")
            .expect("Codex base URL"),
        &aead,
    );
    let mut config = Config::default();
    config.warmup.openai_model = "custom-codex-model".to_owned();
    let client = http_client();
    let attempt = dispatch_provider_warmup_attempt(params(&client, &record, &aead, &config));
    tokio::pin!(attempt);
    let request = tokio::select! {
        request = server.request() => request,
        early = &mut attempt => panic!("warmup completed before response stream: {early:?}"),
    };
    assert_eq!(request.method, Method::POST);
    assert_eq!(request.uri.path(), "/backend-api/codex/responses");
    for header in [
        "authorization",
        "chatgpt-account-id",
        "content-type",
        "accept",
    ] {
        assert!(request.headers.contains_key(header), "missing {header}");
    }
    assert_eq!(request.body["model"], "custom-codex-model");
    assert_eq!(request.body["stream"], true);
    assert_eq!(request.body["store"], false);
    assert!(
        request.body["instructions"]
            .as_str()
            .is_some_and(|s| !s.is_empty())
    );
    assert_eq!(
        request.body["input"],
        json!([{"role": "user", "content": "."}])
    );
    assert!(request.body.get("max_output_tokens").is_none());
    tokio::select! {
        early = &mut attempt => panic!("warmup did not drain SSE: {early:?}"),
        _ = tokio::time::sleep(Duration::from_millis(20)) => {}
    }
    gate.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(5), attempt)
        .await
        .expect("drained response");
    let WarmupRequestAttempt::Response {
        status,
        observations,
        error_detail,
        ..
    } = result
    else {
        panic!("expected response: {result:?}");
    };
    assert_eq!(status, StatusCode::OK);
    assert!(error_detail.is_none());
    assert_eq!(observations.len(), 2);
    assert_eq!(observations[0].window, SubscriptionQuotaWindow::FiveHour);
    assert_eq!(observations[0].utilization, Some(0.12));
    assert_eq!(observations[0].resets_at_unix_secs, Some(NOW + 18_000));
    assert_eq!(observations[1].window, SubscriptionQuotaWindow::SevenDay);
    assert_eq!(observations[1].resets_at_unix_secs, Some(NOW + 604_800));
}

#[tokio::test]
async fn gemini_profiles_reuse_proxy_envelope_host_and_headers() {
    for profile in [
        GeminiOAuthProfile::GeminiCli,
        GeminiOAuthProfile::Antigravity,
    ] {
        let mut server = MockServer::start(StatusCode::OK, HeaderMap::new(), "{}", None).await;
        let aead = AeadService::from_master_key([9; 32]);
        let is_antigravity = profile == GeminiOAuthProfile::Antigravity;
        let base_url = if is_antigravity {
            "https://cloudcode-pa.googleapis.com/v1internal"
                .parse()
                .expect("legacy host")
        } else {
            server.base_url.clone()
        };
        let record = upstream(UpstreamKind::GeminiOauth, profile, base_url, &aead);
        let mut config = Config::default();
        config.warmup.gemini_model = "custom-gemini-model".to_owned();
        config.oauth.antigravity = Some(cc_lb_config::AntigravityOAuthConfig {
            code_assist_url: server.base_url.clone(),
            user_agent: "antigravity/mock".to_owned(),
            ..Default::default()
        });
        let client = http_client();
        let result =
            dispatch_provider_warmup_attempt(params(&client, &record, &aead, &config)).await;
        let WarmupRequestAttempt::Response {
            status,
            observations,
            ..
        } = result
        else {
            panic!("expected response: {result:?}");
        };
        assert_eq!(status, StatusCode::OK);
        assert!(observations.is_empty());
        let request = server.request().await;
        assert_eq!(request.method, Method::POST);
        assert_eq!(request.uri.path(), "/v1internal:generateContent");
        assert!(request.uri.query().is_none());
        for header in ["authorization", "content-type"] {
            assert!(request.headers.contains_key(header), "missing {header}");
        }
        assert_eq!(request.body["model"], "custom-gemini-model");
        assert_eq!(request.body["project"], "mock-project");
        assert_eq!(
            request.body["request"]["contents"],
            json!([{
                "role": "user", "parts": [{"text": "."}],
            }])
        );
        assert_eq!(
            request.body["request"]["generationConfig"]["maxOutputTokens"],
            1
        );
        if is_antigravity {
            assert!(request.headers.contains_key("user-agent"));
            assert_eq!(request.body["userAgent"], "antigravity");
            assert!(request.body.get("requestType").is_none());
            assert!(
                request.body["requestId"]
                    .as_str()
                    .is_some_and(|id| id.starts_with("agent-"))
            );
        } else {
            assert!(request.body.get("userAgent").is_none());
            assert!(request.body.get("requestType").is_none());
        }
    }
}

#[tokio::test]
async fn gemini_third_party_lane_uses_third_party_model_through_antigravity_envelope() {
    let mut server = MockServer::start(StatusCode::OK, HeaderMap::new(), "{}", None).await;
    let aead = AeadService::from_master_key([9; 32]);
    let record = upstream(
        UpstreamKind::GeminiOauth,
        GeminiOAuthProfile::Antigravity,
        "https://cloudcode-pa.googleapis.com/v1internal"
            .parse()
            .expect("legacy host"),
        &aead,
    );
    let mut config = Config::default();
    config.warmup.gemini_model = "custom-gemini-model".to_owned();
    config.warmup.gemini_third_party_model = "claude-sonnet-5-5-low".to_owned();
    config.oauth.antigravity = Some(cc_lb_config::AntigravityOAuthConfig {
        code_assist_url: server.base_url.clone(),
        user_agent: "antigravity/mock".to_owned(),
        ..Default::default()
    });
    let client = http_client();
    let result = dispatch_provider_warmup_attempt(ProviderWarmupDispatchParams {
        lane: WarmupLane::ThirdParty,
        ..params(&client, &record, &aead, &config)
    })
    .await;
    let WarmupRequestAttempt::Response { status, .. } = result else {
        panic!("expected response: {result:?}");
    };
    assert_eq!(status, StatusCode::OK);
    let request = server.request().await;
    assert_eq!(request.uri.path(), "/v1internal:generateContent");
    assert_eq!(request.body["model"], "claude-sonnet-5-5-low");
    assert_eq!(request.body["userAgent"], "antigravity");
    assert!(request.body.get("requestType").is_none());
    assert!(
        request.body["requestId"]
            .as_str()
            .is_some_and(|id| id.starts_with("agent-"))
    );
    assert_eq!(
        request.body["request"]["contents"],
        json!([{"role": "user", "parts": [{"text": "."}]}])
    );
    assert_eq!(
        request.body["request"]["generationConfig"]["maxOutputTokens"],
        1
    );
}

#[tokio::test]
async fn third_party_lane_is_a_request_build_failure_for_non_gemini_kinds() {
    for kind in [UpstreamKind::OpenaiOauth, UpstreamKind::AnthropicOauth] {
        let server = MockServer::start(StatusCode::OK, HeaderMap::new(), "{}", None).await;
        let aead = AeadService::from_master_key([9; 32]);
        let record = upstream(
            kind,
            GeminiOAuthProfile::GeminiCli,
            server.base_url.clone(),
            &aead,
        );
        let config = Config::default();
        let client = http_client();
        let result = dispatch_provider_warmup_attempt(ProviderWarmupDispatchParams {
            lane: WarmupLane::ThirdParty,
            ..params(&client, &record, &aead, &config)
        })
        .await;
        let WarmupRequestAttempt::RequestBuildFailed { error } = result else {
            panic!("expected request build failure for {kind:?}: {result:?}");
        };
        assert!(error.contains("third_party"), "{error}");
    }
}

#[tokio::test]
async fn provider_http_failures_preserve_status_and_scrub_provider_error_detail() {
    for (kind, profile) in [
        (UpstreamKind::OpenaiOauth, GeminiOAuthProfile::GeminiCli),
        (UpstreamKind::GeminiOauth, GeminiOAuthProfile::GeminiCli),
        (UpstreamKind::GeminiOauth, GeminiOAuthProfile::Antigravity),
    ] {
        for status in [
            StatusCode::UNAUTHORIZED,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_REQUEST,
        ] {
            let server = MockServer::start(
                status,
                HeaderMap::new(),
                "{\"error\":{\"message\":\"model unsupported; mock-secret-access-token\"}}",
                None,
            )
            .await;
            let aead = AeadService::from_master_key([9; 32]);
            let record = upstream(kind, profile, server.base_url.clone(), &aead);
            let client = http_client();
            let config = Config::default();
            let result =
                dispatch_provider_warmup_attempt(params(&client, &record, &aead, &config)).await;
            let WarmupRequestAttempt::Response {
                status: actual,
                error_detail,
                ..
            } = result
            else {
                panic!("expected HTTP failure response: {result:?}");
            };
            assert_eq!(actual, status);
            let detail = error_detail.expect("provider message retained");
            assert!(detail.contains("model unsupported"));
            assert!(!detail.contains("mock-secret-access-token"));
        }
    }
}

#[tokio::test]
async fn anthropic_native_shape_and_quota_headers_remain_supported() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "anthropic-ratelimit-unified-5h-reset",
        HeaderValue::from_static("1700018000"),
    );
    let mut server = MockServer::start(StatusCode::OK, headers, "{}", None).await;
    let aead = AeadService::from_master_key([9; 32]);
    let record = upstream(
        UpstreamKind::AnthropicOauth,
        GeminiOAuthProfile::GeminiCli,
        server.base_url.clone(),
        &aead,
    );
    let client = http_client();
    let config = Config::default();
    let result = dispatch_provider_warmup_attempt(params(&client, &record, &aead, &config)).await;
    let WarmupRequestAttempt::Response {
        status,
        observations,
        ..
    } = result
    else {
        panic!("expected response: {result:?}");
    };
    assert_eq!(status, StatusCode::OK);
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].window, SubscriptionQuotaWindow::FiveHour);
    let request = server.request().await;
    assert_eq!(request.method, Method::POST);
    assert_eq!(request.uri.path(), "/v1/messages");
    for header in [
        "authorization",
        "content-type",
        "anthropic-version",
        "anthropic-beta",
    ] {
        assert!(request.headers.contains_key(header), "missing {header}");
    }
    assert_eq!(request.body["model"], crate::warmup::request::WARMUP_MODEL);
    assert_eq!(request.body["max_tokens"], 1);
    assert_eq!(
        request.body["messages"],
        json!([{"role": "user", "content": "."}])
    );
}

#[tokio::test]
async fn corrupt_credentials_fail_before_http_dispatch() {
    let aead = AeadService::from_master_key([9; 32]);
    let record = upstream(
        UpstreamKind::OpenaiOauth,
        GeminiOAuthProfile::GeminiCli,
        "http://127.0.0.1:1/".parse().expect("URL"),
        &aead,
    );
    let wrong_key = AeadService::from_master_key([8; 32]);
    let client = http_client();
    let config = Config::default();
    let result =
        dispatch_provider_warmup_attempt(params(&client, &record, &wrong_key, &config)).await;
    assert!(matches!(
        result,
        WarmupRequestAttempt::CredentialDecryptFailed { .. }
    ));
}

struct ProviderShapePlugin {
    native: Arc<dyn UpstreamDialect>,
    kind: UpstreamKind,
}

impl UpstreamDialect for ProviderShapePlugin {
    fn shape(
        &self,
        context: &DialectShapeContext,
        upstream: &Upstream,
        principal: &Principal,
        builder: &mut cc_lb_upstream::ShapedRequestBuilder,
    ) -> Result<cc_lb_upstream::ShapedRequest, cc_lb_upstream::DialectError> {
        let mut body: Value = sonic_rs::from_slice(&context.body_bytes).unwrap();
        match self.kind {
            UpstreamKind::OpenaiOauth => {
                assert_eq!(context.path, "/v1/responses");
                body["input"][0]["content"] = json!("plugin warmup");
            }
            UpstreamKind::GeminiOauth => {
                assert!(context.path.ends_with(":generateContent"));
                assert!(
                    body.get("request").is_none(),
                    "plugin must precede Code Assist wrap"
                );
                assert!(body.get("project").is_none());
                body["contents"][0]["parts"][0]["text"] = json!("plugin warmup");
            }
            _ => unreachable!(),
        }
        let mut shaped = self.native.shape(context, upstream, principal, builder)?;
        shaped.set_body(Bytes::from(serde_json::to_vec(&body).unwrap()));
        Ok(shaped)
    }
}

#[tokio::test]
async fn provider_plugin_output_is_dispatched_before_oauth_signing_and_gemini_wrap() {
    for (kind, profile) in [
        (UpstreamKind::OpenaiOauth, GeminiOAuthProfile::GeminiCli),
        (UpstreamKind::GeminiOauth, GeminiOAuthProfile::GeminiCli),
        (UpstreamKind::GeminiOauth, GeminiOAuthProfile::Antigravity),
    ] {
        let mut server = MockServer::start(StatusCode::OK, HeaderMap::new(), "{}", None).await;
        let aead = AeadService::from_master_key([9; 32]);
        let record = upstream(kind, profile, server.base_url.clone(), &aead);
        let bundle = record
            .oauth_credentials
            .as_ref()
            .unwrap()
            .decrypt(&aead, record.id.as_bytes())
            .unwrap();
        let client = http_client();
        let config = Config::default();
        let dialect = ProviderShapePlugin {
            native: cc_lb_engine::lifecycle::native_route_dialect(&record),
            kind,
        };
        let (request, signer) = build_provider_warmup_request(
            &params(&client, &record, &aead, &config),
            &bundle,
            &dialect,
        )
        .await
        .unwrap();
        let result =
            dispatch_http_request(&client, request, |_| Vec::new(), Some(signer.as_ref())).await;
        assert!(matches!(
            result,
            WarmupRequestAttempt::Response {
                status: StatusCode::OK,
                ..
            }
        ));
        let request = server.request().await;
        assert!(request.headers.contains_key("authorization"));
        match kind {
            UpstreamKind::OpenaiOauth => {
                assert_eq!(request.body["input"][0]["content"], "plugin warmup")
            }
            UpstreamKind::GeminiOauth => {
                assert_eq!(request.uri.path(), "/v1internal:generateContent");
                assert_eq!(request.body["project"], "mock-project");
                assert_eq!(
                    request.body["request"]["contents"][0]["parts"][0]["text"],
                    "plugin warmup"
                );
            }
            _ => unreachable!(),
        }
    }
}
