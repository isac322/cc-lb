use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::{Bytes, to_bytes};
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use cc_lb_aead::OAuthTokenBundle;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens};
use cc_lb_clock::{Clock, ClockHandle};
use cc_lb_config::AntigravityOAuthConfig;
use cc_lb_control::DynamicViewHolder;
use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_engine::{Lifecycle, LifecycleConfig};
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_server::dynamic_view_builder::build_dynamic_view;
use cc_lb_signer_anthropic_oauth::{LazyRefreshError, LazyRefreshHandle};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{PrincipalCreate, PrincipalKind};
use cc_lb_storage_api::{UpstreamCreate, UpstreamStatusUpdate, UpstreamStore};
use cc_lb_storage_sqlite::SqliteStorage as Storage;
use http::header::AUTHORIZATION;
use http::{Method, Request, StatusCode};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use url::Url;
use uuid::Uuid;

use super::*;

#[derive(Clone, Debug)]
struct ProviderRequest {
    authorization: Option<String>,
    account_id: Option<String>,
    body: Vec<u8>,
}

#[derive(Clone, Default)]
struct ProviderJournalState {
    requests: Arc<Mutex<Vec<ProviderRequest>>>,
    statuses: Arc<Mutex<VecDeque<StatusCode>>>,
}

struct ProviderJournal {
    base_url: String,
    state: ProviderJournalState,
    task: JoinHandle<Result<(), std::io::Error>>,
}

impl ProviderJournal {
    async fn spawn(statuses: impl IntoIterator<Item = StatusCode>) -> Self {
        let state = ProviderJournalState {
            requests: Arc::new(Mutex::new(Vec::new())),
            statuses: Arc::new(Mutex::new(statuses.into_iter().collect())),
        };
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind provider journal");
        let address = listener.local_addr().expect("provider journal address");
        let router = Router::new()
            .fallback(provider_handler)
            .with_state(state.clone());
        let task = tokio::spawn(async move { axum::serve(listener, router).await });
        Self {
            base_url: format!("http://{address}"),
            state,
            task,
        }
    }

    fn requests(&self) -> Vec<ProviderRequest> {
        self.state
            .requests
            .lock()
            .expect("provider journal lock")
            .clone()
    }
}

impl Drop for ProviderJournal {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn provider_handler(
    State(state): State<ProviderJournalState>,
    request: axum::extract::Request,
) -> Response {
    let path = request.uri().path().to_owned();
    let authorization = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let account_id = request
        .headers()
        .get("ChatGPT-Account-ID")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = to_bytes(request.into_body(), 2 * 1024 * 1024)
        .await
        .expect("provider request body")
        .to_vec();
    state
        .requests
        .lock()
        .expect("provider journal lock")
        .push(ProviderRequest {
            authorization,
            account_id,
            body,
        });

    let status = state
        .statuses
        .lock()
        .expect("provider status lock")
        .pop_front()
        .unwrap_or(StatusCode::OK);
    let payload = if status == StatusCode::UNAUTHORIZED {
        json!({
            "error": {
                "type": "invalid_token",
                "message": "expired access token"
            }
        })
    } else if path == "/v1/responses" {
        json!({
            "id": "resp_oauth_refresh",
            "object": "response",
            "created_at": 1,
            "status": "completed",
            "model": "gpt-5-codex",
            "output": [{
                "id": "msg_oauth_refresh",
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "text": "ok",
                    "annotations": []
                }]
            }],
            "usage": {
                "input_tokens": 1,
                "output_tokens": 1,
                "total_tokens": 2
            }
        })
    } else if path == "/v1/messages" {
        json!({
            "id": "msg_oauth_refresh",
            "type": "message",
            "role": "assistant",
            "content": [{"type": "text", "text": "ok"}],
            "model": "claude-sonnet-4-20250514",
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 1, "output_tokens": 1}
        })
    } else {
        json!({
            "candidates": [{"content": {"parts": [{"text": "ok"}], "role": "model"}, "finishReason": "STOP"}],
            "usageMetadata": {"promptTokenCount": 1, "candidatesTokenCount": 1, "totalTokenCount": 2},
            "modelVersion": "gemini-2.5-flash"
        })
    };
    (status, Json(payload)).into_response()
}

#[derive(Clone)]
struct StatefulRefreshHandle {
    storage: Arc<Storage>,
    aead: Arc<AeadService>,
    replacement_access_token: String,
    calls: Arc<AtomicUsize>,
    clock: ClockHandle,
    terminal: bool,
}

impl StatefulRefreshHandle {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    async fn replace_tokens(&self, upstream_id: Uuid) -> Result<(), LazyRefreshError> {
        let record = UpstreamStore::get_by_id(self.storage.as_ref(), upstream_id)
            .await
            .map_err(|error| LazyRefreshError::Failed {
                reason: error.to_string(),
            })?
            .ok_or_else(|| LazyRefreshError::Failed {
                reason: "upstream missing".to_owned(),
            })?;
        let encrypted =
            record
                .oauth_credentials
                .as_ref()
                .ok_or_else(|| LazyRefreshError::Failed {
                    reason: "oauth credentials missing".to_owned(),
                })?;
        let mut tokens = encrypted
            .decrypt(self.aead.as_ref(), upstream_id.as_bytes())
            .map_err(|error| LazyRefreshError::Failed {
                reason: error.to_string(),
            })?;
        tokens.access_token = self.replacement_access_token.clone();
        tokens.expires_at_unix_secs = now_secs(self.clock.as_ref()) + 3600;
        let encrypted = encrypted_tokens(self.aead.as_ref(), upstream_id, &tokens);
        UpstreamStore::store_oauth_tokens(
            self.storage.as_ref(),
            upstream_id,
            record.revision,
            encrypted,
            tokens.never_refresh,
        )
        .await
        .map_err(|error| LazyRefreshError::Failed {
            reason: error.to_string(),
        })?;
        Ok(())
    }
}

#[async_trait]
impl LazyRefreshHandle for StatefulRefreshHandle {
    async fn refresh_one(&self, upstream_id: Uuid) -> Result<(), LazyRefreshError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.terminal {
            UpstreamStore::set_status(
                self.storage.as_ref(),
                upstream_id,
                UpstreamStatusUpdate {
                    last_apply_error: Some(Some("status_401".to_owned())),
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| LazyRefreshError::Failed {
                reason: error.to_string(),
            })?;
            return Err(LazyRefreshError::Failed {
                reason: "provider rejected refresh".to_owned(),
            });
        }
        self.replace_tokens(upstream_id).await
    }
}

fn encrypted_tokens(
    aead: &AeadService,
    upstream_id: Uuid,
    tokens: &OAuthTokenBundle,
) -> EncryptedOAuthTokens {
    EncryptedOAuthTokens::encrypt(aead, tokens, upstream_id.as_bytes()).expect("encrypt tokens")
}

fn subscription_bundle(
    profile: cc_lb_aead::GeminiOAuthProfile,
    access_token: &str,
    expires_at_unix_secs: u64,
    never_refresh: bool,
) -> OAuthTokenBundle {
    OAuthTokenBundle {
        access_token: access_token.to_owned(),
        refresh_token: "refresh-token".to_owned(),
        expires_at_unix_secs,
        refresh_token_expires_at_unix_secs: None,
        account_id: Some("account-preserved".to_owned()),
        project_id: Some("project-preserved".to_owned()),
        oauth_profile: profile,
        scopes: vec!["openid".to_owned()],
        never_refresh,
    }
}

async fn create_subscription_upstream(
    fixture: &Fixture,
    journal: &ProviderJournal,
    kind: UpstreamKind,
    bundle: OAuthTokenBundle,
) -> Uuid {
    let record = fixture
        .storage
        .create(UpstreamCreate {
            id: Uuid::new_v4(),
            name: "subscription-oauth".to_owned(),
            kind,
            base_url: Some(Url::parse(&journal.base_url).expect("journal URL")),
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        })
        .await
        .expect("subscription upstream created");
    let encrypted = encrypted_tokens(fixture.aead.as_ref(), record.id, &bundle);
    UpstreamStore::store_oauth_tokens(
        fixture.storage.as_ref(),
        record.id,
        record.revision,
        encrypted,
        bundle.never_refresh,
    )
    .await
    .expect("subscription tokens stored");
    record.id
}

async fn make_lifecycle(
    fixture: &Fixture,
    key_store: Arc<KeyStore>,
    lazy: Arc<dyn LazyRefreshHandle>,
) -> Lifecycle {
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let antigravity = AntigravityOAuthConfig {
        user_agent: "antigravity/oauth-refresh-test".to_owned(),
        ..Default::default()
    };
    let view = build_dynamic_view(
        fixture.stores.as_ref(),
        None,
        Some(&antigravity),
        fixture.aead.clone(),
        Some(lazy),
        0,
        &runtime,
        fixture._dir.path(),
        Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
        30,
        Arc::new(cc_lb_control::NoopPromptCacheObservationSink),
        1800,
        fixture.clock.clone(),
    )
    .await
    .expect("dynamic view builds");
    Lifecycle::new_with_dynamic_view(
        Arc::new(BuiltinAuthn::new(key_store, fixture.clock.clone())),
        Arc::new(DynamicViewHolder::new(view)),
        cc_lb_engine::make_default_dispatcher(50),
        LifecycleConfig::default(),
        fixture.clock.clone(),
    )
}

async fn make_key(fixture: &Fixture, upstream_id: Uuid) -> (Arc<KeyStore>, String) {
    cc_lb_storage_api::PrincipalStore::create(
        fixture.storage.as_ref(),
        PrincipalCreate {
            name: "subscription-principal".to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: vec!["*".to_owned()],
            allowed_upstreams: vec![upstream_id],
            default_limits: Vec::new(),
            cache_keepalive: None,
            openai_oauth_compat: None,
        },
        now_secs(fixture.clock.as_ref()),
    )
    .await
    .expect("principal created");
    let key_store = Arc::new(KeyStore::new(fixture.storage.clone()));
    let (_, secret) = key_store
        .create(
            "subscription-principal",
            CreateParams {
                label: "subscription-oauth-dispatch".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: Vec::new(),
            },
        )
        .await
        .expect("managed key created");
    (key_store, secret.expose().to_owned())
}

fn request_for(kind: UpstreamKind, key: &str) -> Request<Bytes> {
    let (path, provider, body): (&str, cc_lb_domain::Provider, &[u8]) = match kind {
        UpstreamKind::OpenaiOauth => (
            "/v1/responses",
            cc_lb_domain::Provider::Openai,
            br#"{"model":"gpt-5-codex","input":"hi","stream":false}"#,
        ),
        UpstreamKind::GeminiOauth => (
            "/v1beta/models/gemini-2.5-flash:generateContent",
            cc_lb_domain::Provider::Gemini,
            br#"{"contents":[{"parts":[{"text":"hi"}]}]}"#,
        ),
        UpstreamKind::AnthropicOauth => (
            "/v1/messages",
            cc_lb_domain::Provider::Anthropic,
            br#"{"model":"claude-sonnet-4-20250514","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#,
        ),
        _ => panic!("subscription test only supports OAuth upstreams"),
    };
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(path)
        .header("x-api-key", key)
        .header("content-type", "application/json")
        .body(Bytes::from_static(body))
        .expect("request builds");
    request
        .extensions_mut()
        .insert(cc_lb_engine::request_context::ProviderScope(provider));
    request
}

async fn dispatch(lifecycle: &Lifecycle, request: Request<Bytes>) -> (StatusCode, Bytes) {
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle response");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    (status, body)
}

fn now_secs(clock: &dyn Clock) -> u64 {
    clock
        .now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[tokio::test]
async fn antigravity_dispatch_qualifies_tiered_model_and_omits_request_type() {
    for profile in [
        cc_lb_aead::GeminiOAuthProfile::GeminiCli,
        cc_lb_aead::GeminiOAuthProfile::Antigravity,
    ] {
        let fixture = Fixture::new_without_scheduler(AppConfig::default()).await;
        let journal = ProviderJournal::spawn([StatusCode::OK]).await;
        let upstream_id = create_subscription_upstream(
            &fixture,
            &journal,
            UpstreamKind::GeminiOauth,
            subscription_bundle(
                profile,
                "live-tiered-model-token",
                now_secs(fixture.clock.as_ref()) + 3600,
                false,
            ),
        )
        .await;
        cc_lb_storage_api::PrincipalStore::create(
            fixture.storage.as_ref(),
            PrincipalCreate {
                name: "tiered-model-principal".to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: vec!["gemini-3.8-flash".to_owned()],
                allowed_upstreams: vec![upstream_id],
                default_limits: Vec::new(),
                cache_keepalive: None,
                openai_oauth_compat: None,
            },
            now_secs(fixture.clock.as_ref()),
        )
        .await
        .expect("principal created");
        let key_store = Arc::new(KeyStore::new(fixture.storage.clone()));
        let (_, key) = key_store
            .create(
                "tiered-model-principal",
                CreateParams {
                    label: "tiered-model-dispatch".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: Vec::new(),
                },
            )
            .await
            .expect("managed key created");
        let handle = Arc::new(StatefulRefreshHandle {
            storage: fixture.storage.clone(),
            aead: fixture.aead.clone(),
            replacement_access_token: "must-not-be-used".to_owned(),
            calls: Arc::new(AtomicUsize::new(0)),
            clock: fixture.clock.clone(),
            terminal: false,
        });
        let lifecycle = make_lifecycle(&fixture, key_store, handle).await;
        let input = json!({
            "contents": [{"role": "user", "parts": [{"text": "Look up the weather."}]}],
            "tools": [{
                "functionDeclarations": [{
                    "name": "get_weather",
                    "description": "Get the weather for a city.",
                    "parameters": {
                        "type": "OBJECT",
                        "properties": {"city": {"type": "STRING"}},
                        "required": ["city"]
                    }
                }]
            }],
            "generationConfig": {"thinkingConfig": {"thinkingLevel": "XHIGH"}}
        });
        let mut request = Request::builder()
            .method(Method::POST)
            .uri("/v1beta/models/gemini-3.8-flash:generateContent")
            .header("x-api-key", key.expose())
            .header("content-type", "application/json")
            .body(Bytes::from(
                serde_json::to_vec(&input).expect("request JSON"),
            ))
            .expect("request builds");
        request
            .extensions_mut()
            .insert(cc_lb_engine::request_context::ProviderScope(
                cc_lb_domain::Provider::Gemini,
            ));
        let (status, response_body) = dispatch(&lifecycle, request).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&response_body)
        );
        let requests = journal.requests();
        assert_eq!(requests.len(), 1);
        let body: Value =
            serde_json::from_slice(&requests[0].body).expect("Gemini request envelope");
        assert_eq!(body["request"]["tools"], input["tools"]);
        assert_eq!(
            body["request"]["generationConfig"]["thinkingConfig"],
            input["generationConfig"]["thinkingConfig"]
        );
        match profile {
            cc_lb_aead::GeminiOAuthProfile::Antigravity => {
                assert_eq!(body["model"], "gemini-3.8-flash-xhigh");
                assert!(body.get("requestType").is_none());
                assert_eq!(body["userAgent"], "antigravity");
                assert!(
                    body["requestId"]
                        .as_str()
                        .is_some_and(|id| id.starts_with("agent-"))
                );
            }
            cc_lb_aead::GeminiOAuthProfile::GeminiCli => {
                assert_eq!(body["model"], "gemini-3.8-flash");
                assert!(body.get("userAgent").is_none());
                assert!(body.get("requestType").is_none());
                assert!(body.get("requestId").is_none());
            }
        }
    }
}

#[tokio::test]
async fn expired_openai_gemini_cli_and_antigravity_dispatch_refreshes_without_old_bearer() {
    for (kind, profile) in [
        (
            UpstreamKind::OpenaiOauth,
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
        ),
        (
            UpstreamKind::GeminiOauth,
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
        ),
        (
            UpstreamKind::GeminiOauth,
            cc_lb_aead::GeminiOAuthProfile::Antigravity,
        ),
    ] {
        let fixture = Fixture::new_without_scheduler(AppConfig::default()).await;
        let journal = ProviderJournal::spawn([StatusCode::OK]).await;
        let bundle = subscription_bundle(
            profile,
            "old-expired-token",
            now_secs(fixture.clock.as_ref()).saturating_sub(1),
            false,
        );
        let upstream_id = create_subscription_upstream(&fixture, &journal, kind, bundle).await;
        let (key_store, key) = make_key(&fixture, upstream_id).await;
        let handle = Arc::new(StatefulRefreshHandle {
            storage: fixture.storage.clone(),
            aead: fixture.aead.clone(),
            replacement_access_token: "new-expired-token".to_owned(),
            calls: Arc::new(AtomicUsize::new(0)),
            clock: fixture.clock.clone(),
            terminal: false,
        });
        let lifecycle = make_lifecycle(&fixture, key_store, handle.clone()).await;
        let (status, body) = dispatch(&lifecycle, request_for(kind, &key)).await;
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        assert_eq!(handle.calls(), 1);
        let requests = journal.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].authorization.as_deref(),
            Some("Bearer new-expired-token")
        );
        let record = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await
            .expect("upstream read")
            .expect("upstream exists");
        let stored = record
            .oauth_credentials
            .expect("stored credentials")
            .decrypt(fixture.aead.as_ref(), upstream_id.as_bytes())
            .expect("decrypt credentials");
        assert_eq!(stored.account_id.as_deref(), Some("account-preserved"));
        assert_eq!(stored.project_id.as_deref(), Some("project-preserved"));
    }
}

#[tokio::test]
async fn fresh_401_refreshes_once_then_retries_with_preserved_account_and_project() {
    for (kind, profile) in [
        (
            UpstreamKind::OpenaiOauth,
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
        ),
        (
            UpstreamKind::GeminiOauth,
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
        ),
        (
            UpstreamKind::GeminiOauth,
            cc_lb_aead::GeminiOAuthProfile::Antigravity,
        ),
    ] {
        let fixture = Fixture::new_without_scheduler(AppConfig::default()).await;
        let journal = ProviderJournal::spawn([StatusCode::UNAUTHORIZED, StatusCode::OK]).await;
        let bundle = subscription_bundle(
            profile,
            "old-live-token",
            now_secs(fixture.clock.as_ref()) + 3600,
            false,
        );
        let upstream_id = create_subscription_upstream(&fixture, &journal, kind, bundle).await;
        let (key_store, key) = make_key(&fixture, upstream_id).await;
        let handle = Arc::new(StatefulRefreshHandle {
            storage: fixture.storage.clone(),
            aead: fixture.aead.clone(),
            replacement_access_token: "new-live-token".to_owned(),
            calls: Arc::new(AtomicUsize::new(0)),
            clock: fixture.clock.clone(),
            terminal: false,
        });
        let lifecycle = make_lifecycle(&fixture, key_store, handle.clone()).await;
        let (status, body) = dispatch(&lifecycle, request_for(kind, &key)).await;
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        assert_eq!(handle.calls(), 1);
        let requests = journal.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].authorization.as_deref(),
            Some("Bearer old-live-token")
        );
        assert_eq!(
            requests[1].authorization.as_deref(),
            Some("Bearer new-live-token")
        );
        assert_eq!(
            requests[0].account_id.as_deref(),
            (kind == UpstreamKind::OpenaiOauth).then_some("account-preserved"),
        );
        if kind == UpstreamKind::GeminiOauth {
            let envelope: Value =
                serde_json::from_slice(&requests[0].body).expect("Gemini request envelope");
            assert_eq!(envelope["project"], "project-preserved");
        }
        assert_eq!(
            requests[1].account_id.as_deref(),
            requests[0].account_id.as_deref()
        );
        let record = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await
            .expect("upstream read")
            .expect("upstream exists");
        let stored = record
            .oauth_credentials
            .expect("stored credentials")
            .decrypt(fixture.aead.as_ref(), upstream_id.as_bytes())
            .expect("decrypt credentials");
        assert_eq!(stored.account_id.as_deref(), Some("account-preserved"));
        assert_eq!(stored.project_id.as_deref(), Some("project-preserved"));
    }
}

#[tokio::test]
async fn access_only_401_persists_status_with_rejected_token_generation_without_retry() {
    for (kind, profile) in [
        (
            UpstreamKind::OpenaiOauth,
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
        ),
        (
            UpstreamKind::GeminiOauth,
            cc_lb_aead::GeminiOAuthProfile::Antigravity,
        ),
    ] {
        let fixture = Fixture::new_without_scheduler(AppConfig::default()).await;
        let journal = ProviderJournal::spawn([StatusCode::UNAUTHORIZED]).await;
        let upstream_id = create_subscription_upstream(
            &fixture,
            &journal,
            kind,
            subscription_bundle(
                profile,
                "access-only-rejected-token",
                now_secs(fixture.clock.as_ref()) + 3600,
                true,
            ),
        )
        .await;
        let rejected_generation = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await
            .expect("upstream read")
            .expect("upstream exists")
            .oauth_token_generation;
        let (key_store, key) = make_key(&fixture, upstream_id).await;
        let handle = Arc::new(StatefulRefreshHandle {
            storage: fixture.storage.clone(),
            aead: fixture.aead.clone(),
            replacement_access_token: "must-not-be-used".to_owned(),
            calls: Arc::new(AtomicUsize::new(0)),
            clock: fixture.clock.clone(),
            terminal: false,
        });
        let lifecycle = make_lifecycle(&fixture, key_store, handle.clone()).await;

        let (status, _) = dispatch(&lifecycle, request_for(kind, &key)).await;

        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(handle.calls(), 0);
        assert_eq!(journal.requests().len(), 1);
        let record = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await
            .expect("upstream read")
            .expect("upstream exists");
        assert_eq!(record.last_apply_error.as_deref(), Some("status_401"));
        assert_eq!(record.oauth_token_generation, rejected_generation);
    }
}

#[tokio::test]
async fn access_only_401_cannot_overwrite_replacement_status_generation() {
    for (kind, profile) in [
        (
            UpstreamKind::OpenaiOauth,
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
        ),
        (
            UpstreamKind::GeminiOauth,
            cc_lb_aead::GeminiOAuthProfile::Antigravity,
        ),
    ] {
        let fixture = Fixture::new_without_scheduler(AppConfig::default()).await;
        let journal = ProviderJournal::spawn([StatusCode::UNAUTHORIZED]).await;
        let upstream_id = create_subscription_upstream(
            &fixture,
            &journal,
            kind,
            subscription_bundle(
                profile,
                "stale-access-only-token",
                now_secs(fixture.clock.as_ref()) + 3600,
                true,
            ),
        )
        .await;
        let (key_store, key) = make_key(&fixture, upstream_id).await;
        let handle = Arc::new(StatefulRefreshHandle {
            storage: fixture.storage.clone(),
            aead: fixture.aead.clone(),
            replacement_access_token: "must-not-be-used".to_owned(),
            calls: Arc::new(AtomicUsize::new(0)),
            clock: fixture.clock.clone(),
            terminal: false,
        });
        let lifecycle = make_lifecycle(&fixture, key_store, handle).await;
        let before_replacement = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await
            .expect("upstream read")
            .expect("upstream exists");
        let replacement = subscription_bundle(
            profile,
            "replacement-access-only-token",
            now_secs(fixture.clock.as_ref()) + 3600,
            true,
        );
        UpstreamStore::store_oauth_tokens(
            fixture.storage.as_ref(),
            upstream_id,
            before_replacement.revision,
            encrypted_tokens(fixture.aead.as_ref(), upstream_id, &replacement),
            true,
        )
        .await
        .expect("replacement credentials stored");

        let (status, _) = dispatch(&lifecycle, request_for(kind, &key)).await;

        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(journal.requests().len(), 1);
        assert_eq!(
            journal.requests()[0].authorization.as_deref(),
            Some("Bearer stale-access-only-token")
        );
        let record = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await
            .expect("upstream read")
            .expect("upstream exists");
        assert!(record.oauth_token_generation > before_replacement.oauth_token_generation);
        assert_ne!(record.last_apply_error.as_deref(), Some("status_401"));
    }
}

#[tokio::test]
async fn repeated_401_is_bounded_to_one_refresh_and_two_provider_attempts() {
    let fixture = Fixture::new_without_scheduler(AppConfig::default()).await;
    let journal =
        ProviderJournal::spawn([StatusCode::UNAUTHORIZED, StatusCode::UNAUTHORIZED]).await;
    let kind = UpstreamKind::OpenaiOauth;
    let upstream_id = create_subscription_upstream(
        &fixture,
        &journal,
        kind,
        subscription_bundle(
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
            "old-repeated-401-token",
            now_secs(fixture.clock.as_ref()) + 3600,
            false,
        ),
    )
    .await;
    let (key_store, key) = make_key(&fixture, upstream_id).await;
    let handle = Arc::new(StatefulRefreshHandle {
        storage: fixture.storage.clone(),
        aead: fixture.aead.clone(),
        replacement_access_token: "new-repeated-401-token".to_owned(),
        calls: Arc::new(AtomicUsize::new(0)),
        clock: fixture.clock.clone(),
        terminal: false,
    });
    let lifecycle = make_lifecycle(&fixture, key_store, handle.clone()).await;
    let (status, _) = dispatch(&lifecycle, request_for(kind, &key)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(handle.calls(), 1);
    assert_eq!(journal.requests().len(), 2);
}

#[tokio::test]
async fn terminal_refresh_failure_requires_reconnect_and_stops_routing_after_rebind() {
    let fixture = Fixture::new_without_scheduler(AppConfig::default()).await;
    let journal = ProviderJournal::spawn([StatusCode::UNAUTHORIZED]).await;
    let kind = UpstreamKind::OpenaiOauth;
    let upstream_id = create_subscription_upstream(
        &fixture,
        &journal,
        kind,
        subscription_bundle(
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
            "old-terminal-token",
            now_secs(fixture.clock.as_ref()) + 3600,
            false,
        ),
    )
    .await;
    let (key_store, key) = make_key(&fixture, upstream_id).await;
    let handle = Arc::new(StatefulRefreshHandle {
        storage: fixture.storage.clone(),
        aead: fixture.aead.clone(),
        replacement_access_token: "unused-terminal-token".to_owned(),
        calls: Arc::new(AtomicUsize::new(0)),
        clock: fixture.clock.clone(),
        terminal: true,
    });
    let lifecycle = make_lifecycle(&fixture, key_store.clone(), handle.clone()).await;
    let (status, _) = dispatch(&lifecycle, request_for(kind, &key)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(handle.calls(), 1);
    assert_eq!(journal.requests().len(), 1);

    let rebound = make_lifecycle(&fixture, key_store, handle).await;
    let (status, body) = dispatch(&rebound, request_for(kind, &key)).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(journal.requests().len(), 1);
}

#[tokio::test]
async fn stale_dynamic_snapshot_reuses_background_replacement_without_second_refresh() {
    let fixture = Fixture::new_without_scheduler(AppConfig::default()).await;
    let journal = ProviderJournal::spawn([StatusCode::UNAUTHORIZED, StatusCode::OK]).await;
    let kind = UpstreamKind::OpenaiOauth;
    let upstream_id = create_subscription_upstream(
        &fixture,
        &journal,
        kind,
        subscription_bundle(
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
            "old-snapshot-token",
            now_secs(fixture.clock.as_ref()) + 3600,
            false,
        ),
    )
    .await;
    let (key_store, key) = make_key(&fixture, upstream_id).await;
    let handle = Arc::new(StatefulRefreshHandle {
        storage: fixture.storage.clone(),
        aead: fixture.aead.clone(),
        replacement_access_token: "background-token".to_owned(),
        calls: Arc::new(AtomicUsize::new(0)),
        clock: fixture.clock.clone(),
        terminal: false,
    });
    let lifecycle = make_lifecycle(&fixture, key_store, handle.clone()).await;

    let record = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
        .await
        .expect("upstream read")
        .expect("upstream exists");
    let mut replacement = record
        .oauth_credentials
        .expect("credentials")
        .decrypt(fixture.aead.as_ref(), upstream_id.as_bytes())
        .expect("decrypt credentials");
    replacement.access_token = "background-token".to_owned();
    replacement.expires_at_unix_secs = now_secs(fixture.clock.as_ref()) + 3600;
    UpstreamStore::store_oauth_tokens(
        fixture.storage.as_ref(),
        upstream_id,
        record.revision,
        encrypted_tokens(fixture.aead.as_ref(), upstream_id, &replacement),
        false,
    )
    .await
    .expect("background replacement stored");

    let (status, body) = dispatch(&lifecycle, request_for(kind, &key)).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(handle.calls(), 0);
    let requests = journal.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].authorization.as_deref(),
        Some("Bearer old-snapshot-token")
    );
    assert_eq!(
        requests[1].authorization.as_deref(),
        Some("Bearer background-token")
    );
}

#[tokio::test]
async fn never_refresh_control_preserves_expired_access_token_and_skips_refresh() {
    for (kind, profile) in [
        (
            UpstreamKind::OpenaiOauth,
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
        ),
        (
            UpstreamKind::GeminiOauth,
            cc_lb_aead::GeminiOAuthProfile::Antigravity,
        ),
    ] {
        let fixture = Fixture::new_without_scheduler(AppConfig::default()).await;
        let journal = ProviderJournal::spawn([StatusCode::OK]).await;
        let upstream_id = create_subscription_upstream(
            &fixture,
            &journal,
            kind,
            subscription_bundle(
                profile,
                "never-refresh-token",
                now_secs(fixture.clock.as_ref()).saturating_sub(1),
                true,
            ),
        )
        .await;
        let (key_store, key) = make_key(&fixture, upstream_id).await;
        let handle = Arc::new(StatefulRefreshHandle {
            storage: fixture.storage.clone(),
            aead: fixture.aead.clone(),
            replacement_access_token: "must-not-be-used".to_owned(),
            calls: Arc::new(AtomicUsize::new(0)),
            clock: fixture.clock.clone(),
            terminal: false,
        });
        let lifecycle = make_lifecycle(&fixture, key_store, handle.clone()).await;
        let (status, body) = dispatch(&lifecycle, request_for(kind, &key)).await;
        assert_eq!(
            status,
            StatusCode::BAD_GATEWAY,
            "{}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(handle.calls(), 0);
        assert_eq!(journal.requests().len(), 0);
    }
}

fn request_kind_for(kind: UpstreamKind) -> cc_lb_engine::RequestKind {
    match kind {
        UpstreamKind::AnthropicOauth => cc_lb_engine::RequestKind::AnthropicMessages,
        UpstreamKind::OpenaiOauth => cc_lb_engine::RequestKind::OpenaiResponses,
        UpstreamKind::GeminiOauth => cc_lb_engine::RequestKind::GeminiGenerateContent,
        other => panic!("unexpected upstream kind {other:?}"),
    }
}

async fn create_named_subscription_upstream(
    fixture: &Fixture,
    id: Uuid,
    name: &str,
    base_url: &str,
    kind: UpstreamKind,
    bundle: OAuthTokenBundle,
) -> Uuid {
    let record = fixture
        .storage
        .create(UpstreamCreate {
            id,
            name: name.to_owned(),
            kind,
            base_url: Some(Url::parse(base_url).expect("upstream URL")),
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        })
        .await
        .expect("named subscription upstream created");
    let encrypted = encrypted_tokens(fixture.aead.as_ref(), record.id, &bundle);
    UpstreamStore::store_oauth_tokens(
        fixture.storage.as_ref(),
        record.id,
        record.revision,
        encrypted,
        bundle.never_refresh,
    )
    .await
    .expect("subscription tokens stored");
    record.id
}

async fn make_key_for_upstreams(
    fixture: &Fixture,
    allowed_upstreams: Vec<Uuid>,
) -> (Arc<KeyStore>, String) {
    cc_lb_storage_api::PrincipalStore::create(
        fixture.storage.as_ref(),
        PrincipalCreate {
            name: "subscription-principal".to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: vec!["*".to_owned()],
            allowed_upstreams,
            default_limits: Vec::new(),
            cache_keepalive: None,
            openai_oauth_compat: None,
        },
        now_secs(fixture.clock.as_ref()),
    )
    .await
    .expect("principal created");
    let key_store = Arc::new(KeyStore::new(fixture.storage.clone()));
    let (_, secret) = key_store
        .create(
            "subscription-principal",
            CreateParams {
                label: "subscription-oauth-dispatch".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: Vec::new(),
            },
        )
        .await
        .expect("managed key created");
    (key_store, secret.expose().to_owned())
}

async fn rebuild_view(
    fixture: &Fixture,
    lazy: Arc<dyn LazyRefreshHandle>,
) -> Arc<cc_lb_control::DynamicView> {
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let antigravity = AntigravityOAuthConfig {
        user_agent: "antigravity/oauth-refresh-test".to_owned(),
        ..Default::default()
    };
    build_dynamic_view(
        fixture.stores.as_ref(),
        None,
        Some(&antigravity),
        fixture.aead.clone(),
        Some(lazy),
        0,
        &runtime,
        fixture._dir.path(),
        Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
        30,
        Arc::new(cc_lb_control::NoopPromptCacheObservationSink),
        1800,
        fixture.clock.clone(),
    )
    .await
    .expect("dynamic view builds")
}

fn lifecycle_from_view(
    fixture: &Fixture,
    key_store: Arc<KeyStore>,
    view: Arc<cc_lb_control::DynamicView>,
) -> Lifecycle {
    Lifecycle::new_with_dynamic_view(
        Arc::new(BuiltinAuthn::new(key_store, fixture.clock.clone())),
        Arc::new(DynamicViewHolder::new(view)),
        cc_lb_engine::make_default_dispatcher(50),
        LifecycleConfig::default(),
        fixture.clock.clone(),
    )
}

#[tokio::test]
async fn post_refresh_unauthorized_persists_status_401_excludes_routing_and_reconnect_restores() {
    for (kind, profile) in [
        (
            UpstreamKind::OpenaiOauth,
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
        ),
        (
            UpstreamKind::GeminiOauth,
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
        ),
        (
            UpstreamKind::GeminiOauth,
            cc_lb_aead::GeminiOAuthProfile::Antigravity,
        ),
        (
            UpstreamKind::AnthropicOauth,
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
        ),
    ] {
        let fixture = Fixture::new_without_scheduler(AppConfig::default()).await;
        let journal = ProviderJournal::spawn([
            StatusCode::UNAUTHORIZED,
            StatusCode::UNAUTHORIZED,
            StatusCode::OK,
        ])
        .await;
        let upstream_id = create_subscription_upstream(
            &fixture,
            &journal,
            kind,
            subscription_bundle(
                profile,
                "stale-access-token",
                now_secs(fixture.clock.as_ref()) + 3600,
                false,
            ),
        )
        .await;
        let (key_store, key) = make_key(&fixture, upstream_id).await;
        let handle = Arc::new(StatefulRefreshHandle {
            storage: fixture.storage.clone(),
            aead: fixture.aead.clone(),
            replacement_access_token: "refreshed-access-token".to_owned(),
            calls: Arc::new(AtomicUsize::new(0)),
            clock: fixture.clock.clone(),
            terminal: false,
        });
        let lifecycle = make_lifecycle(&fixture, key_store.clone(), handle.clone()).await;

        let (status, body) = dispatch(&lifecycle, request_for(kind, &key)).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{kind:?}/{profile:?}: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(handle.calls(), 1);
        let requests = journal.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].authorization.as_deref(),
            Some("Bearer stale-access-token")
        );
        assert_eq!(
            requests[1].authorization.as_deref(),
            Some("Bearer refreshed-access-token")
        );

        let rejected = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await
            .expect("upstream fetch")
            .expect("upstream exists");
        assert_eq!(rejected.last_apply_error.as_deref(), Some("status_401"));
        assert_eq!(rejected.oauth_token_generation, 2);
        assert!(cc_lb_oauth_protocol::refresh_requires_reconnect(
            rejected.last_apply_error.as_deref()
        ));

        let excluded_view = rebuild_view(&fixture, handle.clone()).await;
        let candidates = cc_lb_engine::build_candidates(
            &excluded_view,
            "subscription-principal",
            request_kind_for(kind),
            &[],
            &std::collections::HashMap::new(),
            fixture.clock.as_ref(),
        );
        assert!(
            candidates.is_empty(),
            "{kind:?}/{profile:?}: upstream with status_401 must be excluded from routing"
        );
        let excluded_lifecycle = lifecycle_from_view(&fixture, key_store.clone(), excluded_view);
        let (excluded_status, _) = dispatch(&excluded_lifecycle, request_for(kind, &key)).await;
        assert_eq!(excluded_status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            journal.requests().len(),
            2,
            "excluded upstream must not receive upstream traffic"
        );

        // QA-6: reconnect clears status_401 and restores routing.
        UpstreamStore::store_oauth_tokens(
            fixture.storage.as_ref(),
            upstream_id,
            rejected.revision,
            encrypted_tokens(
                fixture.aead.as_ref(),
                upstream_id,
                &subscription_bundle(
                    profile,
                    "reconnected-access-token",
                    now_secs(fixture.clock.as_ref()) + 7200,
                    false,
                ),
            ),
            false,
        )
        .await
        .expect("reconnect stores new tokens");
        let restored = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await
            .expect("upstream fetch")
            .expect("upstream exists");
        assert_eq!(restored.last_apply_error, None);
        assert_eq!(restored.oauth_token_generation, 3);

        let restored_view = rebuild_view(&fixture, handle.clone()).await;
        let restored_candidates = cc_lb_engine::build_candidates(
            &restored_view,
            "subscription-principal",
            request_kind_for(kind),
            &[],
            &std::collections::HashMap::new(),
            fixture.clock.as_ref(),
        );
        assert_eq!(restored_candidates.len(), 1);
        let restored_lifecycle = lifecycle_from_view(&fixture, key_store, restored_view);
        let (ok_status, ok_body) = dispatch(&restored_lifecycle, request_for(kind, &key)).await;
        assert_eq!(
            ok_status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&ok_body)
        );
        let final_requests = journal.requests();
        assert_eq!(final_requests.len(), 3);
        assert_eq!(
            final_requests[2].authorization.as_deref(),
            Some("Bearer reconnected-access-token")
        );
    }
}

#[tokio::test]
async fn post_refresh_failover_marks_failed_upstream_before_failing_over() {
    for (kind, profile) in [
        (
            UpstreamKind::OpenaiOauth,
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
        ),
        (
            UpstreamKind::GeminiOauth,
            cc_lb_aead::GeminiOAuthProfile::Antigravity,
        ),
    ] {
        let fixture = Fixture::new_without_scheduler(AppConfig::default()).await;
        let journal_a =
            ProviderJournal::spawn([StatusCode::UNAUTHORIZED, StatusCode::UNAUTHORIZED]).await;
        let journal_b = ProviderJournal::spawn([StatusCode::OK]).await;
        let id_a = Uuid::from_u128(1);
        let id_b = Uuid::from_u128(2);
        let upstream_a = create_named_subscription_upstream(
            &fixture,
            id_a,
            "a-failing",
            &journal_a.base_url,
            kind,
            subscription_bundle(
                profile,
                "a-stale-token",
                now_secs(fixture.clock.as_ref()) + 3600,
                false,
            ),
        )
        .await;
        let upstream_b = create_named_subscription_upstream(
            &fixture,
            id_b,
            "b-healthy",
            &journal_b.base_url,
            kind,
            subscription_bundle(
                profile,
                "b-healthy-token",
                now_secs(fixture.clock.as_ref()) + 3600,
                false,
            ),
        )
        .await;
        let (key_store, key) = make_key_for_upstreams(&fixture, vec![upstream_a, upstream_b]).await;
        let handle = Arc::new(StatefulRefreshHandle {
            storage: fixture.storage.clone(),
            aead: fixture.aead.clone(),
            replacement_access_token: "a-refreshed-token".to_owned(),
            calls: Arc::new(AtomicUsize::new(0)),
            clock: fixture.clock.clone(),
            terminal: false,
        });
        let view = rebuild_view(&fixture, handle.clone()).await;
        let candidates = cc_lb_engine::build_candidates(
            &view,
            "subscription-principal",
            request_kind_for(kind),
            &[],
            &std::collections::HashMap::new(),
            fixture.clock.as_ref(),
        );
        assert_eq!(candidates.len(), 2);
        assert_eq!(
            candidates[0].upstream_id, upstream_a,
            "first-pick candidate must try upstream_a first"
        );
        let lifecycle = lifecycle_from_view(&fixture, key_store, view);

        let (status, body) = dispatch(&lifecycle, request_for(kind, &key)).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{kind:?}: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(handle.calls(), 1);
        assert_eq!(journal_a.requests().len(), 2);
        assert_eq!(journal_b.requests().len(), 1);

        let record_a = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_a)
            .await
            .expect("upstream_a fetch")
            .expect("upstream_a exists");
        let record_b = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_b)
            .await
            .expect("upstream_b fetch")
            .expect("upstream_b exists");
        assert_eq!(record_a.last_apply_error.as_deref(), Some("status_401"));
        assert_eq!(record_b.last_apply_error, None);
    }
}

#[tokio::test]
async fn post_refresh_retry_200_and_403_leave_last_apply_error_clear() {
    for (kind, profile) in [
        (
            UpstreamKind::OpenaiOauth,
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
        ),
        (
            UpstreamKind::GeminiOauth,
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
        ),
        (
            UpstreamKind::GeminiOauth,
            cc_lb_aead::GeminiOAuthProfile::Antigravity,
        ),
        (
            UpstreamKind::AnthropicOauth,
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
        ),
    ] {
        for retry_status in [StatusCode::OK, StatusCode::FORBIDDEN] {
            let fixture = Fixture::new_without_scheduler(AppConfig::default()).await;
            let journal = ProviderJournal::spawn([StatusCode::UNAUTHORIZED, retry_status]).await;
            let upstream_id = create_subscription_upstream(
                &fixture,
                &journal,
                kind,
                subscription_bundle(
                    profile,
                    "stale-access-token",
                    now_secs(fixture.clock.as_ref()) + 3600,
                    false,
                ),
            )
            .await;
            let (key_store, key) = make_key(&fixture, upstream_id).await;
            let handle = Arc::new(StatefulRefreshHandle {
                storage: fixture.storage.clone(),
                aead: fixture.aead.clone(),
                replacement_access_token: "refreshed-access-token".to_owned(),
                calls: Arc::new(AtomicUsize::new(0)),
                clock: fixture.clock.clone(),
                terminal: false,
            });
            let lifecycle = make_lifecycle(&fixture, key_store, handle.clone()).await;

            let (status, body) = dispatch(&lifecycle, request_for(kind, &key)).await;
            assert_eq!(
                status,
                retry_status,
                "{kind:?}/{profile:?}: {}",
                String::from_utf8_lossy(&body)
            );
            assert_eq!(handle.calls(), 1);
            let stored = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
                .await
                .expect("upstream fetch")
                .expect("upstream exists");
            assert_eq!(
                stored.last_apply_error, None,
                "{kind:?}/{profile:?}/{retry_status} must not mark reconnect-required"
            );
        }
    }
}

#[tokio::test]
async fn post_refresh_generation_guard_ignores_concurrent_reconnect() {
    for (kind, profile) in [
        (
            UpstreamKind::OpenaiOauth,
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
        ),
        (
            UpstreamKind::GeminiOauth,
            cc_lb_aead::GeminiOAuthProfile::Antigravity,
        ),
        (
            UpstreamKind::AnthropicOauth,
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
        ),
    ] {
        let fixture = Fixture::new_without_scheduler(AppConfig::default()).await;
        let upstream_id = Uuid::new_v4();
        let state = ProviderJournalState {
            requests: Arc::new(Mutex::new(Vec::new())),
            statuses: Arc::new(Mutex::new(
                [
                    StatusCode::UNAUTHORIZED,
                    StatusCode::UNAUTHORIZED,
                    StatusCode::OK,
                ]
                .into_iter()
                .collect(),
            )),
        };
        let storage_for_hook = fixture.storage.clone();
        let aead_for_hook = fixture.aead.clone();
        let clock_for_hook = fixture.clock.clone();
        let hook_state = state.clone();
        let app = Router::new().fallback(move |request: axum::extract::Request| {
            let state = hook_state.clone();
            let storage = storage_for_hook.clone();
            let aead = aead_for_hook.clone();
            let clock = clock_for_hook.clone();
            async move {
                let count = state.requests.lock().expect("requests lock").len();
                if count == 1 {
                    let current = UpstreamStore::get_by_id(storage.as_ref(), upstream_id)
                        .await
                        .expect("upstream read")
                        .expect("upstream exists");
                    UpstreamStore::store_oauth_tokens(
                        storage.as_ref(),
                        upstream_id,
                        current.revision,
                        encrypted_tokens(
                            aead.as_ref(),
                            upstream_id,
                            &subscription_bundle(
                                profile,
                                "concurrent-reconnected-token",
                                now_secs(clock.as_ref()) + 7200,
                                false,
                            ),
                        ),
                        false,
                    )
                    .await
                    .expect("concurrent reconnect commits before retry 401 arrives");
                }
                provider_handler(State(state), request).await
            }
        });
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind race provider");
        let address = listener.local_addr().expect("race provider addr");
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        let base_url = format!("http://{address}");

        create_named_subscription_upstream(
            &fixture,
            upstream_id,
            "race-upstream",
            &base_url,
            kind,
            subscription_bundle(
                profile,
                "stale-access-token",
                now_secs(fixture.clock.as_ref()) + 3600,
                false,
            ),
        )
        .await;
        let (key_store, key) = make_key(&fixture, upstream_id).await;
        let handle = Arc::new(StatefulRefreshHandle {
            storage: fixture.storage.clone(),
            aead: fixture.aead.clone(),
            replacement_access_token: "refreshed-access-token".to_owned(),
            calls: Arc::new(AtomicUsize::new(0)),
            clock: fixture.clock.clone(),
            terminal: false,
        });
        let lifecycle = make_lifecycle(&fixture, key_store.clone(), handle.clone()).await;

        let (status, _) = dispatch(&lifecycle, request_for(kind, &key)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(handle.calls(), 1);

        let latest = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await
            .expect("upstream fetch")
            .expect("upstream exists");
        assert_eq!(latest.oauth_token_generation, 3);
        assert_eq!(
            latest.last_apply_error, None,
            "{kind:?}: generation guard must protect concurrent reconnect"
        );

        let next_view = rebuild_view(&fixture, handle).await;
        let next_lifecycle = lifecycle_from_view(&fixture, key_store, next_view);
        let (ok_status, _) = dispatch(&next_lifecycle, request_for(kind, &key)).await;
        assert_eq!(ok_status, StatusCode::OK);
        let requests = state.requests.lock().expect("requests lock").clone();
        assert_eq!(requests.len(), 3);
        assert_eq!(
            requests[2].authorization.as_deref(),
            Some("Bearer concurrent-reconnected-token")
        );
        task.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_refresh_unauthorized_expedites_future_scheduled_job_through_apalis_worker() {
    for (kind, profile) in [
        (
            UpstreamKind::OpenaiOauth,
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
        ),
        (
            UpstreamKind::AnthropicOauth,
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
        ),
        (
            UpstreamKind::GeminiOauth,
            cc_lb_aead::GeminiOAuthProfile::Antigravity,
        ),
    ] {
        let fixture = Fixture::new().await;
        let journal =
            ProviderJournal::spawn([StatusCode::UNAUTHORIZED, StatusCode::UNAUTHORIZED]).await;
        let tokens = initial_tokens(&fixture.fake_base).await;
        let expires_at = now_secs(fixture.clock.as_ref()) + 3_600;
        let upstream_id = create_named_subscription_upstream(
            &fixture,
            Uuid::new_v4(),
            "expedite-oauth",
            &journal.base_url,
            kind,
            OAuthTokenBundle {
                access_token: tokens.access_token.clone(),
                refresh_token: tokens.refresh_token.clone(),
                expires_at_unix_secs: expires_at,
                refresh_token_expires_at_unix_secs: None,
                account_id: Some("account-preserved".to_owned()),
                project_id: Some("project-preserved".to_owned()),
                oauth_profile: profile,
                scopes: vec!["messages".to_owned()],
                never_refresh: false,
            },
        )
        .await;

        // Seed the future proactive refresh task (run_at = expires_at - 300) using the
        // exact idempotency key that ApalisLazyRefreshClaimGuard::begin_refresh builds.
        enqueue_next_oauth_refresh(fixture.scheduler_backend.clone(), upstream_id, expires_at)
            .await
            .expect("seed future proactive refresh task");

        let (key_store, key) = make_key(&fixture, upstream_id).await;
        let lazy = Arc::new(LazyRefresher::new(LazyRefresherParams {
            deps: LazyRefresherDeps {
                google_config: Default::default(),
                stores: fixture.stores.clone(),
                aead: fixture.aead.clone(),
                oauth_cfg: fixture.oauth_cfg.clone(),
                clock: fixture.clock.clone(),
            },
            replica_id: Uuid::new_v4(),
            metadata_hook: None,
            cancel: CancellationToken::new(),
            apalis_handle: fixture.scheduler_backend.clone(),
        }));
        let lifecycle = make_lifecycle(&fixture, key_store, lazy).await;

        // Must complete promptly via worker pickup of the expedited Pending task,
        // well under LazyRefresher's 60s contention timeout.
        let (status, body) = timeout(
            Duration::from_secs(5),
            dispatch(&lifecycle, request_for(kind, &key)),
        )
        .await
        .expect("forced refresh must expedite future Pending task rather than wait until expiry");
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{kind:?}/{profile:?}: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(
            refresh_history_len(&fixture.fake_base).await,
            1,
            "{kind:?}/{profile:?}: expedited worker must execute exactly one refresh"
        );
        let requests = journal.requests();
        assert_eq!(requests.len(), 2);
        assert_ne!(requests[0].authorization, requests[1].authorization);

        let updated = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await
            .expect("upstream read")
            .expect("upstream exists");
        assert_eq!(updated.last_apply_error.as_deref(), Some("status_401"));
        assert_eq!(updated.oauth_token_generation, 2);
    }
}

#[tokio::test]
async fn stale_dynamic_snapshot_advances_shared_state_and_marks_status_401_on_next_request() {
    let fixture = Fixture::new_without_scheduler(AppConfig::default()).await;
    let journal = ProviderJournal::spawn([
        StatusCode::UNAUTHORIZED,
        StatusCode::UNAUTHORIZED,
        StatusCode::UNAUTHORIZED,
        StatusCode::UNAUTHORIZED,
    ])
    .await;
    let kind = UpstreamKind::OpenaiOauth;
    let upstream_id = create_subscription_upstream(
        &fixture,
        &journal,
        kind,
        subscription_bundle(
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
            "stale-snapshot-token",
            now_secs(fixture.clock.as_ref()) + 3600,
            false,
        ),
    )
    .await;
    let (key_store, key) = make_key(&fixture, upstream_id).await;
    let handle = Arc::new(StatefulRefreshHandle {
        storage: fixture.storage.clone(),
        aead: fixture.aead.clone(),
        replacement_access_token: "forced-token".to_owned(),
        calls: Arc::new(AtomicUsize::new(0)),
        clock: fixture.clock.clone(),
        terminal: false,
    });
    let lifecycle = make_lifecycle(&fixture, key_store, handle.clone()).await;

    let record = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
        .await
        .expect("upstream read")
        .expect("upstream exists");
    let mut replacement = record
        .oauth_credentials
        .expect("credentials")
        .decrypt(fixture.aead.as_ref(), upstream_id.as_bytes())
        .expect("decrypt credentials");
    replacement.access_token = "background-token".to_owned();
    replacement.expires_at_unix_secs = now_secs(fixture.clock.as_ref()) + 3600;
    UpstreamStore::store_oauth_tokens(
        fixture.storage.as_ref(),
        upstream_id,
        record.revision,
        encrypted_tokens(fixture.aead.as_ref(), upstream_id, &replacement),
        false,
    )
    .await
    .expect("background replacement stored");

    // Request 1 sends stale snapshot T1 -> 401 -> reuses background T2 (no refresh_one) -> 401.
    let (status1, _) = dispatch(&lifecycle, request_for(kind, &key)).await;
    assert_eq!(status1, StatusCode::UNAUTHORIZED);
    assert_eq!(handle.calls(), 0);
    assert_eq!(
        UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await
            .expect("upstream read")
            .expect("upstream exists")
            .last_apply_error,
        None
    );

    // Request 2 on the same DynamicView starts directly from shared T2 -> 401 -> forces T3 -> 401 -> status_401.
    let (status2, _) = dispatch(&lifecycle, request_for(kind, &key)).await;
    assert_eq!(status2, StatusCode::UNAUTHORIZED);
    assert_eq!(handle.calls(), 1);
    let requests = journal.requests();
    assert_eq!(requests.len(), 4);
    assert_eq!(
        requests[0].authorization.as_deref(),
        Some("Bearer stale-snapshot-token")
    );
    assert_eq!(
        requests[1].authorization.as_deref(),
        Some("Bearer background-token")
    );
    assert_eq!(
        requests[2].authorization.as_deref(),
        Some("Bearer background-token")
    );
    assert_eq!(
        requests[3].authorization.as_deref(),
        Some("Bearer forced-token")
    );
    let final_record = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
        .await
        .expect("upstream read")
        .expect("upstream exists");
    assert_eq!(final_record.last_apply_error.as_deref(), Some("status_401"));
}
