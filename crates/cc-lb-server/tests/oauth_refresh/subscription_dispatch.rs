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
        _ => panic!("subscription test only supports OpenAI/Gemini OAuth"),
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
