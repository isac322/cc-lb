// tier-allow(silent-skip): storage trait optional-return signatures never skip assertions until=2027-03-31
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Bytes;
use axum::http::{Method, StatusCode};
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::{
    AnthropicOAuthConfig, DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind,
};
use cc_lb_engine::api_keys::builtin_authn::BuiltinAuthn;
use cc_lb_engine::{
    Body, DispatchError, DynamicViewHolder, Lifecycle, LifecycleConfig, UpstreamDispatch,
};
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamStatusUpdate};
use cc_lb_storage_api::{
    PrincipalCreate, PrincipalKind, PrincipalStore, StorageResult, UpstreamCreate, UpstreamRecord,
    UpstreamStore, UpstreamUpdate,
};
use cc_lb_testkit::{InMemoryStorage as Storage, fixed_clock};
use cc_lb_upstream::SignedRequest;
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use url::Url;
use uuid::Uuid;

use crate::composite_signer_factory::EmptyDynamicStore;

const NOW_UNIX_SECS: u64 = 1_800_000_000;

#[tokio::test]
async fn t2__router_choice_dispatches_to_matching_oauth_upstream_not_first_anthropic_direct() {
    let fixture = Fixture::new().await;
    let target_id = fixture
        .create_oauth_upstream("oauth-target", NOW_UNIX_SECS + 3600, true)
        .await;
    fixture
        .create_missing_oauth_upstream_before(target_id)
        .await;
    fixture
        .create_principal("oauth-principal", vec![target_id])
        .await;
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let view = build_dynamic_view(
        fixture.stores.as_ref(),
        fixture.oauth_cfg.as_ref(),
        fixture.aead.clone(),
        None,
        0,
        &runtime,
        Path::new("."),
        Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
        None,
        None,
        1800,
        fixture.clock.clone(),
    )
    .await
    .expect("dynamic view builds");
    let captured = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Lifecycle::new_with_dynamic_view(
        Arc::new(BuiltinAuthn::new(
            DownstreamAuthMode::None,
            Some(NoneModeConfig {
                principal_id: "oauth-principal".to_owned(),
                upstream_kind: NoneModeUpstreamKind::AnthropicOAuth,
            }),
            None,
            fixture.clock.clone(),
        )),
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(RecordingDispatcher {
            captured: captured.clone(),
        }),
        LifecycleConfig::default(),
        fixture.clock.clone(),
    );

    let response = lifecycle
        .handle(message_request())
        .await
        .expect("lifecycle response");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();

    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let captured = captured.lock().expect("captured dispatch lock");
    assert_eq!(captured.len(), 1, "expected one upstream dispatch");
    assert_eq!(captured[0].url.host_str(), Some("oauth-target.invalid"));
    assert_eq!(
        captured[0].authorization.as_deref(),
        Some("Bearer sk-ant-oat01-oauth-target-access-token")
    );
}

struct Fixture {
    storage: Arc<Storage>,
    stores: Arc<Stores>,
    aead: Arc<AeadService>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    clock: cc_lb_engine::ClockHandle,
}

impl Fixture {
    async fn new() -> Self {
        let clock = fixed_clock(NOW_UNIX_SECS);
        let storage = Arc::new(Storage::with_clock(clock.clone()));
        let empty = Arc::new(EmptyDynamicStore);
        let stores = Arc::new(Stores {
            upstreams: Arc::new(OrderedUpstreamStore {
                inner: storage.clone(),
            }),
            principals: storage.clone(),
            plugin_registry: storage.clone(),
            upstream_rate_limits: empty.clone(),
            upstream_subscription_quotas: storage.clone(),
            upstream_subscription_metadata: empty.clone(),
            organization_metadata: empty.clone(),
            plan_tiers: empty,
            prompt_cache_observations: storage.clone(),
            anthropic_compatibility_kv: storage.clone(),
            audit: None,
        });
        let aead = Arc::new(AeadService::from_master_key([33; 32]));
        let oauth_cfg = Arc::new(AnthropicOAuthConfig {
            client_id: "test-client".to_owned(),
            auth_url: Url::parse("http://oauth.invalid/authorize").expect("auth url"),
            token_url: Url::parse("http://oauth.invalid/token").expect("token url"),
            redirect_uri: Url::parse("http://localhost/callback").expect("redirect url"),
            scopes: vec!["messages".to_owned()],
        });
        Self {
            storage,
            stores,
            aead,
            oauth_cfg,
            clock,
        }
    }

    async fn create_principal(&self, name: &str, allowed_upstreams: Vec<Uuid>) {
        PrincipalStore::create(
            self.storage.as_ref(),
            PrincipalCreate {
                name: name.to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams,
                default_limits: Vec::new(),
                cache_keepalive: None,
            },
            NOW_UNIX_SECS,
        )
        .await
        .expect("principal created");
    }

    async fn create_oauth_upstream(&self, name: &str, expires_at: u64, store_tokens: bool) -> Uuid {
        let record = UpstreamStore::create(
            self.storage.as_ref(),
            UpstreamCreate {
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: Some(
                    Url::parse(&format!("http://{name}.invalid")).expect("test base URL parses"),
                ),
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            },
        )
        .await
        .expect("upstream created");
        if store_tokens {
            UpstreamStore::store_oauth_tokens(
                self.storage.as_ref(),
                record.id,
                record.revision,
                encrypted(
                    &self.aead,
                    record.id,
                    &OAuthTokenBundle {
                        access_token: format!("sk-ant-oat01-{name}-access-token"),
                        refresh_token: format!("sk-ant-ort01-{name}-refresh-token"),
                        expires_at_unix_secs: expires_at,
                        refresh_token_expires_at_unix_secs: None,
                        scopes: vec!["messages".to_owned()],
                    },
                ),
            )
            .await
            .expect("tokens stored");
        }
        record.id
    }

    async fn create_missing_oauth_upstream_before(&self, before: Uuid) {
        let id = self
            .create_oauth_upstream("missing-before-target", NOW_UNIX_SECS + 3600, false)
            .await;
        assert_ne!(id, before);
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ObservedDispatch {
    url: Url,
    authorization: Option<String>,
}

struct RecordingDispatcher {
    captured: Arc<Mutex<Vec<ObservedDispatch>>>,
}

#[async_trait]
impl UpstreamDispatch for RecordingDispatcher {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.captured
            .lock()
            .expect("captured dispatch lock")
            .push(ObservedDispatch {
                url: request.url().clone(),
                authorization: request
                    .headers()
                    .get(http::header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .map(ToOwned::to_owned),
            });
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::new(Full::from(Bytes::from_static(
                br#"{"type":"message","content":[]}"#,
            ))))
            .map_err(|error| DispatchError::RequestBuild {
                reason: error.to_string(),
            })
    }
}

struct OrderedUpstreamStore {
    inner: Arc<Storage>,
}

#[async_trait]
impl UpstreamStore for OrderedUpstreamStore {
    async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
        UpstreamStore::create(self.inner.as_ref(), create).await
    }

    async fn get_by_name(&self, name: &str) -> StorageResult<Option<UpstreamRecord>> {
        UpstreamStore::get_by_name(self.inner.as_ref(), name).await
    }

    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
        UpstreamStore::get_by_id(self.inner.as_ref(), id).await
    }

    async fn list(&self, after: Option<Uuid>, limit: usize) -> StorageResult<Vec<UpstreamRecord>> {
        let mut records = UpstreamStore::list(self.inner.as_ref(), after, limit).await?;
        records.sort_by_key(|record| record.name != "missing-before-target");
        Ok(records)
    }

    async fn update(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::update(self.inner.as_ref(), id, expected_revision, update).await
    }

    async fn set_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::set_enabled(self.inner.as_ref(), id, expected_revision, enabled).await
    }

    async fn update_spec(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::update_spec(self.inner.as_ref(), id, expected_revision, update).await
    }

    async fn update_api_key_secret(
        &self,
        id: Uuid,
        api_key_ciphertext: Option<Vec<u8>>,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::update_api_key_secret(self.inner.as_ref(), id, api_key_ciphertext).await
    }

    async fn update_oauth_token(
        &self,
        id: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::update_oauth_token(self.inner.as_ref(), id, tokens).await
    }

    async fn set_status(&self, id: Uuid, status: UpstreamStatusUpdate) -> StorageResult<()> {
        UpstreamStore::set_status(self.inner.as_ref(), id, status).await
    }

    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::store_oauth_tokens(self.inner.as_ref(), id, expected_revision, tokens).await
    }

    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::complete_refresh(self.inner.as_ref(), id, holder, tokens).await
    }

    async fn set_last_apply_error(&self, id: Uuid, error: Option<String>) -> StorageResult<()> {
        UpstreamStore::set_last_apply_error(self.inner.as_ref(), id, error).await
    }

    async fn soft_delete(&self, id: Uuid, expected_revision: u64) -> StorageResult<()> {
        UpstreamStore::soft_delete(self.inner.as_ref(), id, expected_revision).await
    }

    async fn hard_delete(&self, id: Uuid) -> StorageResult<()> {
        UpstreamStore::hard_delete(self.inner.as_ref(), id).await
    }

    async fn clear_warmup_dialect_plugin(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<UpstreamRecord>> {
        UpstreamStore::clear_warmup_dialect_plugin(self.inner.as_ref(), id, expected_revision).await
    }
}

fn encrypted(
    aead: &AeadService,
    upstream_id: Uuid,
    bundle: &OAuthTokenBundle,
) -> EncryptedOAuthTokens {
    EncryptedOAuthTokens::encrypt(aead, bundle, upstream_id.as_bytes()).expect("encrypt")
}

fn message_request() -> Request<Bytes> {
    Request::builder()
        .method(Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", "sk-ant-downstream")
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .body(Bytes::from_static(
            br#"{"model":"claude-3-5-sonnet-20241022","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}"#,
        ))
        .expect("request builds")
}
