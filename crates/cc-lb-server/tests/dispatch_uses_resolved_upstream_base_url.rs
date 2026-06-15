//! Reproducer for `.omo/issues/2026-06-07-dispatch-ignores-admin-state.md`.
//!
//! When two AnthropicApiKey upstreams are enabled, the principal allows ONLY
//! the second one, but the first one is returned by `DbRouter::route` because
//! it currently uses `routes.first()` and `route.dialect` (which carries the
//! per-upstream `base_url` for AnthropicApiKey / AnthropicOauth) is taken from
//! the wrong upstream. The lifecycle correctly recomputes `route.upstream` and
//! `resolved_upstream_id` from the candidate set, but never recomputes the
//! dialect — so dispatch goes to whatever `base_url` the first route happens
//! to carry (defaulting to `https://api.anthropic.com` when `base_url=None`).
//!
//! Expected behavior: dispatch URL must match the resolved upstream's
//! `base_url` (i.e. the principal-allowed upstream). This test FAILS before
//! the fix and PASSES after the dialect is rebuilt for the resolved upstream.

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::body::Bytes;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens};
use cc_lb_config::{
    AnthropicOAuthConfig, DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind,
};
use cc_lb_core::api_keys::builtin_authn::BuiltinAuthn;
use cc_lb_core::{
    Body, DispatchError, DynamicViewBuilder, DynamicViewHolder, Lifecycle, LifecycleConfig,
    UpstreamDispatch,
};
use cc_lb_plugin_api::SignedRequest;
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_server::SubscriptionQuotaCache;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamLeaseKind, UpstreamStatusUpdate};
use cc_lb_storage_api::{
    PrincipalCreate, PrincipalKind, PrincipalStore, StorageResult, UpstreamCreate, UpstreamRecord,
    UpstreamStore, UpstreamUpdate,
};
use cc_lb_storage_redb::Storage;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use url::Url;
use uuid::Uuid;

#[tokio::test]
async fn dispatch_uses_resolved_upstream_base_url_not_first_route_dialect() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = Arc::new(
        Storage::open(&dir.path().join("base-url-dispatch.redb"), [33; 32]).expect("storage"),
    );

    // Two enabled AnthropicApiKey upstreams.
    //   * A "aaa-primary"          — no base_url override (defaults to https://api.anthropic.com)
    //   * B "bbb-target"           — base_url=http://target.invalid (the principal-allowed upstream)
    // The principal allows only B. `routes.first()` (sorted by name via the
    // wrapper below) will be A, so the buggy code path leaks A's dialect into
    // the dispatch — pointing at api.anthropic.com instead of target.invalid.
    let primary = UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            name: "aaa-primary".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: Some(vec![1, 2, 3]),
            warmup_enabled: false,
            next_warmup_at: None,
            last_warmup_cycle_key: None,
            warmup_lease_holder: None,
            warmup_lease_until_unix_secs: None,
            warmup_dialect_plugin: None,
        },
    )
    .await
    .expect("primary upstream created");

    let target = UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            name: "bbb-target".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: Some(Url::parse("http://target.invalid").expect("target base_url parses")),
            api_key_ciphertext: Some(vec![1, 2, 3]),
            warmup_enabled: false,
            next_warmup_at: None,
            last_warmup_cycle_key: None,
            warmup_lease_holder: None,
            warmup_lease_until_unix_secs: None,
            warmup_dialect_plugin: None,
        },
    )
    .await
    .expect("target upstream created");

    PrincipalStore::create(
        storage.as_ref(),
        PrincipalCreate {
            name: "test-principal".to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: vec![target.id],
            default_limits: Vec::new(),
        },
        now_secs(),
    )
    .await
    .expect("principal created");

    let stores = Arc::new(Stores {
        upstreams: Arc::new(NameSortedUpstreamStore {
            inner: storage.clone(),
        }),
        principals: storage.clone(),
        plugin_registry: storage.clone(),
        upstream_rate_limits: storage.clone(),
        upstream_subscription_quotas: storage.clone(),
        anthropic_compatibility_kv: storage.clone(),
        audit: Some(storage.clone()),
        plugin_registry_repo: None,
        prompt_cache_observations: storage.clone(),
    });

    let aead = Arc::new(AeadService::from_master_key([33; 32]));
    let oauth_cfg = Arc::new(AnthropicOAuthConfig {
        client_id: "unused-by-this-test".to_owned(),
        auth_url: Url::parse("http://unused.invalid/authorize").expect("auth url"),
        token_url: Url::parse("http://unused.invalid/token").expect("token url"),
        redirect_uri: Url::parse("http://unused.invalid/callback").expect("redirect url"),
        scopes: vec!["messages".to_owned()],
    });
    let runtime = ExtismRuntime::new();
    let config = cc_lb_config::Config::default();
    let view = build_dynamic_view(
        stores.as_ref(),
        oauth_cfg.as_ref(),
        aead.clone(),
        None,
        0,
        &runtime,
        dir.path(),
        Arc::new(SubscriptionQuotaCache::new()),
        1800,
        &config,
    )
    .await
    .expect("dynamic view builds");

    // Swap the default HTTPS dispatcher with a no-network recording one.
    let captured: Arc<Mutex<Vec<Url>>> = Arc::new(Mutex::new(Vec::new()));
    let recording = Arc::new(RecordingDispatcher {
        captured: captured.clone(),
    }) as Arc<dyn UpstreamDispatch>;
    let view = DynamicViewBuilder::from_view(&view)
        .dispatcher(recording)
        .build();
    let holder = Arc::new(DynamicViewHolder::new(view));

    let lifecycle = Lifecycle::new_with_dynamic_view(
        Arc::new(BuiltinAuthn::new(
            DownstreamAuthMode::None,
            Some(NoneModeConfig {
                principal_id: "test-principal".to_owned(),
                upstream_kind: NoneModeUpstreamKind::AnthropicKey,
            }),
            None,
        )),
        holder,
        LifecycleConfig::default(),
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

    let urls = captured.lock().expect("captured lock").clone();
    assert_eq!(
        urls.len(),
        1,
        "expected exactly one upstream dispatch; got {urls:?} status={status} body={body:?}"
    );
    let url = &urls[0];
    assert_eq!(
        url.host_str(),
        Some("target.invalid"),
        "dispatch must use the resolved (principal-allowed) upstream's base_url. \
         The principal only allows {target_id} (bbb-target, base_url=http://target.invalid), \
         so dispatch must point there. Actual URL: {url}. \
         status={status} body={body:?}",
        target_id = target.id,
    );
    assert_eq!(
        url.path(),
        "/v1/messages",
        "dispatched path must be /v1/messages, got {url}",
    );

    // Keep handles alive until the assertions complete.
    let _ = primary;
    let _ = target;
}

struct RecordingDispatcher {
    captured: Arc<Mutex<Vec<Url>>>,
}

#[async_trait]
impl UpstreamDispatch for RecordingDispatcher {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.captured
            .lock()
            .expect("captured lock")
            .push(request.url().clone());
        let body = Body::new(Full::from(Bytes::from_static(
            br#"{"type":"message","content":[]}"#,
        )));
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(body)
            .map_err(|err| DispatchError::RequestBuild {
                reason: err.to_string(),
            })
    }
}

struct NameSortedUpstreamStore {
    inner: Arc<Storage>,
}

#[async_trait]
impl UpstreamStore for NameSortedUpstreamStore {
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
        records.sort_by(|a, b| a.name.cmp(&b.name));
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

    async fn claim_lease(
        &self,
        id: Uuid,
        lease_kind: UpstreamLeaseKind,
        holder: String,
        ttl_secs: i64,
    ) -> StorageResult<bool> {
        UpstreamStore::claim_lease(self.inner.as_ref(), id, lease_kind, holder, ttl_secs).await
    }

    async fn renew_lease(
        &self,
        id: Uuid,
        lease_kind: UpstreamLeaseKind,
        holder: String,
        ttl_secs: i64,
    ) -> StorageResult<bool> {
        UpstreamStore::renew_lease(self.inner.as_ref(), id, lease_kind, holder, ttl_secs).await
    }

    async fn release_lease(
        &self,
        id: Uuid,
        lease_kind: UpstreamLeaseKind,
        holder: String,
    ) -> StorageResult<bool> {
        UpstreamStore::release_lease(self.inner.as_ref(), id, lease_kind, holder).await
    }

    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::store_oauth_tokens(self.inner.as_ref(), id, expected_revision, tokens).await
    }

    async fn claim_refresh_lease(
        &self,
        id: Uuid,
        holder: Uuid,
        ttl_secs: u64,
    ) -> StorageResult<bool> {
        UpstreamStore::claim_refresh_lease(self.inner.as_ref(), id, holder, ttl_secs).await
    }

    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        UpstreamStore::complete_refresh(self.inner.as_ref(), id, holder, tokens).await
    }

    async fn release_lease_on_failure(
        &self,
        id: Uuid,
        holder: Uuid,
        reason: String,
    ) -> StorageResult<()> {
        UpstreamStore::release_lease_on_failure(self.inner.as_ref(), id, holder, reason).await
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

    async fn claim_warmup_lease(
        &self,
        upstream_id: Uuid,
        holder: &str,
        ttl_secs: i64,
    ) -> StorageResult<bool> {
        UpstreamStore::claim_warmup_lease(self.inner.as_ref(), upstream_id, holder, ttl_secs).await
    }

    async fn write_warmup_cycle_key(
        &self,
        upstream_id: Uuid,
        holder: &str,
        new_cycle_key: i64,
        next_warmup_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> StorageResult<bool> {
        UpstreamStore::write_warmup_cycle_key(
            self.inner.as_ref(),
            upstream_id,
            holder,
            new_cycle_key,
            next_warmup_at,
        )
        .await
    }

    async fn release_warmup_lease(&self, id: Uuid, holder: &str) -> StorageResult<bool> {
        UpstreamStore::release_warmup_lease(self.inner.as_ref(), id, holder).await
    }

    async fn clear_warmup_dialect_plugin(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<UpstreamRecord>> {
        UpstreamStore::clear_warmup_dialect_plugin(self.inner.as_ref(), id, expected_revision).await
    }
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

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
