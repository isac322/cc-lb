#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use bytes::Bytes;
use cc_lb_admin::{AdminState, ConfigDraftError, CurrentConfig, router};
use cc_lb_config::Config;
use cc_lb_core::api_keys::{
    concurrent_guard::KeyConcurrencyManager, limit_engine::LimitEngine,
    principal_view::PrincipalView,
};
use cc_lb_core::{
    ApiKeyAwareSignerFactory, DispatchError, DynamicViewBuilder, DynamicViewHolder,
    ErrorNormalizer, UpstreamDispatch, UpstreamStatusSnapshot,
};
use cc_lb_plugin_api::{
    ObservabilityHook, Principal, RequestContext, RouteDecision, RouteError, RouterPlugin,
    SignedRequest, SignerFactory, Upstream,
};
use cc_lb_storage_redb::RedbStorage;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

pub const TOKEN: &str = "test-token";

pub fn temp_storage() -> (tempfile::TempDir, Arc<RedbStorage>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(RedbStorage::open(&dir.path().join("test.redb"), [0; 32]).unwrap());
    (dir, storage)
}

pub fn test_storage() -> Arc<RedbStorage> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.redb");
    let storage = Arc::new(RedbStorage::open(&path, [0; 32]).unwrap());
    std::mem::forget(dir);
    storage
}

pub fn minimal_config() -> Config {
    Config::default()
}

pub fn config_with_requests(_default_requests_per_window: u64) -> Config {
    Config::default()
}

pub fn config_value(default_requests_per_window: u64) -> Value {
    let mut value =
        serde_json::to_value(config_with_requests(default_requests_per_window)).unwrap();
    value.as_object_mut().unwrap().insert(
        "quotas".to_owned(),
        json!({ "default_requests_per_window": default_requests_per_window }),
    );
    value
}

pub fn test_state(config: Config, storage: Option<Arc<RedbStorage>>) -> AdminState {
    let principal_view = Arc::new(PrincipalView::from_db(
        &[],
        std::collections::HashMap::new(),
    ));
    let dynamic_view = dynamic_view_holder(principal_view);
    AdminState {
        storage: storage.map(|s| s as Arc<dyn cc_lb_storage_api::Storage>),
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: LimitEngine::new(Arc::new(KeyConcurrencyManager::new())),
        lifecycle: None,
        audit_sink: None,
        dynamic_view,
        config: Arc::new(config),
        admin_token: Some(TOKEN.to_owned()),
        start_time: std::time::Instant::now(),
    }
}

pub fn test_state_without_storage() -> AdminState {
    test_state(minimal_config(), None)
}

pub fn apply_state(
    _storage: Arc<RedbStorage>,
    _config_path: PathBuf,
    reloader: Arc<TestReloader>,
) -> AdminState {
    let principal_view = Arc::new(PrincipalView::from_db(
        &[],
        std::collections::HashMap::new(),
    ));
    let dynamic_view = dynamic_view_holder(principal_view);
    AdminState {
        storage: Some(test_storage() as Arc<dyn cc_lb_storage_api::Storage>),
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: LimitEngine::new(Arc::new(KeyConcurrencyManager::new())),
        lifecycle: None,
        audit_sink: None,
        dynamic_view,
        config: reloader,
        admin_token: Some(TOKEN.to_owned()),
        start_time: std::time::Instant::now(),
    }
}

fn dynamic_view_holder(principal_view: Arc<PrincipalView>) -> Arc<DynamicViewHolder> {
    Arc::new(DynamicViewHolder::new(
        DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(NoopSignerFactory))
            .global_router(Arc::new(NoopRouter))
            .dispatcher(Arc::new(NoopDispatch))
            .global_observability_hooks(Vec::<Arc<dyn ObservabilityHook>>::new())
            .error_normalizer(Arc::new(ErrorNormalizer::new()))
            .principal_view(principal_view)
            .upstream_status_snapshot(Arc::new(UpstreamStatusSnapshot::default()))
            .build(),
    ))
}

struct NoopSignerFactory;

impl ApiKeyAwareSignerFactory for NoopSignerFactory {
    fn with_api_key(&self, _api_key: String) -> Arc<dyn SignerFactory> {
        Arc::new(NoopSignerFactory)
    }
}

#[async_trait]
impl SignerFactory for NoopSignerFactory {
    async fn build(
        &self,
        _upstream: &Upstream,
    ) -> Result<Arc<dyn cc_lb_plugin_api::Signer>, cc_lb_plugin_api::SignerError> {
        Err(cc_lb_plugin_api::SignerError::MissingCredentials {
            reason: "noop test signer factory".to_owned(),
        })
    }
}

struct NoopRouter;

impl RouterPlugin for NoopRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
    ) -> Result<RouteDecision, RouteError> {
        Err(RouteError::NoRoute {
            reason: "noop test router".to_owned(),
        })
    }
}

struct NoopDispatch;

#[async_trait]
impl UpstreamDispatch for NoopDispatch {
    async fn dispatch(
        &self,
        _request: SignedRequest,
    ) -> Result<http::Response<Body>, DispatchError> {
        Ok(http::Response::new(Body::empty()))
    }
}

pub fn app(state: AdminState) -> axum::Router {
    router(state)
}

pub async fn authed_json(
    app: axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, HeaderMap, Value, Bytes) {
    let (status, headers, body_bytes) = authed_bytes(app, method, uri, body).await;
    assert_private(&body_bytes);
    let json = serde_json::from_slice(&body_bytes).unwrap();
    (status, headers, json, body_bytes)
}

pub async fn authed_bytes(
    app: axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, HeaderMap, Bytes) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("Authorization", format!("Bearer {TOKEN}"));
    let request_body = match body {
        Some(value) => {
            builder = builder.header("content-type", "application/json");
            Body::from(serde_json::to_vec(&value).unwrap())
        }
        None => Body::empty(),
    };
    let response = app
        .oneshot(builder.body(request_body).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, headers, body)
}

pub async fn unauthenticated_status(app: axum::Router, method: &str, uri: &str) -> StatusCode {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    response.status()
}

pub fn assert_private(body: &[u8]) {
    let text = String::from_utf8_lossy(body);
    for needle in ["sk-ant", "aead_master_key"] {
        assert!(!text.contains(needle), "response leaked {needle}: {text}");
    }
}

pub fn put_body(draft: Value, expected_revision: u64) -> Value {
    json!({ "draft": draft, "expected_revision": expected_revision })
}

pub fn expected_revision_body(expected_revision: u64) -> Value {
    json!({ "expected_revision": expected_revision })
}

pub fn write_config(path: &Path, config: &Config) {
    std::fs::write(path, toml::to_string_pretty(config).unwrap()).unwrap();
}

pub struct TestReloader {
    current: Arc<RwLock<Config>>,
    draft: Arc<RwLock<Option<Config>>>,
    reloads: AtomicUsize,
}

impl TestReloader {
    pub fn new(_path: PathBuf, initial: Config) -> Self {
        Self {
            current: Arc::new(RwLock::new(initial)),
            draft: Arc::new(RwLock::new(None)),
            reloads: AtomicUsize::new(0),
        }
    }

    pub fn current(&self) -> Config {
        self.current.read().unwrap().clone()
    }

    pub fn reloads(&self) -> usize {
        self.reloads.load(Ordering::Acquire)
    }
}

impl CurrentConfig for TestReloader {
    fn current_config(&self) -> Arc<Config> {
        Arc::new(self.current())
    }

    fn put_draft_config(&self, config: Config) -> Result<(), ConfigDraftError> {
        *self.draft.write().unwrap() = Some(config);
        Ok(())
    }

    fn apply_draft_config(&self) -> Result<Arc<Config>, ConfigDraftError> {
        let config = self
            .draft
            .write()
            .unwrap()
            .take()
            .ok_or(ConfigDraftError::MissingDraft)?;
        *self.current.write().unwrap() = config.clone();
        self.reloads.fetch_add(1, Ordering::AcqRel);
        Ok(Arc::new(config))
    }
}
