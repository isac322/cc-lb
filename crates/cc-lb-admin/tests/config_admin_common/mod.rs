#![allow(dead_code, deprecated)]

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use bytes::Bytes;
use cc_lb_admin::{AdminState, router};
use cc_lb_clock::{ClockHandle, SystemClock};
use cc_lb_config::Config;
use cc_lb_control::api_keys::{
    concurrent_guard::KeyConcurrencyManager, key_store::KeyStore, limit_engine::LimitEngine,
    principal_view::PrincipalView,
};
use cc_lb_control::{
    DynamicViewBuilder, DynamicViewHolder, RouteDecision, RouteError, RouterPlugin, RoutingContext,
    UpstreamStatusSnapshot,
};
use cc_lb_domain::{Principal, Upstream, UpstreamCandidate};
use cc_lb_observability::ObservabilityHook;
use cc_lb_storage_api::{BackendKind, MetaStore};
use cc_lb_storage_sqlite::SqliteStorage;
use cc_lb_upstream::{ApiKeyAwareSignerFactory, SignerFactory};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

pub const TOKEN: &str = "test-token";

pub async fn temp_storage() -> (tempfile::TempDir, Arc<SqliteStorage>) {
    temp_storage_with_clock(system_clock()).await
}

pub async fn temp_storage_with_clock(
    clock: ClockHandle,
) -> (tempfile::TempDir, Arc<SqliteStorage>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = open_storage(dir.path(), "test.sqlite", clock).await;
    (dir, storage)
}

pub async fn test_storage() -> Arc<SqliteStorage> {
    test_storage_with_clock(system_clock()).await
}

pub async fn test_storage_with_clock(clock: ClockHandle) -> Arc<SqliteStorage> {
    let dir = tempfile::tempdir().unwrap();
    let storage = open_storage(dir.path(), "test.sqlite", clock).await;
    std::mem::forget(dir);
    storage
}

pub fn key_store(storage: Arc<SqliteStorage>) -> Arc<KeyStore> {
    Arc::new(KeyStore::new(storage))
}

async fn open_storage(dir: &Path, filename: &str, clock: ClockHandle) -> Arc<SqliteStorage> {
    let database_url = format!("sqlite://{}", dir.join(filename).display());
    let storage = cc_lb_storage_sqlite::open_sqlite(&database_url, clock)
        .await
        .unwrap();
    storage.initialize(BackendKind::Sqlite).await.unwrap();
    Arc::new(storage)
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
    value["timeouts"]["upstream_total_secs"] = json!(default_requests_per_window);
    value
}

pub fn test_state(config: Config, storage: Option<Arc<SqliteStorage>>) -> AdminState {
    test_state_with_clock(config, storage, system_clock())
}

pub fn test_state_with_clock(
    config: Config,
    storage: Option<Arc<SqliteStorage>>,
    clock: ClockHandle,
) -> AdminState {
    let principal_view = Arc::new(PrincipalView::from_db(
        &[],
        std::collections::HashMap::new(),
    ));
    let dynamic_view = dynamic_view_holder(principal_view);
    let key_store = storage.clone().map(key_store);
    let event_bus: Option<Arc<dyn cc_lb_control::RequestEventBus>> = storage.as_ref().map(|_| {
        Arc::new(cc_lb_control::InMemoryBus::new()) as Arc<dyn cc_lb_control::RequestEventBus>
    });
    AdminState {
        config_path: None,
        storage: storage.map(|s| s as Arc<dyn cc_lb_storage_api::Storage>),
        key_store,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: LimitEngine::new(Arc::new(KeyConcurrencyManager::new()), clock.clone()),
        lifecycle: None,
        dynamic_view,
        config: Arc::new(config),
        scheduler: None,
        admin_auth: crate::admin_test_common::static_token_auth(TOKEN),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        subscription_metadata_hook: None,
        start_time: std::time::Instant::now(),
        event_bus,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock,
    }
}

fn system_clock() -> ClockHandle {
    Arc::new(SystemClock)
}

pub fn test_state_without_storage() -> AdminState {
    test_state(minimal_config(), None)
}

fn dynamic_view_holder(principal_view: Arc<PrincipalView>) -> Arc<DynamicViewHolder> {
    Arc::new(DynamicViewHolder::new(
        DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(NoopSignerFactory))
            .global_router(Arc::new(NoopRouter))
            .global_observability_hooks(Vec::<Arc<dyn ObservabilityHook>>::new())
            .principal_view(principal_view)
            .upstream_status_snapshot(Arc::new(UpstreamStatusSnapshot::default()))
            .build(),
    ))
}

struct NoopSignerFactory;

impl ApiKeyAwareSignerFactory for NoopSignerFactory {
    fn with_router_choice(
        &self,
        _api_key: String,
        _router_chosen_upstream_name: String,
    ) -> Arc<dyn SignerFactory> {
        Arc::new(NoopSignerFactory)
    }
}

#[async_trait]
impl SignerFactory for NoopSignerFactory {
    async fn build(
        &self,
        _upstream: &Upstream,
    ) -> Result<Arc<dyn cc_lb_upstream::Signer>, cc_lb_upstream::SignerError> {
        Err(cc_lb_upstream::SignerError::MissingCredentials {
            reason: "noop test signer factory".to_owned(),
        })
    }
}

struct NoopRouter;

impl RouterPlugin for NoopRouter {
    fn route(
        &self,
        _ctx: &RoutingContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        Err(RouteError::NoRoute {
            reason: "noop test router".to_owned(),
        })
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
    authed_bytes_with_headers(app, method, uri, body, &[]).await
}

pub async fn authed_bytes_with_headers(
    app: axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
    headers: &[(&str, &str)],
) -> (StatusCode, HeaderMap, Bytes) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("Authorization", format!("Bearer {TOKEN}"));
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
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
