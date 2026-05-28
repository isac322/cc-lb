#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use arc_swap::ArcSwap;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use bytes::Bytes;
use cc_lb_admin::{AdminState, ConfigDraftError, CurrentConfig, router};
use cc_lb_config::Config;
use cc_lb_core::api_keys::{
    concurrent_guard::KeyConcurrencyManager, limit_engine::LimitEngine,
    principal_view::PrincipalView,
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
    let principal_view = Arc::new(ArcSwap::from(
        PrincipalView::from_config(&config, std::collections::HashMap::new())
            .expect("principal view builds"),
    ));
    AdminState {
        storage: storage.map(|s| s as Arc<dyn cc_lb_storage_api::Storage>),
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            principal_view.clone(),
        ),
        lifecycle: None,
        audit_sink: None,
        principal_view,
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
    let config = reloader.current();
    let principal_view = Arc::new(ArcSwap::from(
        PrincipalView::from_config(&config, std::collections::HashMap::new())
            .expect("principal view builds"),
    ));
    AdminState {
        storage: Some(test_storage() as Arc<dyn cc_lb_storage_api::Storage>),
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            principal_view.clone(),
        ),
        lifecycle: None,
        audit_sink: None,
        principal_view,
        config: reloader,
        admin_token: Some(TOKEN.to_owned()),
        start_time: std::time::Instant::now(),
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
