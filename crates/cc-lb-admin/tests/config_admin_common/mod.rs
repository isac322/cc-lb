#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use bytes::Bytes;
use cc_lb_admin::{AdminState, ConfigReloader, CurrentConfig, router};
use cc_lb_config::{Config, QuotasConfig};
use cc_lb_core::DashboardBroadcaster;
use cc_lb_storage_redb::RedbStorage;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

pub const TOKEN: &str = "test-token";

pub fn temp_storage() -> (tempfile::TempDir, Arc<RedbStorage>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(RedbStorage::open(&dir.path().join("test.redb")).unwrap());
    (dir, storage)
}

pub fn test_storage() -> Arc<RedbStorage> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.redb");
    let storage = Arc::new(RedbStorage::open(&path).unwrap());
    std::mem::forget(dir);
    storage
}

pub fn minimal_config() -> Config {
    config_with_requests(1_000)
}

pub fn config_with_requests(default_requests_per_window: u64) -> Config {
    let mut config = Config::default();
    config.signers.anthropic_oauth.scopes = Vec::new();
    config.quotas = QuotasConfig {
        default_window_secs: 60,
        default_requests_per_window,
        default_input_tokens: 1_000_000,
        default_output_tokens: 1_000_000,
    };
    config
}

pub fn config_value(default_requests_per_window: u64) -> Value {
    serde_json::to_value(config_with_requests(default_requests_per_window)).unwrap()
}

pub fn test_state(config: Config, storage: Option<Arc<RedbStorage>>) -> AdminState {
    AdminState {
        storage: storage.unwrap_or_else(test_storage),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        quota_manager: None,
        lifecycle: None,
        breaker_registry: None,
        drain_controller: None,
        bulkhead_registry: None,
        plugin_runtime_status: None,
        dashboard_broadcaster: Arc::new(DashboardBroadcaster::new()),
        config: Arc::new(config),
        config_path: None,
        config_watcher: None,
        config_started_at_unix_secs: 0,
        admin_token: Some(TOKEN.to_owned()),
        start_time: std::time::Instant::now(),
    }
}

pub fn test_state_without_storage() -> AdminState {
    test_state(minimal_config(), None)
}

pub fn apply_state(
    storage: Arc<RedbStorage>,
    config_path: PathBuf,
    reloader: Arc<TestReloader>,
) -> AdminState {
    let config: Arc<dyn CurrentConfig> = reloader.clone();
    let config_watcher: Arc<dyn ConfigReloader> = reloader;
    AdminState {
        storage,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        quota_manager: None,
        lifecycle: None,
        breaker_registry: None,
        drain_controller: None,
        bulkhead_registry: None,
        plugin_runtime_status: None,
        dashboard_broadcaster: Arc::new(DashboardBroadcaster::new()),
        config,
        config_path: Some(config_path),
        config_watcher: Some(config_watcher),
        config_started_at_unix_secs: 0,
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
    for needle in [
        "\"messages\"",
        "\"system\"",
        "\"tools\"",
        "\"tool_use\"",
        "\"content\"",
    ] {
        assert!(!text.contains(needle), "response leaked {needle}: {text}");
    }
    for needle in ["sk-ant", "aead_master_key"] {
        assert!(!text.contains(needle), "response leaked {needle}: {text}");
    }
}

pub fn put_body(draft: Value, expected_revision: u64) -> Value {
    json!({
        "draft": draft,
        "expected_revision": expected_revision,
    })
}

pub fn expected_revision_body(expected_revision: u64) -> Value {
    json!({ "expected_revision": expected_revision })
}

pub fn write_config(path: &Path, config: &Config) {
    std::fs::write(path, toml::to_string_pretty(config).unwrap()).unwrap();
}

pub struct TestReloader {
    path: PathBuf,
    current: Arc<RwLock<Config>>,
    reloads: AtomicUsize,
}

impl TestReloader {
    pub fn new(path: PathBuf, initial: Config) -> Self {
        Self {
            path,
            current: Arc::new(RwLock::new(initial)),
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
}

impl ConfigReloader for TestReloader {
    fn reload_now(&self) -> Result<(), String> {
        let config = Config::load(&self.path).map_err(|source| source.to_string())?;
        *self.current.write().unwrap() = config;
        self.reloads.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}
