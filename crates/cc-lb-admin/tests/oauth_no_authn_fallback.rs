use crate::admin_test_common;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use cc_lb_control::api_keys::key_store::KeyStore;
use cc_lb_storage_sqlite::SqliteStorage as Storage;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tower::ServiceExt;

fn test_state(storage: Arc<Storage>) -> AdminState {
    let config = Config::default();
    AdminState {
        config_path: None,
        startup_config_overrides: Default::default(),
        storage: Some(storage.clone()),
        key_store: Some(Arc::new(KeyStore::new(storage.clone()))),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(config),
        dynamic_view_rebinder: None,
        scheduler: None,
        admin_auth: crate::admin_test_common::static_token_auth("test-token"),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        subscription_metadata_hook: None,
        start_time: Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock: Arc::new(cc_lb_clock::SystemClock),
    }
}

async fn new_storage() -> (tempfile::TempDir, Arc<Storage>) {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        admin_test_common::sqlite_storage(dir.path(), "oauth_no_authn_fallback.sqlite").await;
    (dir, storage)
}

#[tokio::test]
async fn admin_401_sleeps_100ms() {
    let (_dir, storage) = new_storage().await;
    let app = router(test_state(storage));

    let req = Request::builder()
        .method("GET")
        .uri("/admin/v1/config/editor")
        .header("Authorization", "Bearer wrong-token")
        .body(Body::empty())
        .unwrap();

    let started = Instant::now();
    let response = app.oneshot(req).await.unwrap();
    let elapsed = started.elapsed();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(elapsed >= Duration::from_millis(100));
}
