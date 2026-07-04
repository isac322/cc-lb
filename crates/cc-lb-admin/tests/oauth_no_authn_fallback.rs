mod admin_test_common;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use cc_lb_engine::api_keys::key_store::KeyStore;
use cc_lb_storage_sqlite::SqliteStorage as Storage;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tower::ServiceExt;

fn test_state(storage: Arc<Storage>) -> AdminState {
    let config = Config::default();
    AdminState {
        storage: Some(storage.clone()),
        key_store: Some(Arc::new(KeyStore::new(storage.clone()))),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        audit_sink: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(Config::default()),
        scheduler: None,
        admin_token: Some("test-token".to_string()),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        subscription_metadata_hook: None,
        start_time: Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock: Arc::new(cc_lb_engine::SystemClock),
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
        .uri("/admin/config/current")
        .header("Authorization", "Bearer wrong-token")
        .body(Body::empty())
        .unwrap();

    let started = Instant::now();
    let response = app.oneshot(req).await.unwrap();
    let elapsed = started.elapsed();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(elapsed >= Duration::from_millis(100));
}
