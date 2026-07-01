use crate::admin_test_common;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use cc_lb_storage_api::{BackendKind, MetaStore};
use cc_lb_storage_sqlite::SqliteStorage as Storage;
use http_body_util::BodyExt;
use std::sync::Arc;
use tower::ServiceExt;

fn test_state(storage: Arc<Storage>) -> AdminState {
    let config = Config::default();
    AdminState {
        storage: Some(storage.clone()),
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        audit_sink: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(Config::default()),
        config_reloader: None,
        scheduler: None,
        admin_token: Some("test-token".to_string()),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        subscription_metadata_hook: None,
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock: Arc::new(cc_lb_clock::SystemClock),
    }
}

#[tokio::test]
async fn test_killswitch_persists() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.sqlite");

    {
        let storage = open_storage(&db_path).await;
        let app = router(test_state(storage.clone()));

        let req = Request::builder()
            .method("POST")
            .uri("/admin/killswitch")
            .header("Authorization", "Bearer test-token")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["killswitch"], true);

        assert!(storage.killswitch_enabled().await.unwrap());
    }

    {
        let storage = open_storage(&db_path).await;
        assert!(storage.killswitch_enabled().await.unwrap());

        let app = router(test_state(storage.clone()));

        let req = Request::builder()
            .method("DELETE")
            .uri("/admin/killswitch")
            .header("Authorization", "Bearer test-token")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["killswitch"], false);

        assert!(!storage.killswitch_enabled().await.unwrap());
    }
}

async fn open_storage(path: &std::path::Path) -> Arc<Storage> {
    let database_url = format!("sqlite://{}", path.display());
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .unwrap();
    storage.initialize(BackendKind::Sqlite).await.unwrap();
    Arc::new(storage)
}
