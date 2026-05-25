use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{router, AdminState};
use cc_lb_config::Config;
use cc_lb_storage_redb::Storage;
use http_body_util::BodyExt;
use std::sync::Arc;
use tower::ServiceExt;

fn test_state(storage: Arc<Storage>) -> AdminState {
    AdminState {
        storage: Some(storage),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: cc_lb_core::api_keys::limit_engine::LimitEngine::new(
            Arc::new(cc_lb_core::api_keys::concurrent_guard::KeyConcurrencyManager::new()),
            Arc::new(arc_swap::ArcSwap::from(
                cc_lb_core::api_keys::principal_view::PrincipalView::from_config(&Config::default()),
            )),
        ),
        lifecycle: None,
        audit_sink: None,
        principal_view: Arc::new(arc_swap::ArcSwap::from(
            cc_lb_core::api_keys::principal_view::PrincipalView::from_config(
                &cc_lb_admin::CurrentConfig::current_config((Arc::new(Config::default())).as_ref()),
            ),
        )),
        config: Arc::new(Config::default()),
        admin_token: Some("test-token".to_string()),
        start_time: std::time::Instant::now(),
    }
}

#[tokio::test]
async fn test_killswitch_persists() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.redb");
    let master_key = [0u8; 32];

    {
        let storage = Arc::new(Storage::open(&db_path, master_key).unwrap());
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

        assert!(storage.killswitch_enabled().unwrap());
    }

    {
        let storage = Arc::new(Storage::open(&db_path, master_key).unwrap());
        assert!(storage.killswitch_enabled().unwrap());

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

        assert!(!storage.killswitch_enabled().unwrap());
    }
}
