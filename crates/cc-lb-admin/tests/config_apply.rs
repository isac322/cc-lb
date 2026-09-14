use crate::admin_test_common;
use crate::config_admin_common::{
    TestReloader, app, apply_state, authed_json, config_value, minimal_config, put_body,
    temp_storage,
};

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use tower::ServiceExt;

fn test_state() -> AdminState {
    let config = Config::default();
    AdminState {
        storage: None,
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(config),
        scheduler: None,
        admin_auth: crate::admin_test_common::static_token_auth("test-token"),
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
async fn config_apply_current_admin_config_smoke() {
    let response = router(test_state())
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/config/current")
                .header("Authorization", "Bearer test-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn config_apply_audit_records_static_token_actor_identity() {
    let (dir, storage) = temp_storage().await;
    let config_path = dir.path().join("cc-lb.toml");
    let reloader = Arc::new(TestReloader::new(config_path.clone(), minimal_config()));
    let app = app(apply_state(storage, config_path, reloader).await);

    let (status, _, draft, _) = authed_json(
        app.clone(),
        "PUT",
        "/admin/v1/config/draft",
        Some(put_body(config_value(180), 0)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(draft["revision"], 1);

    let (status, _, applied, _) =
        authed_json(app.clone(), "POST", "/admin/v1/config/apply", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(applied["status"], "applied");

    let (status, _, audit, _) = authed_json(app, "GET", "/admin/v1/audit", None).await;
    assert_eq!(status, StatusCode::OK);
    let entry = audit["entries"]
        .as_array()
        .expect("audit entries array")
        .iter()
        .find(|entry| entry["admin_action"] == "config_apply")
        .expect("config apply audit entry");
    assert_eq!(entry["actor_authority"], "static-token");
    assert_eq!(entry["actor_subject"], "test-static-token");
    assert_eq!(entry["actor_kind"], "break_glass");
}
