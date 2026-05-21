mod reload_common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use cc_lb_admin::AdminState;
use cc_lb_server::reload::ConfigWatcher;
use http_body_util::BodyExt;
use serde_json::json;
use tower::ServiceExt;

#[tokio::test]
async fn sighup_reloads_quota_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    let proxy_addr: SocketAddr = "127.0.0.1:18080".parse().unwrap();
    reload_common::write_config(&config_path, 100, proxy_addr);

    let watcher = Arc::new(ConfigWatcher::new(
        &config_path,
        reload_common::load_config(&config_path),
    ));
    let app = cc_lb_admin::router(AdminState {
        storage: None,
        quota_manager: None,
        lifecycle: None,
        config: watcher.clone(),
        admin_token: Some("test-token".to_string()),
        start_time: std::time::Instant::now(),
    });
    let before_admin = admin_config(app.clone()).await;
    assert_eq!(
        before_admin["quotas"]["default_requests_per_window"],
        json!(100)
    );

    reload_common::write_config(&config_path, 200, proxy_addr);
    watcher.reload_now().unwrap();

    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if watcher.current_config().quotas.default_requests_per_window == 200 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "quota default did not reload to 200"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let after_admin = admin_config(app).await;
    let after = watcher.current_config();
    let evidence = json!({
        "before": {
            "admin_config_current": before_admin,
            "quotas.default_requests_per_window": 100,
        },
        "trigger": "ConfigWatcher::reload_now()",
        "after": {
            "admin_config_current": after_admin,
            "quotas.default_requests_per_window": after.quotas.default_requests_per_window,
        }
    });
    std::fs::write(
        reload_common::evidence_path("task-31-reload-effect.json"),
        serde_json::to_string_pretty(&evidence).unwrap(),
    )
    .unwrap();

    assert_eq!(after.quotas.default_requests_per_window, 200);
}

async fn admin_config(app: axum::Router) -> serde_json::Value {
    let response = app
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
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}
