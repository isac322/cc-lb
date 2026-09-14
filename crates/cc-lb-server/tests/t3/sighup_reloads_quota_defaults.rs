use crate::reload_common;

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
async fn t3__sighup_reloads_body_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    let proxy_addr: SocketAddr = "127.0.0.1:18080".parse().unwrap();
    reload_common::write_config(&config_path, 100, proxy_addr);

    let watcher = Arc::new(ConfigWatcher::new(
        &config_path,
        reload_common::load_config(&config_path),
        Arc::new(cc_lb_runtime_wasmtime::WasmtimeRuntime::with_defaults().expect("engine build")),
    ));
    let app = cc_lb_admin::router(AdminState {
        storage: None,
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: cc_lb_engine::api_keys::limit_engine::LimitEngine::new(
            Arc::new(cc_lb_engine::api_keys::concurrent_guard::KeyConcurrencyManager::new()),
            cc_lb_testkit::fixed_clock(1_700_000_000),
        ),
        lifecycle: None,
        dynamic_view: reload_common::dynamic_view_holder(
            &cc_lb_admin::CurrentConfig::current_config((watcher.clone()).as_ref()),
        ),
        config: watcher.clone(),
        scheduler: None,
        admin_auth: Arc::new(cc_lb_admin::auth::AdminAuthenticator::new(
            cc_lb_admin::auth::build_providers(
                &cc_lb_config::AdminAuthConfig::default(),
                Some("test-token".to_owned()),
            )
            .expect("test admin auth builds"),
        )),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        subscription_metadata_hook: None,
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        start_time: std::time::Instant::now(),
        clock: cc_lb_testkit::fixed_clock(1_700_000_000),
    });
    let before_admin = admin_config(app.clone()).await;
    assert_eq!(before_admin["body"]["messages_cap_bytes"], json!(100));

    reload_common::write_config(&config_path, 200, proxy_addr);
    watcher.reload_now().unwrap();

    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if watcher.current_config().body.messages_cap_bytes == 200 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "body default did not reload to 200"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let after_admin = admin_config(app).await;
    let after = watcher.current_config();
    let evidence = json!({
        "before": {
            "admin_config_current": before_admin,
            "body.messages_cap_bytes": 100,
        },
        "trigger": "ConfigWatcher::reload_now()",
        "after": {
            "admin_config_current": after_admin,
            "body.messages_cap_bytes": after.body.messages_cap_bytes,
        }
    });
    std::fs::write(
        reload_common::evidence_path("task-31-reload-effect.json"),
        serde_json::to_string_pretty(&evidence).unwrap(),
    )
    .unwrap();

    assert_eq!(after.body.messages_cap_bytes, 200);
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
