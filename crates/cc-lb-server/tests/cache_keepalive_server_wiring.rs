use crate::common;

use std::sync::Arc;

use cc_lb_config::Config;
use cc_lb_server::app::build_app_for_testing;

#[tokio::test]
async fn cache_keepalive_disabled_principal_builds_without_scheduler_dependency_cycle()
-> Result<(), Box<dyn std::error::Error>> {
    let app = build_app_for_testing(Config::default(), Arc::new(cc_lb_engine::SystemClock)).await?;

    assert_eq!(
        app.proxy_addr.port(),
        Config::default().listener.proxy_addr.port()
    );
    Ok(())
}

#[tokio::test]
async fn spawned_proxy_handles_cache_control_request_with_temp_sqlite() {
    let server = common::spawn_test_server().await;
    let body = r#"{"model":"claude-3-5-sonnet-20241022","max_tokens":10,"system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral","ttl":"5m"}}],"messages":[{"role":"user","content":"hi"}]}"#;

    let response = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        &server.managed_key.plaintext,
        body,
        &[],
    )
    .await
    .expect("post cache-control messages through spawned proxy");

    assert_eq!(response.status, 200);
    assert!(response.body.contains(r#""type":"message""#));
    assert!(server.sqlite_path.exists());
}

#[tokio::test]
async fn spawned_proxy_handles_no_cache_control_request_with_temp_sqlite() {
    let server = common::spawn_test_server().await;
    let body = r#"{"model":"claude-3-5-sonnet-20241022","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}"#;

    let response = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        &server.managed_key.plaintext,
        body,
        &[],
    )
    .await
    .expect("post no-cache-control messages through spawned proxy");

    assert_eq!(response.status, 200);
    assert!(response.body.contains(r#""type":"message""#));
    assert!(server.sqlite_path.exists());
}
