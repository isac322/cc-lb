mod common;

use std::fs;
use std::time::{Duration, Instant};

use cc_lb_config::Config;
use tokio::sync::mpsc;
use tokio::time::timeout;

#[tokio::test(flavor = "current_thread")]
async fn hot_reload_sends_valid_changes_and_skips_invalid_configs() {
    let (_dir, path) = common::temp_config(
        r#"[listener]
proxy_addr = "[::]:8080"
"#,
    );
    let (tx, mut rx) = mpsc::channel(4);
    let handle = Config::watch_for_reload(&path, tx);

    tokio::time::sleep(Duration::from_millis(200)).await;
    fs::write(
        &path,
        r#"[listener]
proxy_addr = "[::]:8181"
"#,
    )
    .unwrap();

    let started = Instant::now();
    let config = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    println!("reload received after {}ms", started.elapsed().as_millis());

    assert_eq!(config.listener.proxy_addr, "[::]:8181".parse().unwrap());

    fs::write(
        &path,
        r#"[tls]
cert_path = "/definitely/missing/cert.pem"
"#,
    )
    .unwrap();

    let invalid = timeout(Duration::from_millis(900), rx.recv()).await;
    assert!(invalid.is_err(), "invalid config was delivered");

    handle.abort();
    let _ = handle.await;
}
