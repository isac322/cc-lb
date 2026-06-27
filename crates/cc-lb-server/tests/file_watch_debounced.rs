mod reload_common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use cc_lb_server::reload::ConfigWatcher;

#[tokio::test]
async fn file_watch_debounced() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    let proxy_addr: SocketAddr = "127.0.0.1:18080".parse().unwrap();
    reload_common::write_config(&config_path, 100, proxy_addr);

    let watcher = Arc::new(ConfigWatcher::new(
        &config_path,
        reload_common::load_config(&config_path),
        Arc::new(cc_lb_runtime_extism::ExtismRuntime::new()),
        Arc::new(cc_lb_core::SystemClock),
    ));
    let task = watcher.spawn_file_watcher();
    tokio::time::sleep(Duration::from_millis(300)).await;

    for value in 101..=105 {
        reload_common::write_config(&config_path, value, proxy_addr);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
    task.abort();
    let _ = task.await;

    assert_eq!(watcher.reload_attempts(), 1);
    assert_eq!(watcher.current_config().body.messages_cap_bytes, 105);
}
