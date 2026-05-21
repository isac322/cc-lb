mod reload_common;

use std::net::SocketAddr;

use cc_lb_server::reload::ConfigWatcher;

#[test]
fn restart_required_field_warns() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    let proxy_a: SocketAddr = "127.0.0.1:18080".parse().unwrap();
    let proxy_b: SocketAddr = "127.0.0.1:18081".parse().unwrap();
    reload_common::write_config(&config_path, 100, proxy_a);

    let watcher = ConfigWatcher::new(&config_path, reload_common::load_config(&config_path));
    reload_common::write_config(&config_path, 100, proxy_b);

    let logs = reload_common::capture_warn_logs(|| {
        watcher.reload_now().unwrap();
    });

    assert_eq!(watcher.current_config().listener.proxy_addr, proxy_b);
    assert!(logs.contains("listener.proxy_addr"));
    assert!(logs.contains("restart required to apply"));
}
