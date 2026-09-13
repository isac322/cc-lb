use crate::reload_common;

use std::net::SocketAddr;
use std::sync::Arc;

use cc_lb_server::reload::ConfigWatcher;

#[test]
fn restart_required_field_warns() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    let proxy_a: SocketAddr = "127.0.0.1:18080".parse().unwrap();
    let proxy_b: SocketAddr = "127.0.0.1:18081".parse().unwrap();
    reload_common::write_config(&config_path, 100, proxy_a);

    let watcher = ConfigWatcher::new(
        &config_path,
        reload_common::load_config(&config_path),
        Arc::new(cc_lb_runtime_wasmtime::WasmtimeRuntime::with_defaults().expect("engine build")),
    );
    reload_common::write_config(&config_path, 100, proxy_b);

    let logs = reload_common::capture_warn_logs(|| {
        watcher.reload_now().unwrap();
    });

    assert_eq!(watcher.current_config().listener.proxy_addr, proxy_b);
    assert!(logs.contains("listener.proxy_addr"));
    assert!(logs.contains("restart required to apply"));
}

#[test]
fn upstream_affinity_ttl_reload_warns_with_exact_field_path() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    std::fs::write(&config_path, "[listener]\n").unwrap();
    let watcher = ConfigWatcher::new(
        &config_path,
        reload_common::load_config(&config_path),
        Arc::new(cc_lb_runtime_wasmtime::WasmtimeRuntime::with_defaults().expect("engine build")),
    );
    std::fs::write(
        &config_path,
        "[listener]\n\n[upstream_affinity]\nttl_days = 14\n",
    )
    .unwrap();

    let logs = reload_common::capture_warn_logs(|| {
        watcher.reload_now().unwrap();
    });

    assert_eq!(watcher.current_config().upstream_affinity.ttl_days, 14);
    assert!(logs.contains("upstream_affinity.ttl_days"));
    assert!(logs.contains("restart required to apply"));
}

#[test]
fn reload_does_not_warn_per_principal_path_change() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    let router_a = dir.path().join("router-a.wasm");
    let router_b = dir.path().join("router-b.wasm");
    let observe_a = dir.path().join("observe-a.wasm");
    let observe_b = dir.path().join("observe-b.wasm");
    reload_common::write_bytes(&router_a, reload_common::ROUTER_WASM);
    reload_common::write_bytes(&router_b, reload_common::ROUTER_WASM);
    reload_common::write_bytes(&observe_a, reload_common::OBSERVE_WASM);
    reload_common::write_bytes(&observe_b, reload_common::OBSERVE_WASM);
    let proxy_addr: SocketAddr = "127.0.0.1:18080".parse().unwrap();
    reload_common::write_config_with_principal_plugins(
        &config_path,
        100,
        proxy_addr,
        &router_a,
        &observe_a,
    );

    let watcher = ConfigWatcher::new(
        &config_path,
        reload_common::load_config(&config_path),
        Arc::new(cc_lb_runtime_wasmtime::WasmtimeRuntime::with_defaults().expect("engine build")),
    );
    reload_common::write_config_with_principal_plugins(
        &config_path,
        100,
        proxy_addr,
        &router_b,
        &observe_b,
    );

    let logs = reload_common::capture_warn_logs(|| {
        watcher.reload_now().unwrap();
    });

    assert!(
        !logs.contains("restart required to apply"),
        "plugin changes should not produce restart-required warnings: {logs}"
    );
}
