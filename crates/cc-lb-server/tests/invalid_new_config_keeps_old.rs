mod reload_common;

use std::net::SocketAddr;
use std::sync::Arc;

use cc_lb_server::reload::ConfigWatcher;

#[test]
fn invalid_new_config_keeps_old() {
    let handle = reload_common::install_prometheus();
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    let proxy_addr: SocketAddr = "127.0.0.1:18080".parse().unwrap();
    reload_common::write_config(&config_path, 100, proxy_addr);

    let watcher = ConfigWatcher::new(
        &config_path,
        reload_common::load_config(&config_path),
        Arc::new(cc_lb_runtime_wasmtime::WasmtimeRuntime::with_defaults().expect("engine build")),
        Arc::new(cc_lb_core::SystemClock),
    );
    let before_failed = reload_common::counter_value(&handle, "cc_lb_config_reload_failed_total");
    std::fs::write(&config_path, "[listener\nthis is not valid toml").unwrap();

    let logs = reload_common::capture_warn_logs(|| {
        assert!(watcher.reload_now().is_err());
    });

    let after_failed = reload_common::counter_value(&handle, "cc_lb_config_reload_failed_total");
    let failure_delta = after_failed - before_failed;
    assert_eq!(failure_delta, 1.0);
    assert_eq!(
        reload_common::labeled_counter_value(
            &handle,
            "cc_lb_config_reload_total",
            "outcome",
            "failure"
        ),
        1.0
    );
    assert_eq!(watcher.current_config().body.messages_cap_bytes, 100);
    assert!(logs.contains("configuration reload failed"));

    let evidence = format!(
        "bad TOML reload rejected\nwarn_log={logs}\nfailed_counter_before={before_failed}\nfailed_counter_after={after_failed}\nfailed_counter_delta={failure_delta}\ncurrent_messages_cap_bytes={}\n",
        watcher.current_config().body.messages_cap_bytes
    );
    std::fs::write(
        reload_common::evidence_path("task-31-bad-reload-rejected.log"),
        evidence,
    )
    .unwrap();
}

#[test]
fn config_reload_accepts_config_only_change_and_keeps_runtime_view() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    let proxy_addr: SocketAddr = "127.0.0.1:18080".parse().unwrap();
    reload_common::write_config_with_principal_model(&config_path, 100, proxy_addr, "*");

    let initial_config = reload_common::load_config(&config_path);
    let dynamic_view = reload_common::dynamic_view_holder(&initial_config);
    let before_view = dynamic_view.load();
    let watcher = ConfigWatcher::new_with_principal_view(
        &config_path,
        initial_config,
        Arc::new(cc_lb_runtime_wasmtime::WasmtimeRuntime::with_defaults().expect("engine build")),
        Some(dynamic_view.clone()),
        Arc::new(cc_lb_core::SystemClock),
    );
    reload_common::write_config_with_principal_model(&config_path, 200, proxy_addr, "[");

    let logs = reload_common::capture_warn_logs(|| {
        assert!(watcher.reload_now().is_ok());
    });

    assert_eq!(watcher.current_config().body.messages_cap_bytes, 200);
    assert!(Arc::ptr_eq(&before_view, &dynamic_view.load()));
    assert!(!logs.contains("configuration reload failed"));
}

#[test]
fn config_reload_accepts_plugin_unrelated_change_and_records_success() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    let router_path = dir.path().join("router.wasm");
    let observe_path = dir.path().join("observe.wasm");
    let bad_router_path = dir.path().join("bad-router.wasm");
    reload_common::write_bytes(&router_path, reload_common::ROUTER_WASM);
    reload_common::write_bytes(&observe_path, reload_common::OBSERVE_WASM);
    reload_common::write_bytes(&bad_router_path, b"not wasm");
    let proxy_addr: SocketAddr = "127.0.0.1:18080".parse().unwrap();
    reload_common::write_config_with_principal_plugins(
        &config_path,
        100,
        proxy_addr,
        &router_path,
        &observe_path,
    );

    let initial_config = reload_common::load_config(&config_path);
    let dynamic_view = reload_common::dynamic_view_holder(&initial_config);
    let before_view = dynamic_view.load();
    let watcher = ConfigWatcher::new_with_principal_view(
        &config_path,
        initial_config,
        Arc::new(cc_lb_runtime_wasmtime::WasmtimeRuntime::with_defaults().expect("engine build")),
        Some(dynamic_view.clone()),
        Arc::new(cc_lb_core::SystemClock),
    );
    reload_common::write_config_with_principal_plugins(
        &config_path,
        200,
        proxy_addr,
        &bad_router_path,
        &observe_path,
    );

    let logs = reload_common::capture_warn_logs(|| {
        assert!(watcher.reload_now().is_ok());
    });

    assert_eq!(watcher.current_config().body.messages_cap_bytes, 200);
    assert!(Arc::ptr_eq(&before_view, &dynamic_view.load()));
    let status = <ConfigWatcher as cc_lb_admin::CurrentConfig>::last_reload_status(&watcher)
        .expect("successful reload status is recorded");
    assert_eq!(
        status.config_path.as_deref(),
        Some(config_path.to_str().unwrap())
    );
    assert!(matches!(
        status.outcome,
        cc_lb_admin::ReloadOutcome::Success
    ));
    assert!(!logs.contains("configuration reload failed"));
}
