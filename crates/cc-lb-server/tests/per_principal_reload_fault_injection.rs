mod reload_common;

use std::net::SocketAddr;
use std::sync::Arc;

use cc_lb_admin::CurrentConfig;
use cc_lb_server::reload::ConfigWatcher;

#[test]
fn config_reload_is_storage_driven_and_keeps_runtime_view() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    let router_path = dir.path().join("router.wasm");
    let bad_router_path = dir.path().join("bad-charlie-router.wasm");
    reload_common::write_bytes(&router_path, reload_common::ROUTER_WASM);
    reload_common::write_bytes(&bad_router_path, b"not wasm");
    let proxy_addr: SocketAddr = "127.0.0.1:18080".parse().unwrap();
    write_config_with_principal_routers(&config_path, proxy_addr, &router_path, None);

    let initial_config = reload_common::load_config(&config_path);
    let runtime =
        Arc::new(cc_lb_runtime_wasmtime::WasmtimeRuntime::with_defaults().expect("engine build"));
    let dynamic_view = reload_common::dynamic_view_holder(&initial_config);
    let watcher = ConfigWatcher::new_with_principal_view(
        &config_path,
        initial_config,
        runtime.clone(),
        Some(dynamic_view.clone()),
        Arc::new(cc_lb_core::SystemClock),
    );
    watcher
        .reload_now()
        .expect("initial config-only reload succeeds");
    let previous_view = dynamic_view.load();
    let pre_reload_slot_count = runtime.slot_count();
    assert_eq!(pre_reload_slot_count, 0);

    write_config_with_principal_routers(
        &config_path,
        proxy_addr,
        &router_path,
        Some(&bad_router_path),
    );

    let reload_result = watcher.reload_now();

    assert!(reload_result.is_ok());
    assert_eq!(watcher.current_config().body.messages_cap_bytes, 200);
    assert!(Arc::ptr_eq(&previous_view, &dynamic_view.load()));
    let loaded_view = dynamic_view.load().principal_view.clone();
    assert!(loaded_view.get("alice").is_none());
    assert!(loaded_view.get("bob").is_none());
    assert!(loaded_view.get("charlie").is_none());
    let status = watcher
        .last_reload_status()
        .expect("successful reload status is recorded");
    assert_eq!(
        status.config_path.as_deref(),
        Some(config_path.to_str().unwrap())
    );
    assert!(matches!(
        status.outcome,
        cc_lb_admin::ReloadOutcome::Success
    ));
    assert_eq!(runtime.slot_count(), pre_reload_slot_count);
}

fn write_config_with_principal_routers(
    path: &std::path::Path,
    proxy_addr: SocketAddr,
    _router_path: &std::path::Path,
    charlie_router_path: Option<&std::path::Path>,
) {
    let messages_cap_bytes = if charlie_router_path.is_some() {
        200
    } else {
        100
    };
    let config = format!(
        r#"[listener]
proxy_addr = "{proxy_addr}"
admin_addr = "127.0.0.1:19090"
metrics_addr = "127.0.0.1:19091"

[body]
messages_cap_bytes = {messages_cap_bytes}
files_cap_bytes = 1048576

[downstream_auth]
mode = "none"

[downstream_auth.none_mode]
principal_id = "alice"
upstream_kind = "anthropic_key"
"#
    );
    std::fs::write(path, config).unwrap();
}
