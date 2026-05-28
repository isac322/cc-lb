mod reload_common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwap;
use cc_lb_admin::{CurrentConfig, ReloadOutcome};
use cc_lb_core::api_keys::principal_view::PrincipalView;
use cc_lb_server::reload::ConfigWatcher;

#[test]
fn per_principal_instantiation_failure_aborts_reload() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    let router_path = dir.path().join("router.wasm");
    let bad_router_path = dir.path().join("bad-charlie-router.wasm");
    reload_common::write_bytes(&router_path, reload_common::ROUTER_WASM);
    reload_common::write_bytes(&bad_router_path, b"not wasm");
    let proxy_addr: SocketAddr = "127.0.0.1:18080".parse().unwrap();
    write_config_with_principal_routers(&config_path, proxy_addr, &router_path, None);

    let initial_config = reload_common::load_config(&config_path);
    let runtime = Arc::new(cc_lb_runtime_extism::ExtismRuntime::new());
    let principal_view = Arc::new(ArcSwap::from(
        PrincipalView::from_config(&initial_config, HashMap::new()).expect("principal view builds"),
    ));
    let watcher = ConfigWatcher::new_with_principal_view(
        &config_path,
        initial_config,
        runtime.clone(),
        Some(principal_view.clone()),
    );
    watcher
        .reload_now()
        .expect("initial reload stages principal plugin slots");
    let previous_view = principal_view.load_full();
    let mut pre_reload_keys = runtime.registered_slot_keys();
    pre_reload_keys.sort();
    assert_eq!(
        pre_reload_keys,
        vec![
            ("alice".to_owned(), "alice-router".to_owned()),
            ("bob".to_owned(), "bob-router".to_owned()),
        ]
    );

    write_config_with_principal_routers(
        &config_path,
        proxy_addr,
        &router_path,
        Some(&bad_router_path),
    );

    let reload_result = watcher.reload_now();

    assert!(
        reload_result.is_err(),
        "reload must fail when charlie's plugin artifact is invalid"
    );
    assert_eq!(watcher.current_config().body.messages_cap_bytes, 100);
    assert!(Arc::ptr_eq(&previous_view, &principal_view.load_full()));
    let loaded_view = principal_view.load();
    assert!(loaded_view.get("alice").is_some());
    assert!(loaded_view.get("bob").is_some());
    assert!(loaded_view.get("charlie").is_none());
    let status = watcher
        .last_reload_status()
        .expect("failed reload status is recorded");
    assert_eq!(
        status.config_path.as_deref(),
        Some(config_path.to_str().unwrap())
    );
    match status.outcome {
        ReloadOutcome::Failure {
            reason,
            principal,
            plugin,
        } => {
            assert_eq!(principal.as_deref(), Some("charlie"));
            assert_eq!(plugin.as_deref(), Some("charlie-router"));
            assert!(
                reason.contains("bad-charlie-router.wasm")
                    || reason.contains("Wasm")
                    || reason.contains("wasm")
                    || reason.contains("plugin"),
                "unexpected failure reason: {reason}"
            );
        }
        ReloadOutcome::Success => panic!("invalid reload must record failure"),
    }
    let mut post_reload_keys = runtime.registered_slot_keys();
    post_reload_keys.sort();
    assert_eq!(post_reload_keys, pre_reload_keys);
}

fn write_config_with_principal_routers(
    path: &Path,
    proxy_addr: SocketAddr,
    router_path: &Path,
    charlie_router_path: Option<&Path>,
) {
    let router_path = reload_common::toml_path(router_path);
    let messages_cap_bytes = if charlie_router_path.is_some() {
        200
    } else {
        100
    };
    let charlie_config = charlie_router_path
        .map(|path| {
            let path = reload_common::toml_path(path);
            format!(
                r#"
[principals.charlie]
allowed_models = ["*"]

[principals.charlie.router_plugin]
name = "charlie-router"
wasm_path = "{path}"
"#
            )
        })
        .unwrap_or_default();
    let config = format!(
        r#"[listener]
proxy_addr = "{proxy_addr}"
admin_addr = "127.0.0.1:19090"
metrics_addr = "127.0.0.1:19091"

[body]
messages_cap_bytes = {messages_cap_bytes}
files_cap_bytes = 1048576

[upstreams.fake]
kind = "custom"
base_url = "http://upstream.local/"
auth_strategy = "api_key"

[downstream_auth]
mode = "none"

[downstream_auth.none_mode]
principal_id = "alice"
upstream_kind = "anthropic_key"
upstream_credential_ref = "test-upstream"

[principals.alice]
allowed_models = ["*"]

[principals.alice.router_plugin]
name = "alice-router"
wasm_path = "{router_path}"

[principals.bob]
allowed_models = ["*"]

[principals.bob.router_plugin]
name = "bob-router"
wasm_path = "{router_path}"
{charlie_config}"#
    );
    std::fs::write(path, config).unwrap();
}
