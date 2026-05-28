mod reload_common;

use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use cc_lb_core::api_keys::principal_view::PrincipalView;
use cc_lb_server::reload::ConfigWatcher;

#[test]
fn invalid_new_config_keeps_old() {
    let handle = reload_common::install_prometheus();
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    let proxy_addr: SocketAddr = "127.0.0.1:18080".parse().unwrap();
    reload_common::write_config(&config_path, 100, proxy_addr);

    let watcher = ConfigWatcher::new(&config_path, reload_common::load_config(&config_path));
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
fn invalid_principal_view_reload_keeps_old_view() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    let proxy_addr: SocketAddr = "127.0.0.1:18080".parse().unwrap();
    reload_common::write_config_with_principal_model(&config_path, 100, proxy_addr, "*");

    let initial_config = reload_common::load_config(&config_path);
    let principal_view = Arc::new(ArcSwap::from(
        PrincipalView::from_config(&initial_config).expect("principal view builds"),
    ));
    let before_view = principal_view.load_full();
    let watcher = ConfigWatcher::new_with_principal_view(
        &config_path,
        initial_config,
        Some(principal_view.clone()),
    );
    reload_common::write_config_with_principal_model(&config_path, 200, proxy_addr, "[");

    let logs = reload_common::capture_warn_logs(|| {
        assert!(watcher.reload_now().is_err());
    });

    assert_eq!(watcher.current_config().body.messages_cap_bytes, 100);
    assert!(Arc::ptr_eq(&before_view, &principal_view.load_full()));
    assert!(logs.contains("configuration reload failed"));
    assert!(logs.contains("invalid allowed_models glob"));
}
