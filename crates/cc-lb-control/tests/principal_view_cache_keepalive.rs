use std::collections::HashMap;

use cc_lb_control::api_keys::principal_view::PrincipalView;
use cc_lb_plugin_api::TerminalStrategy;
use cc_lb_storage_api::CacheKeepaliveConfig;
use cc_lb_storage_api::principal::{PrincipalKind, PrincipalRecord};
use uuid::Uuid;

fn principal_with_cache_keepalive(enabled: bool) -> PrincipalRecord {
    PrincipalRecord {
        id: Uuid::new_v4(),
        name: if enabled {
            "keepalive-enabled".to_owned()
        } else {
            "keepalive-disabled".to_owned()
        },
        kind: PrincipalKind::Machine,
        allowed_models: Vec::new(),
        allowed_upstreams: Vec::new(),
        default_limits: Vec::new(),
        enabled: true,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        router_terminal_strategy: TerminalStrategy::FirstPick,
        cache_keepalive: Some(CacheKeepaliveConfig {
            enabled,
            refresh_lead_time_5m_secs: 42,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 7,
            max_total_duration_secs: 3_600,
            snapshot_max_bytes: 128_000,
            classifier: Default::default(),
        }),
    }
}

#[test]
fn from_db_propagates_only_enabled_cache_keepalive_config() {
    let enabled = principal_with_cache_keepalive(true);
    let disabled = principal_with_cache_keepalive(false);
    let view = PrincipalView::from_db(&[enabled, disabled], HashMap::new());

    let enabled_cfg = view
        .get("keepalive-enabled")
        .expect("enabled principal cached")
        .cache_keepalive()
        .expect("enabled keepalive propagates");
    assert_eq!(enabled_cfg.refresh_lead_time_5m_secs, 42);
    assert_eq!(enabled_cfg.max_refreshes_per_session, 7);
    assert!(
        view.get("keepalive-disabled")
            .expect("disabled principal cached")
            .cache_keepalive()
            .is_none()
    );
}
