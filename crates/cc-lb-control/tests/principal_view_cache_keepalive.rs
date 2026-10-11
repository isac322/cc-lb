use std::collections::HashMap;

use cc_lb_control::api_keys::principal_view::PrincipalView;
use cc_lb_domain::TerminalStrategy;
use cc_lb_storage_api::principal::{PrincipalKind, PrincipalRecord};
use cc_lb_storage_api::{CacheKeepaliveConfig, PromptCacheMode};
use uuid::Uuid;

fn principal_with_cache_keepalive(enabled: bool) -> PrincipalRecord {
    PrincipalRecord {
        prompt_cache_mode: Default::default(),
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

#[test]
fn one_hour_mode_masks_an_enabled_keepalive_config_and_reports_its_mode() {
    let mut one_hour = principal_with_cache_keepalive(true);
    one_hour.name = "keepalive-one-hour".to_owned();
    one_hour.prompt_cache_mode = PromptCacheMode::OneHourNoKeepalive;
    let view = PrincipalView::from_db(&[one_hour], HashMap::new());

    let cached = view.get("keepalive-one-hour").expect("principal cached");
    assert!(
        cached.cache_keepalive().is_none(),
        "the effective accessor is the single precedence point: one-hour mode          leaves the stored, still-enabled config dormant"
    );
    assert_eq!(
        cached.prompt_cache_mode(),
        PromptCacheMode::OneHourNoKeepalive,
        "request normalization reads the mode from the same snapshot"
    );
}

#[test]
fn preserve_client_mode_reports_client_policy() {
    let view = PrincipalView::from_db(&[principal_with_cache_keepalive(true)], HashMap::new());

    let cached = view.get("keepalive-enabled").expect("principal cached");
    assert_eq!(cached.prompt_cache_mode(), PromptCacheMode::PreserveClient);
    assert!(cached.cache_keepalive().is_some());
}
