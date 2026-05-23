use std::collections::HashMap;
use std::time::Duration;

use cc_lb_config::{Config, Limit, LimitKind, PrincipalSpec, PrincipalType};
use cc_lb_core::api_keys::principal_view::{PrincipalStatus, PrincipalView};
use cc_lb_core::api_keys::types::PrincipalType as CorePrincipalType;

fn sample_config(enabled: bool) -> Config {
    let mut principals = HashMap::new();
    principals.insert(
        "u1".to_owned(),
        PrincipalSpec {
            principal_type: PrincipalType::Machine,
            default_limits: vec![Limit {
                kind: LimitKind::Requests,
                window: Duration::from_secs(60),
                cap_micros: 1_000,
            }],
            enabled,
            allowed_models: vec![
                "claude-3-5-sonnet-20241022".to_owned(),
                "claude-3-5-sonnet-*".to_owned(),
            ],
            credentials_ref: None,
        },
    );

    Config {
        principals,
        ..Config::default()
    }
}

#[test]
fn principal_view_smoke() {
    let view = PrincipalView::from_config(&sample_config(true));

    let spec = view.get("u1");
    assert!(spec.is_some());
    let spec = spec.expect("principal should exist");
    assert_eq!(spec.id(), "u1");
    assert_eq!(spec.principal_type(), CorePrincipalType::Machine);
    assert!(view.is_model_allowed("u1", "claude-3-5-sonnet-20241022"));
    assert!(view.is_model_allowed("u1", "claude-3-5-sonnet-20250101"));
    assert!(!view.is_model_allowed("u1", "gpt-4"));
    assert_eq!(view.principal_status("u1"), PrincipalStatus::Active);
    assert_eq!(view.principal_status("missing"), PrincipalStatus::Missing);
    assert_eq!(view.default_limits("u1").len(), 1);
}

#[test]
fn disabled_status() {
    let view = PrincipalView::from_config(&sample_config(false));

    assert_eq!(view.principal_status("u1"), PrincipalStatus::Disabled);
}
