use std::time::Duration;

use cc_lb_core::api_keys::principal_view::{PrincipalStatus, PrincipalView};
use cc_lb_core::api_keys::types::{LimitKind as CoreLimitKind, PrincipalType as CorePrincipalType};
use cc_lb_storage_api::principal::{
    Limit, LimitKind, PrincipalKind as DbPrincipalKind, PrincipalRecord,
};

fn sample_principals(enabled: bool) -> Vec<PrincipalRecord> {
    vec![PrincipalRecord {
        id: uuid::Uuid::new_v4(),
        name: "u1".to_owned(),
        kind: DbPrincipalKind::Machine,
        allowed_models: vec![
            "claude-3-5-sonnet-20241022".to_owned(),
            "claude-3-5-sonnet-*".to_owned(),
        ],
        default_limits: vec![Limit {
            kind: LimitKind::Requests,
            window_secs: Duration::from_secs(60).as_secs(),
            cap_micros: 1_000,
        }],
        enabled,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 1,
        updated_at_unix_secs: 1,
    }]
}

#[test]
fn principal_view_smoke() {
    let principals = sample_principals(true);
    let view = PrincipalView::from_db(&principals, std::collections::HashMap::new());

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
    assert_eq!(view.default_limits("u1")[0].kind, CoreLimitKind::Requests);
}

#[test]
fn disabled_status() {
    let principals = sample_principals(false);
    let view = PrincipalView::from_db(&principals, std::collections::HashMap::new());

    assert_eq!(view.principal_status("u1"), PrincipalStatus::Disabled);
}

#[test]
fn invalid_allowed_models_glob_falls_back_to_exact_match() {
    let mut principals = sample_principals(true);
    principals[0].allowed_models = vec!["[".to_owned()];
    let view = PrincipalView::from_db(&principals, std::collections::HashMap::new());

    assert!(view.is_model_allowed("u1", "["));
    assert!(!view.is_model_allowed("u1", "anything-else"));
}
