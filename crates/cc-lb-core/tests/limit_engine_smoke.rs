use arc_swap::ArcSwap;
use cc_lb_config::{Config, PrincipalSpec, PrincipalType};
use cc_lb_core::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_core::api_keys::limit_engine::{LimitEngine, RejectReason};
use cc_lb_core::api_keys::principal_view::PrincipalView;
use cc_lb_storage_redb::{
    KeyStatus, Limit as StoredLimit, LimitKind as StoredLimitKind, StoredApiKeyRecord,
};
use std::collections::HashMap;
use std::sync::Arc;

fn engine(enabled: bool) -> (Arc<LimitEngine>, Arc<PrincipalView>) {
    let mut principals = HashMap::new();
    principals.insert(
        "principal-1".to_owned(),
        PrincipalSpec {
            principal_type: PrincipalType::Machine,
            default_limits: Vec::new(),
            enabled,
            allowed_models: Vec::new(),
            credentials_ref: None,
            router_plugin: None,
            observability_hooks: None,
        },
    );
    let view = PrincipalView::from_config(
        &Config {
            principals,
            ..Config::default()
        },
        std::collections::HashMap::new(),
    )
    .expect("principal view builds");

    (
        LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            Arc::new(ArcSwap::from(view.clone())),
        ),
        view,
    )
}

fn record(status: KeyStatus, limits: Vec<StoredLimit>) -> StoredApiKeyRecord {
    StoredApiKeyRecord {
        key_hash_b64: "key-1".to_owned(),
        status,
        limit_overrides: limits,
        ..StoredApiKeyRecord::default()
    }
}

fn limit(kind: StoredLimitKind, window_secs: u64, cap_micros: i64) -> StoredLimit {
    StoredLimit {
        kind,
        window_secs,
        cap_micros,
    }
}

#[test]
fn requests_cap_two_rejects_third() {
    let (engine, view) = engine(true);
    let record = record(
        KeyStatus::Active,
        vec![limit(StoredLimitKind::Requests, 60, 2)],
    );

    let first = engine.reserve(&view, &record, "principal-1", "claude", 0, 0, None);
    assert!(first.is_ok());
    let second = engine.reserve(&view, &record, "principal-1", "claude", 0, 0, None);
    assert!(second.is_ok());
    let third = engine.reserve(&view, &record, "principal-1", "claude", 0, 0, None);
    assert_eq!(third.err(), Some(RejectReason::RequestsRateLimit));
}

#[test]
fn output_cap_rejects_single_request_above_cap() {
    let (engine, view) = engine(true);
    let record = record(
        KeyStatus::Active,
        vec![limit(StoredLimitKind::OutputTokens, 60, 100)],
    );

    let result = engine.reserve(&view, &record, "principal-1", "claude", 200, 0, None);

    assert_eq!(
        result.err(),
        Some(RejectReason::OutputCapExceeded {
            cap: 100,
            requested: 200,
        })
    );
}

#[test]
fn cost_usd_cap_rejects_when_estimate_exceeds_cap() {
    let (engine, view) = engine(true);
    let record = record(
        KeyStatus::Active,
        vec![limit(StoredLimitKind::CostUsd, 60, 1_000)],
    );

    let result = engine.reserve(&view, &record, "principal-1", "claude", 0, 0, Some(2_000));

    assert_eq!(result.err(), Some(RejectReason::CostRateLimit));
}

#[test]
fn principal_disabled_rejects() {
    let (engine, view) = engine(false);
    let record = record(KeyStatus::Active, Vec::new());

    let result = engine.reserve(&view, &record, "principal-1", "claude", 0, 0, None);

    assert_eq!(result.err(), Some(RejectReason::PrincipalDisabled));
}

#[test]
fn record_disabled_rejects() {
    let (engine, view) = engine(true);
    let record = record(KeyStatus::Disabled, Vec::new());

    let result = engine.reserve(&view, &record, "principal-1", "claude", 0, 0, None);

    assert_eq!(result.err(), Some(RejectReason::KeyDisabled));
}

#[test]
fn reconcile_refund_leaves_actual_output_usage() {
    let (engine, view) = engine(true);
    let record = record(
        KeyStatus::Active,
        vec![
            limit(StoredLimitKind::OutputTokens, 60, 200),
            limit(StoredLimitKind::TotalTokens, 60, 200),
        ],
    );

    let reservation = engine
        .reserve(&view, &record, "principal-1", "claude", 100, 0, None)
        .expect("reservation succeeds");
    engine.reconcile(reservation, 0, 50, 0);

    let headers = engine.headers_for("key-1", "principal-1");
    let remaining = headers
        .iter()
        .find(|(name, _)| name == "anthropic-ratelimit-tokens-remaining")
        .map(|(_, value)| value.as_str());

    assert_eq!(remaining, Some("150"));
}
