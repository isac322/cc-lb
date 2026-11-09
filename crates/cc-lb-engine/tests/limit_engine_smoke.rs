use cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_control::api_keys::limit_engine::{LimitEngine, RejectReason};
use cc_lb_control::api_keys::principal_view::PrincipalView;
use cc_lb_storage_api::{
    KeyStatus, Limit as StoredLimit, LimitKind as StoredLimitKind, StoredApiKeyRecord,
};
use std::collections::HashMap;
use std::sync::Arc;

fn engine(enabled: bool) -> (Arc<LimitEngine>, Arc<PrincipalView>) {
    let view = Arc::new(PrincipalView::for_tests(
        "principal-1",
        enabled,
        Vec::new(),
        Vec::new(),
        HashMap::new(),
    ));

    (
        LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            Arc::new(cc_lb_clock::SystemClock),
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

    let first = engine.reserve(&view, &record, "principal-1", "claude", Some(0), 0, None);
    assert!(first.is_ok());
    let second = engine.reserve(&view, &record, "principal-1", "claude", Some(0), 0, None);
    assert!(second.is_ok());
    let third = engine.reserve(&view, &record, "principal-1", "claude", Some(0), 0, None);
    assert_eq!(third.err(), Some(RejectReason::RequestsRateLimit));
}

#[test]
fn output_cap_rejects_single_request_above_cap() {
    let (engine, view) = engine(true);
    let record = record(
        KeyStatus::Active,
        vec![limit(StoredLimitKind::OutputTokens, 60, 100)],
    );

    let result = engine.reserve(&view, &record, "principal-1", "claude", Some(200), 0, None);

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

    let result = engine.reserve(
        &view,
        &record,
        "principal-1",
        "claude",
        Some(0),
        0,
        Some(2_000),
    );

    assert_eq!(
        result.err(),
        Some(RejectReason::Unsatisfiable {
            kind: StoredLimitKind::CostUsd,
            cap: 1_000,
            requested: 2_000,
        })
    );
}

#[test]
fn principal_disabled_rejects() {
    let (engine, view) = engine(false);
    let record = record(KeyStatus::Active, Vec::new());

    let result = engine.reserve(&view, &record, "principal-1", "claude", Some(0), 0, None);

    assert_eq!(result.err(), Some(RejectReason::PrincipalDisabled));
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
        .reserve(&view, &record, "principal-1", "claude", Some(100), 0, None)
        .expect("reservation succeeds");
    engine.reconcile(reservation, 0, 50, Some(0));

    let headers = engine.headers_for("key-1", "principal-1");
    let remaining = headers
        .iter()
        .find(|(name, _)| name == "anthropic-ratelimit-tokens-remaining")
        .map(|(_, value)| value.as_str());

    assert_eq!(remaining, Some("150"));
}

#[test]
fn local_limit_retryability_impossible_reservation_has_no_partial_admission() {
    // QA-14: an impossible later limit must neither leak an earlier acquired
    // concurrency slot nor charge earlier request/window reservations.
    for (kind, requested, max_output, max_input, cost) in [
        (StoredLimitKind::Requests, 1, 0, 0, None),
        (StoredLimitKind::InputTokens, 4_000, 0, 4_000, None),
        (StoredLimitKind::TotalTokens, 6_000, 2_000, 4_000, None),
        (StoredLimitKind::CostUsd, 24_000, 2_000, 4_000, Some(24_000)),
    ] {
        let (engine, view) = engine(true);
        let mut limits = vec![limit(StoredLimitKind::Concurrent, 0, 1)];
        if kind != StoredLimitKind::Requests {
            limits.push(limit(StoredLimitKind::Requests, 60, 1));
        }
        limits.push(limit(kind, 60, requested - 1));
        let mut record = record(KeyStatus::Active, limits);
        let reason = engine
            .reserve(
                &view,
                &record,
                "principal-1",
                "claude",
                Some(max_output),
                max_input,
                cost,
            )
            .err()
            .expect("request cannot fit configured cap");

        record
            .limit_overrides
            .last_mut()
            .expect("offending limit")
            .cap_micros = requested;
        let admitted = engine
            .reserve(
                &view,
                &record,
                "principal-1",
                "claude",
                Some(max_output),
                max_input,
                cost,
            )
            .expect("failed reserve left concurrency and request budgets untouched");
        drop(admitted);

        assert!(
            !matches!(
                reason,
                RejectReason::RequestsRateLimit
                    | RejectReason::TokenRateLimit { .. }
                    | RejectReason::CostRateLimit
                    | RejectReason::ConcurrentRateLimit
            ),
            "configured capacity failure must not use a temporary reason: {reason:?}",
        );
    }
}
