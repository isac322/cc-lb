#![cfg(loom)]

use std::sync::Arc as StdArc;

use arc_swap::ArcSwap;
use cc_lb_loom_tests::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_loom_tests::api_keys::limit_engine::LimitEngine;
use cc_lb_loom_tests::api_keys::principal_view::PrincipalView;
use cc_lb_storage_redb::{
    KeyStatus, Limit as StoredLimit, LimitKind as StoredLimitKind, StoredApiKeyRecord,
};
use loom::sync::{Arc, Mutex};
use loom::thread;

const PRINCIPAL_ID: &str = "principal-1";
const KEY_ID: &str = "key-1";
const MODEL: &str = "claude";

#[test]
fn loom_limit_engine_rolling_drop_refunds_full_reservations() {
    loom::model(|| {
        let engine = engine();
        let record = record(vec![limit(StoredLimitKind::Requests, 60, 100)]);
        let mut handles = Vec::new();

        for _ in 0..2 {
            let engine = StdArc::clone(&engine);
            let record = record.clone();
            handles.push(thread::spawn(move || {
                let reservation = engine
                    .reserve(&record, PRINCIPAL_ID, MODEL, 0, 0, None)
                    .expect("reservation succeeds");
                drop(reservation);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(request_remaining(&engine), 100);
    });
}

#[test]
fn loom_limit_engine_rolling_reconcile_keeps_actual_totals() {
    loom::model(|| {
        let engine = engine();
        let record = record(vec![limit(StoredLimitKind::TotalTokens, 60, 1_000)]);
        let mut handles = Vec::new();

        for _ in 0..2 {
            let engine = StdArc::clone(&engine);
            let record = record.clone();
            handles.push(thread::spawn(move || {
                let reservation = engine
                    .reserve(&record, PRINCIPAL_ID, MODEL, 100, 50, None)
                    .expect("reservation succeeds");
                engine.reconcile(reservation, 10, 5, 0);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(token_remaining(&engine), 970);
    });
}

#[test]
fn loom_limit_engine_rolling_drop_vs_reconcile_single_owner_wins() {
    loom::model(|| {
        let engine = engine();
        let record = record(vec![limit(StoredLimitKind::TotalTokens, 60, 1_000)]);
        let reservation = engine
            .reserve(&record, PRINCIPAL_ID, MODEL, 100, 50, None)
            .expect("reservation succeeds");
        let slot = Arc::new(Mutex::new(Some(reservation)));

        let drop_slot = Arc::clone(&slot);
        let drop_handle = thread::spawn(move || {
            let reservation = drop_slot.lock().unwrap().take();
            drop(reservation);
        });

        let reconcile_slot = Arc::clone(&slot);
        let reconcile_engine = StdArc::clone(&engine);
        let reconcile_handle = thread::spawn(move || {
            if let Some(reservation) = reconcile_slot.lock().unwrap().take() {
                reconcile_engine.reconcile(reservation, 10, 5, 0);
            }
        });

        drop_handle.join().unwrap();
        reconcile_handle.join().unwrap();

        let remaining = token_remaining(&engine);
        assert!(matches!(remaining, 1_000 | 985));
    });
}

fn engine() -> StdArc<LimitEngine> {
    let view = PrincipalView::loom(Vec::new());

    LimitEngine::new(
        StdArc::new(KeyConcurrencyManager::new()),
        StdArc::new(ArcSwap::from(view)),
    )
}

fn record(limits: Vec<StoredLimit>) -> StoredApiKeyRecord {
    StoredApiKeyRecord {
        key_hash_b64: KEY_ID.to_owned(),
        status: KeyStatus::Active,
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

fn request_remaining(engine: &LimitEngine) -> i64 {
    header_value(engine, "anthropic-ratelimit-requests-remaining")
}

fn token_remaining(engine: &LimitEngine) -> i64 {
    header_value(engine, "anthropic-ratelimit-tokens-remaining")
}

fn header_value(engine: &LimitEngine, name: &str) -> i64 {
    engine
        .headers_for(KEY_ID, PRINCIPAL_ID)
        .into_iter()
        .find(|(header_name, _)| header_name == name)
        .and_then(|(_, value)| value.parse().ok())
        .expect("rate limit header")
}
