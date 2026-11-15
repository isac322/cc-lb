use super::*;
use cc_lb_lifecycle::{AuthInfo, ParseInfo, UsageSnapshot, UsageSource};
use cc_lb_request_log::CostBreakdown;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};

type Samples = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

fn counter(snapshot: &Samples, name: &str, label: Option<(&str, &str)>) -> Option<u64> {
    let values: Vec<_> = snapshot
        .iter()
        .filter_map(|(key, _, _, value)| {
            if key.key().name() != name
                || label.is_some_and(|(name, value)| {
                    !key.key()
                        .labels()
                        .any(|l| l.key() == name && l.value() == value)
                })
            {
                return None;
            }
            match value {
                DebugValue::Counter(n) => Some(*n),
                _ => None,
            }
        })
        .collect();
    (!values.is_empty()).then(|| values.into_iter().sum())
}

fn started(id: &str, model: Option<&str>, key: Option<&str>, usage: bool) -> Vec<LifecycleEvent> {
    let mut events = vec![
        LifecycleEvent::AuthCompleted {
            event_id: id.into(),
            result: Ok(AuthInfo {
                principal_id: "principal".into(),
                key_id: key.map(str::to_owned),
                principal_kind: None,
                auth_ms: None,
            }),
        },
        LifecycleEvent::ParseCompleted {
            event_id: id.into(),
            result: Ok(ParseInfo {
                model: model.map(str::to_owned),
                ..Default::default()
            }),
        },
    ];
    if usage {
        // Usage from the captured real Responses request in issue #1299:
        // input_tokens=309, cached_tokens=0, output_tokens=8.
        events.push(LifecycleEvent::UsageObserved {
            event_id: id.into(),
            usage: UsageSnapshot {
                input_tokens: 309,
                output_tokens: 8,
                ..Default::default()
            },
            source: UsageSource::NonStreamBody,
        });
    }
    events
}

fn price(id: &str, total: Option<i64>) -> LifecycleEvent {
    LifecycleEvent::Priced {
        event_id: id.into(),
        cost: CostBreakdown {
            total_micros: total,
            ..Default::default()
        },
    }
}

fn feed(
    partials: &mut HashMap<EventId, Partial>,
    events: impl IntoIterator<Item = LifecycleEvent>,
) {
    for event in events {
        handle_event(partials, 16, event);
    }
}

#[test]
fn price_arrival_orders_emit_cost_and_usage_exactly_once() {
    for early in [false, true] {
        let recorder = DebuggingRecorder::new();
        let snapshot = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let mut partials = HashMap::new();
            feed(
                &mut partials,
                started("request", Some("gpt-6.1-sol"), Some("key"), true),
            );
            if early {
                handle_event(&mut partials, 16, price("request", Some(325)));
            }
            handle_event(&mut partials, 16, tests::terminated("request", 200));
            if !early {
                handle_event(&mut partials, 16, tests::terminated("request", 200));
                handle_event(&mut partials, 16, price("request", Some(325)));
            }
            handle_event(&mut partials, 16, price("request", Some(325)));
            handle_event(&mut partials, 16, tests::terminated("request", 200));
            let snapshot = snapshot.snapshot().into_vec();
            assert_eq!(
                counter(
                    &snapshot,
                    "cclb_api_key_tokens_total",
                    Some(("kind", "input"))
                ),
                Some(309)
            );
            assert_eq!(
                counter(&snapshot, "cc_lb_virtual_cost_usd_total", None),
                Some(325),
                "early price: {early}"
            );
            assert_eq!(
                counter(&snapshot, "cclb_api_key_cost_usd_micro_total", None),
                Some(325)
            );
            assert_eq!(
                counter(&snapshot, "cclb_api_key_requests_total", None),
                Some(1)
            );
            assert_eq!(
                counter(
                    &snapshot,
                    "cclb_api_key_tokens_total",
                    Some(("kind", "output"))
                ),
                Some(8)
            );
        });
    }
}

#[test]
fn unknown_and_known_zero_are_final_price_results() {
    for total in [None, Some(0), Some(-5)] {
        let recorder = DebuggingRecorder::new();
        let snapshot = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let mut partials = HashMap::new();
            feed(
                &mut partials,
                started("request", Some("model"), Some("key"), true),
            );
            handle_event(&mut partials, 16, tests::terminated("request", 200));
            handle_event(&mut partials, 16, price("request", total));
            sweep_orphans(&mut partials, Duration::ZERO);
            let snapshot = snapshot.snapshot().into_vec();
            assert_eq!(
                counter(&snapshot, "cc_lb_virtual_cost_usd_total", None),
                total.map(|_| 0)
            );
            assert_eq!(
                counter(&snapshot, "cclb_api_key_cost_usd_micro_total", None),
                None
            );
            assert_eq!(
                counter(&snapshot, "cclb_api_key_requests_total", None),
                Some(1)
            );
            assert_eq!(
                counter(
                    &snapshot,
                    "cc_lb_lifecycle_api_key_metrics_events_total",
                    Some(("outcome", "cost_unresolved"))
                ),
                None
            );
            assert_eq!(
                counter(
                    &snapshot,
                    "cc_lb_lifecycle_api_key_metrics_events_total",
                    Some(("outcome", "orphan_ttl_evicted"))
                ),
                None
            );
        });
    }
}

#[test]
fn unmetered_missing_model_and_missing_key_do_not_wait() {
    for (model, key, usage) in [
        (Some("model"), Some("key"), false),
        (None, Some("key"), true),
        (Some("model"), None, true),
    ] {
        let recorder = DebuggingRecorder::new();
        let snapshot = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let mut partials = HashMap::new();
            feed(&mut partials, started("request", model, key, usage));
            handle_event(&mut partials, 16, tests::terminated("request", 400));
            sweep_orphans(&mut partials, Duration::ZERO);
            let snapshot = snapshot.snapshot().into_vec();
            assert_eq!(
                counter(&snapshot, "cclb_api_key_requests_total", None),
                key.map(|_| 1)
            );
            assert_eq!(
                counter(
                    &snapshot,
                    "cclb_api_key_tokens_total",
                    Some(("kind", "input"))
                ),
                (usage && key.is_some()).then_some(309)
            );
            assert_eq!(
                counter(&snapshot, "cc_lb_virtual_cost_usd_total", None),
                None
            );
            assert_eq!(
                counter(
                    &snapshot,
                    "cc_lb_lifecycle_api_key_metrics_events_total",
                    Some(("outcome", "cost_unresolved"))
                ),
                None
            );
        });
    }
}

#[test]
fn missing_price_expires_without_reemitting_request() {
    let recorder = DebuggingRecorder::new();
    let snapshot = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        let mut partials = HashMap::new();
        feed(
            &mut partials,
            started("request", Some("model"), Some("key"), true),
        );
        handle_event(&mut partials, 16, tests::terminated("request", 200));
        sweep_orphans(&mut partials, Duration::ZERO);
        handle_event(&mut partials, 16, price("request", Some(325)));
        handle_event(&mut partials, 16, tests::terminated("request", 200));
        let snapshot = snapshot.snapshot().into_vec();
        assert_eq!(
            counter(&snapshot, "cclb_api_key_requests_total", None),
            Some(1)
        );
        assert_eq!(
            counter(
                &snapshot,
                "cclb_api_key_tokens_total",
                Some(("kind", "input"))
            ),
            Some(309)
        );
        assert_eq!(
            counter(&snapshot, "cc_lb_virtual_cost_usd_total", None),
            None
        );
        assert_eq!(
            counter(
                &snapshot,
                "cc_lb_lifecycle_api_key_metrics_events_total",
                Some(("outcome", "cost_unresolved"))
            ),
            Some(1)
        );
    });
}

#[test]
fn cap_prefers_completed_waiter_over_older_inflight_request() {
    let recorder = DebuggingRecorder::new();
    let snapshot = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        let mut partials = HashMap::new();
        feed(
            &mut partials,
            started("inflight", Some("model"), Some("key"), true),
        );
        partials.get_mut("inflight").unwrap().inserted_at =
            Instant::now().checked_sub(Duration::from_secs(60));
        feed(
            &mut partials,
            started("waiter", Some("model"), Some("key"), true),
        );
        handle_event(&mut partials, 16, tests::terminated("waiter", 200));
        handle_event(
            &mut partials,
            2,
            started("third", None, None, false).remove(0),
        );
        handle_event(&mut partials, 2, price("inflight", Some(325)));
        handle_event(&mut partials, 2, tests::terminated("inflight", 200));
        let snapshot = snapshot.snapshot().into_vec();
        assert_eq!(
            counter(&snapshot, "cclb_api_key_requests_total", None),
            Some(2)
        );
        assert_eq!(
            counter(&snapshot, "cc_lb_virtual_cost_usd_total", None),
            Some(325)
        );
        assert_eq!(
            counter(
                &snapshot,
                "cc_lb_lifecycle_api_key_metrics_events_total",
                Some(("outcome", "cost_unresolved"))
            ),
            Some(1)
        );
    });
}

#[test]
fn unterminated_orphans_remain_silent_and_bounded() {
    let recorder = DebuggingRecorder::new();
    let snapshot = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        let mut partials = HashMap::new();
        for index in 0..20 {
            handle_event(
                &mut partials,
                2,
                started(&index.to_string(), None, Some("key"), false).remove(0),
            );
        }
        sweep_orphans(&mut partials, Duration::ZERO);
        let snapshot = snapshot.snapshot().into_vec();
        assert_eq!(
            counter(&snapshot, "cclb_api_key_requests_total", None),
            None
        );
        assert_eq!(
            counter(&snapshot, "cc_lb_virtual_cost_usd_total", None),
            None
        );
        assert_eq!(
            counter(
                &snapshot,
                "cc_lb_lifecycle_api_key_metrics_events_total",
                Some(("outcome", "cap_evicted"))
            ),
            Some(18)
        );
        assert_eq!(
            counter(
                &snapshot,
                "cc_lb_lifecycle_api_key_metrics_events_total",
                Some(("outcome", "orphan_ttl_evicted"))
            ),
            Some(2)
        );
    });
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_drains_prices_and_diagnoses_only_unresolved_costs() {
    let recorder = DebuggingRecorder::new();
    let snapshot = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let (tx, rx) = mpsc::channel(32);
    let handle = spawn_with_config(rx, 16, Duration::from_secs(300));
    for id in ["priced", "missing", "inflight"] {
        for event in started(id, Some("model"), Some("key"), true) {
            tx.send(event).await.unwrap();
        }
        if id != "inflight" {
            tx.send(tests::terminated(id, 200)).await.unwrap();
        }
    }
    tx.send(price("priced", Some(325))).await.unwrap();
    handle.shutdown().await;
    let snapshot = snapshot.snapshot().into_vec();
    assert_eq!(
        counter(&snapshot, "cclb_api_key_requests_total", None),
        Some(2)
    );
    assert_eq!(
        counter(&snapshot, "cc_lb_virtual_cost_usd_total", None),
        Some(325)
    );
    assert_eq!(
        counter(
            &snapshot,
            "cc_lb_lifecycle_api_key_metrics_events_total",
            Some(("outcome", "cost_unresolved"))
        ),
        Some(1)
    );
}
