use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use cc_lb_lifecycle::{LifecycleEvent, StreamSuccess, UsageSnapshot, UsageSource};

use super::*;
use crate::{CatalogSnapshot, CatalogStatus, Pricing, TierRate, UsdPerMillion};

const MODEL: &str = "subscriber-tier-model";

fn install_tierless_pricing() {
    crate::global_catalog().install_snapshot(CatalogSnapshot {
        payload_hash: String::new(),
        fetched_at_ms: 1,
        models: HashMap::from([(
            MODEL.to_owned(),
            Pricing {
                native: None,
                model: MODEL.to_owned(),
                input_per_million_usd: UsdPerMillion::from_whole_usd(2),
                output_per_million_usd: UsdPerMillion::from_whole_usd(8),
                input_image_per_million_usd: None,
                output_image_per_million_usd: None,
                by_tier: BTreeMap::<String, TierRate>::new(),
            },
        )]),
        raw_json: b"{}".to_vec(),
        cache_creation_per_million_usd: HashMap::new(),
        cache_read_per_million_usd: HashMap::new(),
        cache_creation_per_million_usd_by_tier: HashMap::new(),
        cache_read_per_million_usd_by_tier: HashMap::new(),
        above_tokens_rates: HashMap::new(),
        status: CatalogStatus::Ok,
    });
}

fn partial_with_model() -> Partial {
    Partial {
        model: Some(MODEL.to_owned()),
        ..Partial::new(Instant::now())
    }
}

#[test]
fn usage_observed_batch_tier_is_used_for_actual_cost() {
    // Given a model without explicit tiers and a batch UsageObserved event.
    let _guard = crate::GLOBAL_TEST_LOCK
        .lock()
        .expect("global catalog test lock");
    install_tierless_pricing();
    let mut partial = partial_with_model();

    // When usage is merged and actual cost is computed.
    merge(
        &mut partial,
        LifecycleEvent::UsageObserved {
            event_id: "usage-batch".to_owned(),
            source: UsageSource::NonStreamBody,
            usage: UsageSnapshot {
                input_tokens: 1_000_000,
                output_tokens: 1_000_000,
                service_tier: Some("batch".to_owned()),
                ..UsageSnapshot::default()
            },
        },
    );
    let cost = compute_cost(&partial).expect("usage should produce actual cost");

    // Then LiteLLM's batch fallback halves both base prices.
    assert_eq!(cost.input_micros, Some(1_000_000));
    assert_eq!(cost.output_micros, Some(4_000_000));
    assert_eq!(cost.total_micros, Some(5_000_000));
}

#[test]
fn stream_completed_batch_tier_is_used_for_actual_cost() {
    // Given a model without explicit tiers and a batch StreamCompleted event.
    let _guard = crate::GLOBAL_TEST_LOCK
        .lock()
        .expect("global catalog test lock");
    install_tierless_pricing();
    let mut partial = partial_with_model();

    // When stream usage is merged and actual cost is computed.
    merge(
        &mut partial,
        LifecycleEvent::StreamCompleted {
            event_id: "stream-batch".to_owned(),
            result: Ok(StreamSuccess {
                usage: UsageSnapshot {
                    input_tokens: 1_000_000,
                    output_tokens: 1_000_000,
                    service_tier: Some("batch".to_owned()),
                    ..UsageSnapshot::default()
                },
                usage_present: true,
                ..StreamSuccess::default()
            }),
        },
    );
    let cost = compute_cost(&partial).expect("stream usage should produce actual cost");

    // Then LiteLLM's batch fallback halves both base prices.
    assert_eq!(cost.input_micros, Some(1_000_000));
    assert_eq!(cost.output_micros, Some(4_000_000));
    assert_eq!(cost.total_micros, Some(5_000_000));
}

#[test]
fn observed_gemini_flex_priority_and_omitted_tiers_price_each_component() {
    let _guard = crate::GLOBAL_TEST_LOCK
        .lock()
        .expect("global catalog test lock");
    let mut snapshot = CatalogSnapshot::empty_cost_disabled();
    snapshot.status = CatalogStatus::Ok;
    snapshot.models.insert(
        MODEL.to_owned(),
        Pricing {
            model: MODEL.to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(2),
            output_per_million_usd: UsdPerMillion::from_whole_usd(8),
            input_image_per_million_usd: None,
            output_image_per_million_usd: None,
            native: None,
            by_tier: BTreeMap::from([
                (
                    "flex".to_owned(),
                    TierRate {
                        input_per_million_usd: UsdPerMillion::from_whole_usd(1),
                        output_per_million_usd: UsdPerMillion::from_whole_usd(4),
                    },
                ),
                (
                    "priority".to_owned(),
                    TierRate {
                        input_per_million_usd: UsdPerMillion::from_whole_usd(3),
                        output_per_million_usd: UsdPerMillion::from_whole_usd(12),
                    },
                ),
            ]),
        },
    );
    snapshot
        .cache_read_per_million_usd
        .insert(MODEL.to_owned(), UsdPerMillion::from_whole_usd(2));
    snapshot.cache_read_per_million_usd_by_tier.insert(
        MODEL.to_owned(),
        BTreeMap::from([
            ("flex".to_owned(), UsdPerMillion::from_whole_usd(1)),
            ("priority".to_owned(), UsdPerMillion::from_whole_usd(4)),
        ]),
    );
    crate::global_catalog().install_snapshot(snapshot);
    for (tier, input, output, cache_read) in [
        (Some("flex"), 100, 40, 20),
        (Some("priority"), 300, 120, 80),
        (None, 200, 80, 40),
    ] {
        for streaming in [false, true] {
            let mut partial = partial_with_model();
            merge(
                &mut partial,
                LifecycleEvent::ParseCompleted {
                    event_id: "gemini-tier".to_owned(),
                    result: Ok(cc_lb_lifecycle::ParseInfo {
                        thinking_level: Some("HIGH".to_owned()),
                        thinking_budget_tokens: Some(1024),
                        ..Default::default()
                    }),
                },
            );
            let usage = UsageSnapshot {
                input_tokens: 100,
                output_tokens: 10,
                cache_read_input_tokens: 20,
                thinking_tokens: 4,
                service_tier: tier.map(str::to_owned),
                ..UsageSnapshot::default()
            };
            let event = if streaming {
                LifecycleEvent::StreamCompleted {
                    event_id: "gemini-tier".to_owned(),
                    result: Ok(StreamSuccess {
                        usage,
                        usage_present: true,
                        ..StreamSuccess::default()
                    }),
                }
            } else {
                LifecycleEvent::UsageObserved {
                    event_id: "gemini-tier".to_owned(),
                    usage,
                    source: UsageSource::NonStreamBody,
                }
            };
            merge(&mut partial, event);
            let cost = compute_cost(&partial).expect("observed Gemini usage is priced");
            assert_eq!(cost.input_micros, Some(input));
            assert_eq!(cost.output_micros, Some(output));
            assert_eq!(cost.cache_read_micros, Some(cache_read));
            assert_eq!(cost.total_micros, Some(input + output + cache_read));
        }
    }
}

#[test]
fn pricing_slices_price_each_fanout_call_independently_regardless_of_order() {
    use cc_lb_lifecycle::UsagePricingSlice;

    let _guard = crate::GLOBAL_TEST_LOCK
        .lock()
        .expect("global catalog test lock");
    let mut snapshot = CatalogSnapshot::empty_cost_disabled();
    snapshot.status = CatalogStatus::Ok;
    snapshot.models.insert(
        MODEL.to_owned(),
        Pricing {
            model: MODEL.to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(2),
            output_per_million_usd: UsdPerMillion::from_whole_usd(8),
            input_image_per_million_usd: None,
            output_image_per_million_usd: None,
            native: None,
            by_tier: BTreeMap::from([(
                "priority".to_owned(),
                TierRate {
                    input_per_million_usd: UsdPerMillion::from_whole_usd(4),
                    output_per_million_usd: UsdPerMillion::from_whole_usd(16),
                },
            )]),
        },
    );
    crate::global_catalog().install_snapshot(snapshot);

    let default_slice = UsagePricingSlice {
        input_tokens: 1_000,
        output_tokens: 500,
        service_tier: Some("default".to_owned()),
        ..UsagePricingSlice::default()
    };
    let priority_slice = UsagePricingSlice {
        input_tokens: 2_000,
        output_tokens: 1_000,
        service_tier: Some("priority".to_owned()),
        ..UsagePricingSlice::default()
    };
    // default(1000, 500) = 2000 + 4000 = 6000 micros
    // priority(2000, 1000) = 8000 + 16000 = 24000 micros
    // sum = 30000 micros regardless of slice order
    let mut forward = partial_with_model();
    merge(
        &mut forward,
        LifecycleEvent::UsageObserved {
            event_id: "fanout-forward".to_owned(),
            source: UsageSource::NonStreamBody,
            usage: UsageSnapshot {
                input_tokens: 3_000,
                output_tokens: 1_500,
                service_tier: Some("priority".to_owned()),
                pricing_slices: vec![default_slice.clone(), priority_slice.clone()],
                ..UsageSnapshot::default()
            },
        },
    );
    let forward_cost = compute_cost(&forward).expect("forward slices priced");

    let mut reversed = partial_with_model();
    merge(
        &mut reversed,
        LifecycleEvent::UsageObserved {
            event_id: "fanout-reversed".to_owned(),
            source: UsageSource::NonStreamBody,
            usage: UsageSnapshot {
                input_tokens: 3_000,
                output_tokens: 1_500,
                service_tier: Some("priority".to_owned()),
                pricing_slices: vec![priority_slice, default_slice],
                ..UsageSnapshot::default()
            },
        },
    );
    let reversed_cost = compute_cost(&reversed).expect("reversed slices priced");

    assert_eq!(forward_cost.input_micros, Some(10_000));
    assert_eq!(forward_cost.output_micros, Some(20_000));
    assert_eq!(forward_cost.total_micros, Some(30_000));
    assert_eq!(forward_cost, reversed_cost);
}

#[test]
fn compute_usage_slices_cost_uses_single_coherent_snapshot_regardless_of_global_swap() {
    use cc_lb_lifecycle::UsagePricingSlice;

    let _guard = crate::GLOBAL_TEST_LOCK
        .lock()
        .expect("global catalog test lock");

    let snapshot_with_rates = |base: u64, priority: u64| {
        let mut snap = CatalogSnapshot::empty_cost_disabled();
        snap.status = CatalogStatus::Ok;
        snap.models.insert(
            MODEL.to_owned(),
            Pricing {
                model: MODEL.to_owned(),
                input_per_million_usd: UsdPerMillion::from_whole_usd(base),
                output_per_million_usd: UsdPerMillion::from_whole_usd(base),
                input_image_per_million_usd: None,
                output_image_per_million_usd: None,
                native: None,
                by_tier: BTreeMap::from([(
                    "priority".to_owned(),
                    TierRate {
                        input_per_million_usd: UsdPerMillion::from_whole_usd(priority),
                        output_per_million_usd: UsdPerMillion::from_whole_usd(priority),
                    },
                )]),
            },
        );
        snap
    };

    let gen1 = snapshot_with_rates(1, 2);
    let gen2 = snapshot_with_rates(3, 6);
    crate::global_catalog().install_snapshot(gen2);

    let default_slice = UsagePricingSlice {
        input_tokens: 1_000_000,
        service_tier: Some("default".to_owned()),
        ..UsagePricingSlice::default()
    };
    let priority_slice = UsagePricingSlice {
        input_tokens: 1_000_000,
        service_tier: Some("priority".to_owned()),
        ..UsagePricingSlice::default()
    };

    // Snapshot gen1 stays coherent (1 + 2 = 3M micros) even while global_catalog() holds gen2.
    let gen1_forward = crate::compute_usage_slices_cost_in_snapshot(
        &gen1,
        MODEL,
        &[default_slice.clone(), priority_slice.clone()],
    );
    let gen1_reversed = crate::compute_usage_slices_cost_in_snapshot(
        &gen1,
        MODEL,
        &[priority_slice.clone(), default_slice.clone()],
    );
    assert_eq!(gen1_forward.total_micros, 3_000_000);
    assert_eq!(gen1_reversed.total_micros, 3_000_000);

    // Global lookup similarly uses gen2 coherently (3 + 6 = 9M micros) in both orders.
    let global_forward =
        crate::compute_usage_slices_cost(MODEL, &[default_slice.clone(), priority_slice.clone()]);
    let global_reversed = crate::compute_usage_slices_cost(MODEL, &[priority_slice, default_slice]);
    assert_eq!(global_forward.total_micros, 9_000_000);
    assert_eq!(global_reversed.total_micros, 9_000_000);
}
