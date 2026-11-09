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
        inserted_at: Some(Instant::now()),
        model: Some(MODEL.to_owned()),
        ..Partial::default()
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
