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
                model: MODEL.to_owned(),
                input_per_million_usd: UsdPerMillion::from_whole_usd(2),
                output_per_million_usd: UsdPerMillion::from_whole_usd(8),
                by_tier: BTreeMap::<String, TierRate>::new(),
            },
        )]),
        raw_json: b"{}".to_vec(),
        cache_creation_per_million_usd: HashMap::new(),
        cache_read_per_million_usd: HashMap::new(),
        cache_creation_per_million_usd_by_tier: HashMap::new(),
        cache_read_per_million_usd_by_tier: HashMap::new(),
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
