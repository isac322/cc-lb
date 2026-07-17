use std::collections::BTreeMap;

use crate::planner::{PlanInput, Profile, plan};
use crate::traffic::ExpectedLabel;

#[test]
fn request_mix_within_bounds() {
    // Given: a deterministic smoke plan that covers the complete request recipe cycle.
    let manifest = plan(PlanInput::new(777, Profile::Smoke, 0)).expect("plan builds");

    // When: requests are grouped by their expected outcome label.
    let labels = manifest
        .requests
        .iter()
        .fold(BTreeMap::new(), |mut counts, request| {
            *counts.entry(request.expected_label).or_insert(0_usize) += 1;
            counts
        });

    // Then: healthy traffic is the majority while each planned exceptional class is present.
    assert!(labels[&ExpectedLabel::Healthy] * 2 >= manifest.requests.len());
    for label in [
        ExpectedLabel::ProviderError,
        ExpectedLabel::ClientCancel,
        ExpectedLabel::Malformed,
        ExpectedLabel::Unsupported,
    ] {
        assert!(labels.contains_key(&label));
    }
}

#[test]
fn prefix_reuse_cardinality() {
    // Given: a deterministic smoke plan with cache-oriented body recipes.
    let manifest = plan(PlanInput::new(777, Profile::Smoke, 0)).expect("plan builds");

    // When: requests are grouped by their hot prefix and exact body hash.
    let prefixes = manifest
        .requests
        .iter()
        .fold(BTreeMap::new(), |mut counts, request| {
            *counts
                .entry(request.hot_prefix_group.as_str())
                .or_insert(0_usize) += 1;
            counts
        });
    let bodies = manifest
        .requests
        .iter()
        .fold(BTreeMap::new(), |mut counts, request| {
            *counts.entry(request.body_hash.as_str()).or_insert(0_usize) += 1;
            counts
        });

    // Then: a shared hot prefix and an exact duplicate body are both materialized.
    assert!(prefixes.values().any(|count| *count >= 3));
    assert!(bodies.values().any(|count| *count >= 2));
}

#[test]
fn principal_skew_within_bounds() {
    // Given: a deterministic smoke plan with a weighted principal distribution.
    let manifest = plan(PlanInput::new(777, Profile::Smoke, 0)).expect("plan builds");

    // When: requests are grouped by their authenticated principal.
    let principals = manifest
        .requests
        .iter()
        .fold(BTreeMap::new(), |mut counts, request| {
            *counts
                .entry(request.principal_id.as_str())
                .or_insert(0_usize) += 1;
            counts
        });
    let primary = principals["stress-primary"];
    let total = manifest.requests.len();

    // Then: the primary is dominant but every configured principal remains exercised.
    assert!((50..=80).contains(&(primary * 100 / total)));
    assert_eq!(principals.len(), manifest.principals.len());
}
