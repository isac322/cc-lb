use super::*;

#[test]
fn exact_fable_uses_cost_first_trace_version() {
    let candidates = [
        healthy_fable_candidate("fable-a", 1),
        healthy_fable_candidate("fable-b", 2),
    ];
    let output = SubscriptionPreferenceFilter::new()
        .filter(
            &ctx_with_request_id(FABLE_MODEL, "fable-cost-first"),
            &principal(),
            &candidates,
        )
        .expect("builtin filter cannot fail");
    let trace = output.subscription_preference.expect("trace present");
    assert_eq!(output.kept_upstream_ids, vec![candidates[0].upstream_id]);
    assert_eq!(trace.wrh_key_source, WrhKeySource::CostFirst);
    assert_eq!(trace.rendezvous_salt_version, None);
    assert_eq!(trace.formula_version.as_deref(), Some("cost-first-v1"));
}

#[test]
fn non_fable_cost_first_is_independent_of_request_id() {
    let candidates = [
        healthy_oauth_candidate("non-fable-a", 1),
        healthy_oauth_candidate("non-fable-b", 2),
    ];
    let filter = SubscriptionPreferenceFilter::new();
    let output = filter
        .filter(
            &ctx_with_request_id(SONNET_MODEL, "first"),
            &principal(),
            &candidates,
        )
        .expect("builtin filter cannot fail");
    let trace = output.subscription_preference.expect("trace present");

    assert_eq!(output.kept_upstream_ids, vec![candidates[0].upstream_id]);
    assert_eq!(trace.rendezvous_salt_version, None);
    assert_eq!(
        filter
            .filter(
                &ctx_with_request_id(SONNET_MODEL, "second"),
                &principal(),
                &candidates
            )
            .expect("filter")
            .kept_upstream_ids,
        output.kept_upstream_ids
    );
}

#[test]
fn fable_overage_uses_cost_first_tiebreak() {
    let candidate = |name, seed| {
        oauth_at_t0(
            name,
            seed,
            vec![
                fresh(WINDOW_SEVEN_DAY_FABLE)
                    .util(1.0)
                    .status("rejected")
                    .build(),
                fresh(WINDOW_OVERAGE).util(0.5).status("allowed").build(),
            ],
        )
    };
    let candidates = [candidate("overage-a", 1), candidate("overage-b", 2)];
    let output = SubscriptionPreferenceFilter::new()
        .filter(
            &ctx_with_request_id(FABLE_MODEL, "fable-overage"),
            &principal(),
            &candidates,
        )
        .expect("builtin filter cannot fail");

    assert_eq!(output.kept_upstream_ids, vec![candidates[0].upstream_id]);
}

fn healthy_fable_candidate(name: &str, id_seed: u8) -> UpstreamCandidate {
    let mut candidate = healthy_oauth_candidate(name, id_seed);
    candidate.subscription_quotas.push(
        fresh(WINDOW_SEVEN_DAY_FABLE)
            .status("allowed")
            .util(0.10)
            .reset_at(T0_SECS + 604_800)
            .build(),
    );
    candidate
}
