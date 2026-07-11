use super::*;

const V11_FABLE_RENDEZVOUS_SALT_ORACLE: &str =
    "cc-lb:subscription-preference:v11-fable:use-it-or-lose-it-fable-quota-pressure:2026-07-11";

#[test]
fn exact_fable_uses_scoped_salt_and_trace_version() {
    let candidates = [
        healthy_fable_candidate("fable-a", 1),
        healthy_fable_candidate("fable-b", 2),
    ];
    let request_id = "fable-scoped-salt";

    let output = SubscriptionPreferenceFilter::new()
        .filter(
            &ctx_with_request_id(FABLE_MODEL, request_id),
            &principal(),
            &candidates,
        )
        .expect("builtin filter cannot fail");
    let trace = output.subscription_preference.expect("trace present");
    let expected = candidates
        .iter()
        .max_by_key(|candidate| {
            rendezvous_hash(
                V11_FABLE_RENDEZVOUS_SALT_ORACLE,
                request_id,
                candidate.upstream_id,
            )
        })
        .expect("candidate fixture is non-empty");

    assert_eq!(FABLE_RENDEZVOUS_SALT, V11_FABLE_RENDEZVOUS_SALT_ORACLE);
    assert!(FABLE_RENDEZVOUS_SALT.contains(FABLE_SALT_VERSION));
    assert_eq!(output.kept_upstream_ids, vec![expected.upstream_id]);
    assert_eq!(
        trace.rendezvous_salt_version.as_deref(),
        Some(FABLE_SALT_VERSION)
    );
}

#[test]
fn non_fable_v11_winner_hash_and_trace_remain_unchanged() {
    let candidates = [
        healthy_oauth_candidate("non-fable-a", 1),
        healthy_oauth_candidate("non-fable-b", 2),
    ];
    let request_id = "non-fable-v11-invariance";
    let first_hash = rendezvous_hash(
        V11_RENDEZVOUS_SALT_ORACLE,
        request_id,
        candidates[0].upstream_id,
    );
    let second_hash = rendezvous_hash(
        V11_RENDEZVOUS_SALT_ORACLE,
        request_id,
        candidates[1].upstream_id,
    );

    let output = SubscriptionPreferenceFilter::new()
        .filter(
            &ctx_with_request_id(SONNET_MODEL, request_id),
            &principal(),
            &candidates,
        )
        .expect("builtin filter cannot fail");
    let trace = output.subscription_preference.expect("trace present");

    assert_eq!(first_hash, 15_808_380_847_118_007_777);
    assert_eq!(second_hash, 13_471_125_173_849_752_455);
    assert_eq!(output.kept_upstream_ids, vec![candidates[0].upstream_id]);
    assert_eq!(trace.rendezvous_salt_version.as_deref(), Some(SALT_VERSION));
}

#[test]
fn fable_overage_selection_keeps_v10_salt() {
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
    let request_id = "fable-overage-v10";
    let expected = candidates
        .iter()
        .max_by_key(|candidate| {
            rendezvous_hash(
                V10_RENDEZVOUS_SALT_ORACLE,
                request_id,
                candidate.upstream_id,
            )
        })
        .expect("candidate fixture is non-empty");

    let output = SubscriptionPreferenceFilter::new()
        .filter(
            &ctx_with_request_id(FABLE_MODEL, request_id),
            &principal(),
            &candidates,
        )
        .expect("builtin filter cannot fail");

    assert_eq!(output.kept_upstream_ids, vec![expected.upstream_id]);
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
