//! Synthetic quotas: equal five-hour windows leave weekly expiry to decide.
use reset_priority_wasmtime::algorithm::{
    self, CandidateQuotas, CandidateView, DataState, QuotaWindow, RequestView,
};

#[test]
fn earlier_weekly_expiry_with_more_headroom_wins() {
    let now = 1_700_000_000;
    let window = |utilization, reset_in| QuotaWindow {
        state: DataState::Fresh,
        utilization: Some(utilization),
        resets_at_unix_secs: Some(now + reset_in),
        observed_at_unix_secs: Some(now),
        rejected: false,
    };
    let five_hour = window(0.0, 600);
    let later = window(0.8, 100_000);
    let sooner = window(0.1, 25_000);
    let candidate = |id, weekly| CandidateView {
        upstream_id: id,
        kind: algorithm::KIND_ANTHROPIC_OAUTH,
        observed_at_unix_secs: 0,
        predicted_cache_read_tokens: 0,
        quotas: CandidateQuotas {
            five_hour: Some(&five_hour),
            shared_weekly: Some(weekly),
            model_weekly: None,
        },
    };
    let decision = algorithm::decide(&RequestView {
        thread_id: None,
        now_unix_secs: now,
        candidates: &[candidate("later", &later), candidate("sooner", &sooner)],
    });
    assert_eq!(decision.accepted, vec!["sooner"]);
}
