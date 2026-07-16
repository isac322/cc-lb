use crate::common;
mod subscription_preference_v11_preview_support;

use cc_lb_domain::{CandidateUrgency, SubscriptionPreferenceTrace};
use cc_lb_engine::lifecycle::PreviewRouteOutcome;
use uuid::Uuid;

use subscription_preference_v11_preview_support::{PreviewFixture, on_pace_quota, urgent_quota};

const REQUEST_ID: &str = "preview-v11-transition";

#[test]
fn preview_selects_use_it_or_lose_it_candidate_and_traces_all_candidates() {
    // Given: one urgent and one on-pace OAuth candidate at a fixed instant.
    let fixture = PreviewFixture::new(urgent_quota(), on_pace_quota());

    // When: the real lifecycle preview surface evaluates the candidates.
    let before = fixture.preview(REQUEST_ID);
    let before_trace = subscription_trace(&before);
    let urgent_before = candidate(before_trace, fixture.urgent_id);
    let steady_before = candidate(before_trace, fixture.steady_id);

    // Then: the urgent candidate wins and every assessed candidate exposes v11 pressure fields.
    assert_eq!(before.winner_upstream_id, Some(fixture.urgent_id));
    assert_eq!(before.winner_upstream_name.as_deref(), Some("urgent"));
    assert_eq!(terminal_upstream(&before), Some(fixture.urgent_id));
    assert_eq!(
        before_trace.formula_version.as_deref(),
        Some("cost-first-v1")
    );
    assert_eq!(before_trace.candidates.len(), 2);
    assert!(urgent_before.quota_urgency_5h.expect("urgent 5h") > 0.0);
    assert!(urgent_before.quota_urgency_7d.expect("urgent 7d") > 0.0);
    assert_ne!(
        urgent_before.quota_urgency_5h,
        urgent_before.quota_urgency_7d
    );
    assert_eq!(
        urgent_before.quota_urgency_combined,
        Some(urgent_before.quota_urgency)
    );
    assert_eq!(steady_before.quota_urgency_5h, Some(0.0));
    assert_eq!(steady_before.quota_urgency_7d, Some(0.0));
    assert_eq!(steady_before.quota_urgency_combined, Some(0.0));
    assert_ne!(
        urgent_before.quota_urgency_combined,
        steady_before.quota_urgency_combined
    );
    assert_weight_fields(urgent_before, false);
    assert_weight_fields(steady_before, false);
    print_outcome("point_in_time", &before);

    // Given: only the synthetic utilization/reset state changes, swapping urgency.
    fixture.swap_quota_states();

    // When: the same request id is previewed again.
    let after = fixture.preview(REQUEST_ID);
    let after_trace = subscription_trace(&after);
    let urgent_after = candidate(after_trace, fixture.urgent_id);
    let steady_after = candidate(after_trace, fixture.steady_id);

    // Then: selection and per-candidate pressures change exactly with the state transition.
    assert_eq!(after.winner_upstream_id, Some(fixture.steady_id));
    assert_eq!(after.winner_upstream_name.as_deref(), Some("steady"));
    assert_eq!(terminal_upstream(&after), Some(fixture.steady_id));
    assert_eq!(urgent_after.quota_urgency_combined, Some(0.0));
    assert_eq!(
        steady_after.quota_urgency_combined,
        urgent_before.quota_urgency_combined
    );
    assert_eq!(
        urgent_after.quota_urgency_combined,
        steady_before.quota_urgency_combined
    );
    assert_weight_fields(urgent_after, false);
    assert_weight_fields(steady_after, false);
    print_outcome("after_transition", &after);
}

#[test]
fn preview_all_on_pace_uses_deterministic_uniform_factor() {
    // Given: both OAuth candidates are on pace and the request id is fixed.
    let fixture = PreviewFixture::new(on_pace_quota(), on_pace_quota());

    // When: the real lifecycle preview surface evaluates identical state twice.
    let first = fixture.preview("preview-v11-uniform");
    let second = fixture.preview("preview-v11-uniform");
    let trace = subscription_trace(&first);

    // Then: v11 uses uniform neutral factors and a stable WRH winner.
    assert_eq!(first.winner_upstream_id, second.winner_upstream_id);
    assert_eq!(first.winner_upstream_name, second.winner_upstream_name);
    assert_eq!(trace.formula_version.as_deref(), Some("cost-first-v1"));
    assert_eq!(trace.candidates.len(), 2);
    for assessed in &trace.candidates {
        assert_eq!(assessed.quota_urgency_5h, Some(0.0));
        assert_eq!(assessed.quota_urgency_7d, Some(0.0));
        assert_eq!(assessed.quota_urgency_combined, Some(0.0));
        assert_eq!(assessed.quota_urgency, 0.0);
        assert_weight_fields(assessed, true);
    }
    print_outcome("uniform_first", &first);
    print_outcome("uniform_second", &second);
}

fn subscription_trace(outcome: &PreviewRouteOutcome) -> &SubscriptionPreferenceTrace {
    outcome
        .trace
        .stages
        .iter()
        .find_map(|stage| stage.subscription_preference.as_ref())
        .expect("subscription preference trace")
}

fn candidate(trace: &SubscriptionPreferenceTrace, upstream_id: Uuid) -> &CandidateUrgency {
    trace
        .candidates
        .iter()
        .find(|assessed| assessed.upstream_id == upstream_id)
        .expect("candidate appears in trace")
}

fn terminal_upstream(outcome: &PreviewRouteOutcome) -> Option<Uuid> {
    outcome
        .trace
        .terminal_decision
        .as_ref()
        .and_then(|decision| decision.upstream_id)
}

fn assert_weight_fields(candidate: &CandidateUrgency, uniform: bool) {
    if uniform {
        assert_eq!(candidate.quota_urgency, 0.0);
    } else {
        assert!(candidate.quota_urgency >= 0.0);
    }
    assert_eq!(candidate.warning_multiplier, 1.0);
}

fn print_outcome(label: &str, outcome: &PreviewRouteOutcome) {
    println!(
        "{label}: selected_upstream={:?} trace={}",
        outcome.winner_upstream_id,
        serde_json::to_string(&outcome.trace).expect("routing trace serializes")
    );
}
