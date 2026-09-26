use crate::common;
mod subscription_preference_v11_preview_support;

use cc_lb_domain::{CandidateUrgency, SubscriptionPreferenceTrace, SubscriptionTier};
use cc_lb_engine::lifecycle::PreviewRouteOutcome;
use http::StatusCode;
use uuid::Uuid;

use subscription_preference_v11_preview_support::{
    PreviewFixture, exhausted_shared_quota, fable_quota,
    fable_quota_with_unobserved_shared_seven_day, fable_quota_without_shared_seven_day,
    on_pace_quota, sonnet_quota, stale_exhausted_shared_quota, stale_overage_positive_quota,
    urgent_quota,
};

const REQUEST_ID: &str = "preview-v11-transition";

#[tokio::test]
async fn preview_selects_use_it_or_lose_it_candidate_and_traces_all_candidates() {
    // Given: one urgent and one on-pace OAuth candidate at a fixed instant.
    let fixture = PreviewFixture::new(urgent_quota(), on_pace_quota());

    // When: the real lifecycle preview surface evaluates the candidates.
    let before = fixture.preview(REQUEST_ID).await;
    let before_trace = subscription_trace(&before);
    let urgent_before = candidate(before_trace, fixture.urgent_id);
    let steady_before = candidate(before_trace, fixture.steady_id);

    // Then: the urgent candidate wins and every assessed candidate exposes v11 pressure fields.
    assert_eq!(before.winner_upstream_id, Some(fixture.urgent_id));
    assert_eq!(before.winner_upstream_name.as_deref(), Some("urgent"));
    assert_eq!(terminal_upstream(&before), Some(fixture.urgent_id));
    assert_eq!(
        before_trace.formula_version.as_deref(),
        Some("cost-first-v2")
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
    let after = fixture.preview(REQUEST_ID).await;
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

#[tokio::test]
async fn preview_all_on_pace_uses_deterministic_uniform_factor() {
    // Given: both OAuth candidates are on pace and the request id is fixed.
    let fixture = PreviewFixture::new(on_pace_quota(), on_pace_quota());

    // When: the real lifecycle preview surface evaluates identical state twice.
    let first = fixture.preview("preview-v11-uniform").await;
    let second = fixture.preview("preview-v11-uniform").await;
    let trace = subscription_trace(&first);

    // Then: v11 uses uniform neutral factors and a stable WRH winner.
    assert_eq!(first.winner_upstream_id, second.winner_upstream_id);
    assert_eq!(first.winner_upstream_name, second.winner_upstream_name);
    assert_eq!(trace.formula_version.as_deref(), Some("cost-first-v2"));
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

#[tokio::test]
async fn handle_base_warning_preserves_selection_until_rejected() {
    let mut urgent = on_pace_quota();
    let weekly = urgent
        .iter_mut()
        .find(|snapshot| snapshot.window == "7d")
        .expect("fixture has shared weekly quota");
    let now = weekly.observed_at_unix_millis.expect("observed quota") / 1_000;
    weekly.utilization = Some(0.98);
    weekly.resets_at_unix_secs = Some(now + 3_600);

    let mut steady = on_pace_quota();
    let weekly = steady
        .iter_mut()
        .find(|snapshot| snapshot.window == "7d")
        .expect("fixture has shared weekly quota");
    weekly.utilization = Some(0.5);
    weekly.resets_at_unix_secs = Some(now + 60 * 3_600);
    let fixture = PreviewFixture::new(urgent.clone(), steady);

    assert_eq!(fixture.handle_model("claude-test").await, StatusCode::OK);
    assert_eq!(fixture.dispatch_hosts(), vec!["urgent.invalid"]);

    // The provider changes only its warning signal, not capacity or reset.
    urgent
        .iter_mut()
        .find(|snapshot| snapshot.window == "7d")
        .expect("fixture has shared weekly quota")
        .status = Some("allowed_warning".to_owned());
    fixture.set_quota(fixture.urgent_id, urgent.clone());
    assert_eq!(
        fixture.preview("base-warning").await.winner_upstream_id,
        Some(fixture.urgent_id)
    );
    assert_eq!(fixture.handle_model("claude-test").await, StatusCode::OK);
    assert_eq!(
        fixture.dispatch_hosts(),
        vec!["urgent.invalid", "urgent.invalid"]
    );

    // A rejection still changes the selected upstream immediately.
    urgent
        .iter_mut()
        .find(|snapshot| snapshot.window == "7d")
        .expect("fixture has shared weekly quota")
        .status = Some("rejected".to_owned());
    fixture.set_quota(fixture.urgent_id, urgent);
    assert_eq!(
        fixture.preview("base-rejected").await.winner_upstream_id,
        Some(fixture.steady_id)
    );
    assert_eq!(fixture.handle_model("claude-test").await, StatusCode::OK);
    assert_eq!(
        fixture.dispatch_hosts(),
        vec!["urgent.invalid", "urgent.invalid", "steady.invalid"]
    );
}

#[tokio::test]
async fn handle_shared_7d_exhaustion_without_api_key_returns_503_without_dispatch() {
    // Given: both OAuth upstreams have healthy 5h quota but exhausted shared
    // weekly quota, and there is no API-key fallback.
    let fixture = PreviewFixture::new(exhausted_shared_quota(), exhausted_shared_quota());

    // When: the real proxy lifecycle handles a model request.
    let status = fixture.handle_model("claude-sonnet-4-5").await;

    // Then: the proxy fails locally instead of dispatching known-exhausted quota.
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(fixture.dispatch_hosts().is_empty());
}

#[tokio::test]
async fn handle_sonnet_excludes_observed_exhausted_scoped_upstream() {
    let fixture = PreviewFixture::with_statuses(
        sonnet_quota(Some(1.0)),
        sonnet_quota(Some(0.2)),
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::OK,
    );

    let status = fixture.handle_model("claude-sonnet-4-5").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixture.dispatch_hosts(), vec!["steady.invalid"]);
}

#[tokio::test]
async fn handle_sonnet_missing_scoped_window_is_noop() {
    let fixture = PreviewFixture::new(sonnet_quota(None), sonnet_quota(None));

    let status = fixture.handle_model("claude-sonnet-4-5").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixture.dispatch_hosts(), vec!["urgent.invalid"]);
}

#[tokio::test]
async fn handle_stale_exhausted_shared_quota_returns_503_without_dispatch() {
    let fixture = PreviewFixture::new(
        stale_exhausted_shared_quota(),
        stale_exhausted_shared_quota(),
    );

    let status = fixture.handle_model("claude-sonnet-4-5").await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(fixture.dispatch_hosts().is_empty());
}

#[tokio::test]
async fn handle_stale_overage_positive_returns_503_without_dispatch() {
    let fixture = PreviewFixture::new(
        stale_overage_positive_quota(),
        stale_overage_positive_quota(),
    );

    let status = fixture.handle_model("claude-sonnet-4-5").await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(fixture.dispatch_hosts().is_empty());
}

#[tokio::test]
async fn preview_fable_request_excludes_exhausted_scoped_quota() {
    // Given: shared quota is healthy on both OAuth upstreams, while only the
    // first upstream has exhausted its Fable-scoped weekly quota.
    let fixture = PreviewFixture::new(fable_quota(1.0), fable_quota(0.2));
    let outcome = fixture
        .preview_model("preview-fable-exhausted", "claude-fable-5")
        .await;
    let trace = subscription_trace(&outcome);

    // Then: the exhausted upstream is absent from the assessed candidates and
    // cannot survive through terminal selection.
    assert_eq!(outcome.winner_upstream_id, Some(fixture.steady_id));
    assert_eq!(terminal_upstream(&outcome), Some(fixture.steady_id));
    assert_eq!(trace.candidates.len(), 1);
    assert_eq!(trace.candidates[0].upstream_id, fixture.steady_id);
}

#[tokio::test]
async fn handle_fable_excludes_exhausted_scoped_upstream() {
    // Given: dispatching the exhausted first upstream would return 429, while
    // the second upstream has healthy Fable quota and returns 200.
    let fixture = PreviewFixture::with_statuses(
        fable_quota(1.0),
        fable_quota(0.2),
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::OK,
    );

    // When: the real proxy lifecycle handles a Fable request.
    let status = fixture.handle_model("claude-fable-5").await;

    // Then: only the healthy upstream is dispatched and the client sees 200.
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixture.dispatch_hosts(), vec!["steady.invalid"]);
}

#[tokio::test]
async fn handle_fable_without_shared_7d_uses_available_quota_windows() {
    // Given: a new-account quota shape exposes 5h and 7d_fable, while the
    // shared 7d window is absent from the upstream usage response.
    let fixture = PreviewFixture::new(
        fable_quota_without_shared_seven_day(),
        fable_quota_without_shared_seven_day(),
    );
    let preview = fixture
        .preview_model("preview-fable-without-shared-7d", "claude-fable-5")
        .await;
    let status = fixture.handle_model("claude-fable-5").await;

    // Then: the available quota set is complete for this upstream, and the
    // request reaches exactly one mock upstream without a missing-window error.
    assert_eq!(
        subscription_trace(&preview).chosen_tier,
        SubscriptionTier::KnownBase
    );
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixture.dispatch_hosts().len(), 1);
}
#[tokio::test]
async fn handle_fable_with_unobserved_shared_7d_remains_partial_base() {
    let fixture = PreviewFixture::new(
        fable_quota_with_unobserved_shared_seven_day(),
        fable_quota_with_unobserved_shared_seven_day(),
    );
    let preview = fixture
        .preview_model("preview-fable-unobserved-shared-7d", "claude-fable-5")
        .await;
    let status = fixture.handle_model("claude-fable-5").await;

    assert_eq!(
        subscription_trace(&preview).chosen_tier,
        SubscriptionTier::PartialBase
    );
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixture.dispatch_hosts().len(), 1);
}

#[tokio::test]
async fn handle_non_fable_ignores_fable_scoped_exhaustion() {
    // Given: the first upstream is exhausted only in the Fable-scoped window.
    let fixture = PreviewFixture::with_statuses(
        fable_quota(1.0),
        fable_quota(0.2),
        StatusCode::OK,
        StatusCode::OK,
    );

    // When: a non-Fable request uses the same quota snapshots.
    let status = fixture.handle_model("claude-sonnet-4-5").await;

    // Then: 7d_fable is ignored and deterministic first-pick remains unchanged.
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixture.dispatch_hosts(), vec!["urgent.invalid"]);
}

#[tokio::test]
async fn handle_fable_propagates_selected_upstream_429_without_retrying_exhausted() {
    // Given: the exhausted first upstream would return 200 if called, while the
    // healthy Fable candidate returns a provider 429.
    let fixture = PreviewFixture::with_statuses(
        fable_quota(1.0),
        fable_quota(0.2),
        StatusCode::OK,
        StatusCode::TOO_MANY_REQUESTS,
    );

    // When: the healthy candidate is selected but its provider rejects the request.
    let status = fixture.handle_model("claude-fable-5").await;

    // Then: the provider failure propagates without falling back to exhausted quota.
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(fixture.dispatch_hosts(), vec!["steady.invalid"]);
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
