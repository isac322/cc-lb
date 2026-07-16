use anyhow::Result;
use cc_lb_storage_api::RequestEvent;
use serde_json::{Value, json};
use uuid::Uuid;

pub(crate) const SELECTED_EVENT_ID: &str = "0193a7b8-1234-7e2f-9012-quota0000001";
pub(crate) const MISMATCH_EVENT_ID: &str = "0193a7b8-1234-7e2f-9012-quota0000002";
pub(crate) const HISTORICAL_EVENT_ID: &str = "0193a7b8-1234-7e2f-9012-quota0000003";
pub(crate) const SELECTED_UPSTREAM_ID: Uuid = Uuid::from_u128(0x1111);
pub(crate) const LOSING_UPSTREAM_ID: Uuid = Uuid::from_u128(0x2222);
pub(crate) const MISMATCH_UPSTREAM_ID: Uuid = Uuid::from_u128(0x3333);

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct QuotaColumns {
    pub(crate) urgency_5h: f64,
    pub(crate) urgency_7d: f64,
    pub(crate) urgency_combined: f64,
    pub(crate) warning_multiplier: f64,
}

pub(crate) const SELECTED_QUOTA: QuotaColumns = QuotaColumns {
    urgency_5h: 0.125,
    urgency_7d: 0.75,
    urgency_combined: 0.8,
    warning_multiplier: 0.2,
};

pub(crate) const LOSING_QUOTA: QuotaColumns = QuotaColumns {
    urgency_5h: 0.5,
    urgency_7d: 0.25,
    urgency_combined: 0.55,
    warning_multiplier: 1.0,
};

pub(crate) fn populated_event() -> Result<RequestEvent> {
    Ok(RequestEvent {
        ts: 1_900_500_100,
        request_id: "quota-columns-populated".to_owned(),
        event_id: Some(SELECTED_EVENT_ID.to_owned()),
        upstream_id: Some(SELECTED_UPSTREAM_ID),
        formula_winner_upstream_id: Some(SELECTED_UPSTREAM_ID),
        kept_upstream_id: Some(SELECTED_UPSTREAM_ID),
        status: 200,
        duration_ms: 17,
        routing_trace: Some(serde_json::from_value(routing_trace_json(
            SELECTED_UPSTREAM_ID,
        ))?),
        quota_urgency_5h: Some(SELECTED_QUOTA.urgency_5h),
        quota_urgency_7d: Some(SELECTED_QUOTA.urgency_7d),
        quota_urgency_combined: Some(SELECTED_QUOTA.urgency_combined),
        quota_warning_multiplier: Some(SELECTED_QUOTA.warning_multiplier),
        ..Default::default()
    })
}

pub(crate) fn mismatch_event() -> Result<RequestEvent> {
    Ok(RequestEvent {
        ts: 1_900_500_101,
        request_id: "quota-columns-mismatch".to_owned(),
        event_id: Some(MISMATCH_EVENT_ID.to_owned()),
        upstream_id: Some(MISMATCH_UPSTREAM_ID),
        formula_winner_upstream_id: Some(SELECTED_UPSTREAM_ID),
        kept_upstream_id: Some(MISMATCH_UPSTREAM_ID),
        status: 200,
        duration_ms: 19,
        routing_trace: Some(serde_json::from_value(routing_trace_json(
            MISMATCH_UPSTREAM_ID,
        ))?),
        ..Default::default()
    })
}

pub(crate) fn historical_event() -> RequestEvent {
    RequestEvent {
        ts: 1_900_500_102,
        request_id: "quota-columns-historical".to_owned(),
        event_id: Some(HISTORICAL_EVENT_ID.to_owned()),
        status: 200,
        duration_ms: 23,
        ..Default::default()
    }
}

pub(crate) fn assert_populated_event(event: &RequestEvent) {
    assert_eq!(event.quota_urgency_5h, Some(SELECTED_QUOTA.urgency_5h));
    assert_eq!(event.quota_urgency_7d, Some(SELECTED_QUOTA.urgency_7d));
    assert_eq!(
        event.quota_urgency_combined,
        Some(SELECTED_QUOTA.urgency_combined)
    );
    assert_eq!(
        event.quota_warning_multiplier,
        Some(SELECTED_QUOTA.warning_multiplier)
    );

    let preference = event
        .routing_trace
        .as_ref()
        .and_then(|trace| trace.stages.first())
        .and_then(|stage| stage.subscription_preference.as_ref())
        .expect("v11 subscription-preference trace");
    assert_eq!(preference.candidates.len(), 2);
    assert_ne!(SELECTED_QUOTA, LOSING_QUOTA);
    assert_eq!(preference.candidates[0].upstream_id, LOSING_UPSTREAM_ID);
    assert_eq!(
        preference.candidates[0].quota_urgency_combined,
        Some(LOSING_QUOTA.urgency_combined)
    );
    assert_eq!(preference.candidates[1].upstream_id, SELECTED_UPSTREAM_ID);
    assert_eq!(
        preference.candidates[1].quota_urgency_combined,
        Some(SELECTED_QUOTA.urgency_combined)
    );
}

pub(crate) fn assert_mismatch_event(event: &RequestEvent) {
    let trace = event
        .routing_trace
        .as_ref()
        .expect("v11 mismatch routing trace");
    let terminal_upstream_id = trace
        .terminal_decision
        .as_ref()
        .and_then(|terminal| terminal.upstream_id)
        .expect("mismatch terminal upstream");
    let candidates = &trace
        .stages
        .first()
        .and_then(|stage| stage.subscription_preference.as_ref())
        .expect("mismatch subscription-preference trace")
        .candidates;
    assert_eq!(event.upstream_id, Some(terminal_upstream_id));
    assert!(
        candidates
            .iter()
            .all(|candidate| candidate.upstream_id != terminal_upstream_id)
    );
    assert!(event.quota_urgency_5h.is_none());
    assert!(event.quota_urgency_7d.is_none());
    assert!(event.quota_urgency_combined.is_none());
    assert!(event.quota_warning_multiplier.is_none());
}

fn routing_trace_json(terminal_upstream_id: Uuid) -> Value {
    json!({
        "stages": [{
            "stage_name": "subscription_preference",
            "upstream_id": SELECTED_UPSTREAM_ID,
            "subscription_preference": {
                "chosen_tier": "known_base",
                "candidates": [
                    candidate_json(LOSING_UPSTREAM_ID, LOSING_QUOTA),
                    candidate_json(SELECTED_UPSTREAM_ID, SELECTED_QUOTA)
                ],
                "previous_tier": null,
                "formula_winner_upstream_id": SELECTED_UPSTREAM_ID,
                "kept_upstream_id": terminal_upstream_id
            }
        }],
        "terminal_decision": {
            "upstream_id": terminal_upstream_id,
            "strategy": "first-pick"
        }
    })
}

fn candidate_json(upstream_id: Uuid, quota: QuotaColumns) -> Value {
    json!({
        "upstream_id": upstream_id,
        "tier": "known_base",
        "quota_urgency": quota.urgency_combined,
        "quota_urgency_5h": quota.urgency_5h,
        "quota_urgency_7d": quota.urgency_7d,
        "quota_urgency_combined": quota.urgency_combined,
        "predicted_cache_read_tokens": 100,
        "predicted_cache_creation_tokens_5m": 20,
        "predicted_cache_creation_tokens_1h": 10,
        "predicted_uncached_input_tokens": 30,
        "cache_ratio": 0.25,
        "warning_multiplier": quota.warning_multiplier,
        "cache_savings_ratio": 0.5,
        "estimated_input_cost_micros": 42,
    })
}
