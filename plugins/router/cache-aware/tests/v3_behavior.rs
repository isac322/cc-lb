use cache_aware_router::{CacheAwareConfig, filter_candidates};
use cc_lb_plugin_wire::v2::common::{CacheScoreWire, CandidateWire, Principal};
use cc_lb_plugin_wire::v3::filter::{FilterRequest, FilterResponse};
use std::collections::BTreeMap;
use uuid::Uuid;

#[test]
fn default_config_keeps_one_candidate_for_v2_single_pick_compatibility() {
    let response = filter_candidates(
        request(vec![candidate(1, None), candidate(2, Some(4096))]),
        CacheAwareConfig::default(),
    );

    assert_accepts(&response, &[2]);
    assert_reason(&response, 2, "top-K by cache_score");
    assert_reason(&response, 1, "below K by cache_score");
}

#[test]
fn keep_k_override_keeps_top_three_by_cache_score_and_read_tokens() {
    let response = filter_candidates(
        request(vec![
            candidate(1, None),
            candidate(2, Some(512)),
            candidate(3, Some(8192)),
            candidate(4, Some(0)),
            candidate(5, Some(4096)),
        ]),
        CacheAwareConfig::new(3),
    );

    assert_accepts(&response, &[2, 3, 5]);
    assert_rejects(&response, &[1, 4]);
}

#[test]
fn equal_scores_keep_first_candidates_in_input_order() {
    let response = filter_candidates(
        request(vec![
            candidate(1, Some(1024)),
            candidate(2, Some(1024)),
            candidate(3, Some(1024)),
            candidate(4, Some(1024)),
        ]),
        CacheAwareConfig::new(3),
    );

    assert_accepts(&response, &[1, 2, 3]);
    assert_rejects(&response, &[4]);
}

#[test]
fn keep_k_zero_clamps_to_one() {
    let response = filter_candidates(
        request(vec![candidate(1, Some(10)), candidate(2, Some(20))]),
        CacheAwareConfig::new(0),
    );

    assert_accepts(&response, &[2]);
    assert_rejects(&response, &[1]);
}

#[test]
fn keep_k_larger_than_candidates_keeps_all() {
    let response = filter_candidates(
        request(vec![candidate(1, None), candidate(2, Some(1))]),
        CacheAwareConfig::new(99),
    );

    assert_accepts(&response, &[1, 2]);
    assert_rejects(&response, &[]);
}

#[test]
fn empty_candidate_request_returns_empty_results() {
    let response = filter_candidates(request(vec![]), CacheAwareConfig::default());

    assert!(response.kept_upstream_ids.is_empty());
    assert!(response.per_candidate_reasons.is_empty());
}

#[test]
fn string_config_parser_defaults_and_clamps() {
    assert_eq!(CacheAwareConfig::from_keep_k_value(None).keep_k(), 1);
    assert_eq!(CacheAwareConfig::from_keep_k_value(Some("0")).keep_k(), 1);
    assert_eq!(CacheAwareConfig::from_keep_k_value(Some("3")).keep_k(), 3);
}

fn assert_accepts(response: &FilterResponse, expected: &[u128]) {
    let accepted = response.kept_upstream_ids.clone();
    let expected = expected.iter().copied().map(uuid).collect::<Vec<_>>();
    assert_eq!(accepted, expected);
}

fn assert_rejects(response: &FilterResponse, expected: &[u128]) {
    let rejected: Vec<Uuid> = response
        .per_candidate_reasons
        .iter()
        .filter(|result| !result.kept)
        .map(|result| result.upstream_id)
        .collect();
    let expected = expected.iter().copied().map(uuid).collect::<Vec<_>>();
    assert_eq!(rejected, expected);
}

fn assert_reason(response: &FilterResponse, upstream_id: u128, expected: &str) {
    let result = response
        .per_candidate_reasons
        .iter()
        .find(|result| result.upstream_id == uuid(upstream_id))
        .expect("result for upstream");
    assert_eq!(result.reason, expected);
}

fn request(candidates: Vec<CandidateWire>) -> FilterRequest {
    FilterRequest {
        request_id: "req-filter".to_owned(),
        headers: Vec::new(),
        method: "POST".to_owned(),
        path: "/v1/messages".to_owned(),
        query: None,
        body_base64: "e30=".to_owned(),
        principal: Principal::dry_run_sample(),
        candidates,
    }
}

fn candidate(upstream_id: u128, predicted_cache_read_tokens: Option<u32>) -> CandidateWire {
    CandidateWire {
        upstream_id: uuid(upstream_id).to_string(),
        name: format!("candidate-{upstream_id}"),
        kind: "anthropic_api_key".to_owned(),
        observed_rate_limits: Vec::new(),
        subscription_quotas: Vec::new(),
        observed_at_unix_secs: 0,
        cache_score: predicted_cache_read_tokens.map(cache_score_wire),
    }
}

fn cache_score_wire(predicted_cache_read_tokens: u32) -> CacheScoreWire {
    CacheScoreWire {
        predicted_cache_read_tokens,
        predicted_cache_creation_tokens_5m: 0,
        predicted_cache_creation_tokens_1h: 0,
        predicted_uncached_input_tokens: 0,
        predicted_expires_at_unix_secs: None,
        matched_breakpoint_index: if predicted_cache_read_tokens > 0 {
            Some(0)
        } else {
            None
        },
        confidence: 1.0,
        ambiguity_reason: None,
    }
}

fn decisions_by_upstream(response: &FilterResponse) -> BTreeMap<Uuid, bool> {
    response
        .per_candidate_reasons
        .iter()
        .map(|result| (result.upstream_id, result.kept))
        .collect()
}

#[test]
fn response_contains_one_decision_for_each_candidate() {
    let response = filter_candidates(
        request(vec![candidate(1, Some(1)), candidate(2, None)]),
        CacheAwareConfig::default(),
    );

    assert_eq!(decisions_by_upstream(&response).len(), 2);
    assert!(decisions_by_upstream(&response).contains_key(&uuid(1)));
    assert!(decisions_by_upstream(&response).contains_key(&uuid(2)));
}

fn uuid(value: u128) -> Uuid {
    Uuid::from_u128(value)
}
