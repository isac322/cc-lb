use cache_aware_router::{CacheAwareConfig, filter_candidates};
use cc_lb_plugin_wire::v2::common::{CacheScoreWire, CandidateWire, Principal};
use cc_lb_plugin_wire::v3::filter::{FilterRequest, FilterResponse};
use std::collections::BTreeMap;

#[test]
fn default_config_keeps_one_candidate_for_v2_single_pick_compatibility() {
    let response = filter_candidates(
        request(vec![candidate("cold", None), candidate("warm", Some(4096))]),
        CacheAwareConfig::default(),
    );

    assert_accepts(&response, &["warm"]);
    assert_reason(&response, "warm", "top-K by cache_score");
    assert_reason(&response, "cold", "below K by cache_score");
}

#[test]
fn keep_k_override_keeps_top_three_by_cache_score_and_read_tokens() {
    let response = filter_candidates(
        request(vec![
            candidate("cold-missing", None),
            candidate("warm-small", Some(512)),
            candidate("warm-large", Some(8192)),
            candidate("cold-zero", Some(0)),
            candidate("warm-medium", Some(4096)),
        ]),
        CacheAwareConfig::new(3),
    );

    assert_accepts(&response, &["warm-small", "warm-large", "warm-medium"]);
    assert_rejects(&response, &["cold-missing", "cold-zero"]);
}

#[test]
fn equal_scores_keep_first_candidates_in_input_order() {
    let response = filter_candidates(
        request(vec![
            candidate("first", Some(1024)),
            candidate("second", Some(1024)),
            candidate("third", Some(1024)),
            candidate("fourth", Some(1024)),
        ]),
        CacheAwareConfig::new(3),
    );

    assert_accepts(&response, &["first", "second", "third"]);
    assert_rejects(&response, &["fourth"]);
}

#[test]
fn keep_k_zero_clamps_to_one() {
    let response = filter_candidates(
        request(vec![
            candidate("smaller", Some(10)),
            candidate("larger", Some(20)),
        ]),
        CacheAwareConfig::new(0),
    );

    assert_accepts(&response, &["larger"]);
    assert_rejects(&response, &["smaller"]);
}

#[test]
fn keep_k_larger_than_candidates_keeps_all() {
    let response = filter_candidates(
        request(vec![candidate("a", None), candidate("b", Some(1))]),
        CacheAwareConfig::new(99),
    );

    assert_accepts(&response, &["a", "b"]);
    assert_rejects(&response, &[]);
}

#[test]
fn empty_candidate_request_returns_empty_results() {
    let response = filter_candidates(request(vec![]), CacheAwareConfig::default());

    assert!(response.results.is_empty());
}

#[test]
fn string_config_parser_defaults_and_clamps() {
    assert_eq!(CacheAwareConfig::from_keep_k_value(None).keep_k(), 1);
    assert_eq!(CacheAwareConfig::from_keep_k_value(Some("0")).keep_k(), 1);
    assert_eq!(CacheAwareConfig::from_keep_k_value(Some("3")).keep_k(), 3);
}

fn assert_accepts(response: &FilterResponse, expected: &[&str]) {
    let accepted: Vec<&str> = response
        .results
        .iter()
        .filter(|result| result.decision == "accept")
        .map(|result| result.upstream_id.as_str())
        .collect();
    assert_eq!(accepted, expected);
}

fn assert_rejects(response: &FilterResponse, expected: &[&str]) {
    let rejected: Vec<&str> = response
        .results
        .iter()
        .filter(|result| result.decision == "reject")
        .map(|result| result.upstream_id.as_str())
        .collect();
    assert_eq!(rejected, expected);
}

fn assert_reason(response: &FilterResponse, upstream_id: &str, expected: &str) {
    let result = response
        .results
        .iter()
        .find(|result| result.upstream_id == upstream_id)
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

fn candidate(upstream_id: &str, predicted_cache_read_tokens: Option<u32>) -> CandidateWire {
    CandidateWire {
        upstream_id: upstream_id.to_owned(),
        name: upstream_id.to_owned(),
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

fn decisions_by_upstream(response: &FilterResponse) -> BTreeMap<&str, &str> {
    response
        .results
        .iter()
        .map(|result| (result.upstream_id.as_str(), result.decision.as_str()))
        .collect()
}

#[test]
fn response_contains_one_decision_for_each_candidate() {
    let response = filter_candidates(
        request(vec![candidate("a", Some(1)), candidate("b", None)]),
        CacheAwareConfig::default(),
    );

    assert_eq!(decisions_by_upstream(&response).len(), 2);
    assert!(decisions_by_upstream(&response).contains_key("a"));
    assert!(decisions_by_upstream(&response).contains_key("b"));
}
