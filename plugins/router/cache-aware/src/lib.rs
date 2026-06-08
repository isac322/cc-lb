use cc_lb_plugin_wire::v2::{
    common::{CacheScoreWire, CandidateWire, DialectBinding, UpstreamWire},
    route::RouteResponse,
};
use std::sync::atomic::{AtomicUsize, Ordering};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn round_robin_index(counter: &AtomicUsize, candidate_count: usize) -> Option<usize> {
    if candidate_count == 0 {
        return None;
    }
    let current = counter.fetch_add(1, Ordering::Relaxed);
    Some(current % candidate_count)
}

pub fn cache_score(cache_score: Option<&CacheScoreWire>) -> u32 {
    match cache_score {
        Some(score) if score.predicted_cache_read_tokens > 0 => 1,
        _ => 0,
    }
}

pub fn predicted_cache_read_tokens(cache_score: Option<&CacheScoreWire>) -> u32 {
    cache_score
        .map(|score| score.predicted_cache_read_tokens)
        .unwrap_or(0)
}

pub fn select_candidate_index(
    counter: &AtomicUsize,
    candidates: &[CandidateWire],
) -> Option<usize> {
    if candidates.is_empty() {
        return None;
    }

    let max_score = candidates
        .iter()
        .map(|candidate| cache_score(candidate.cache_score.as_ref()))
        .max()
        .unwrap_or(0);

    if max_score == 0 {
        return round_robin_index(counter, candidates.len());
    }

    let max_read_tokens = candidates
        .iter()
        .filter(|candidate| cache_score(candidate.cache_score.as_ref()) == max_score)
        .map(|candidate| predicted_cache_read_tokens(candidate.cache_score.as_ref()))
        .max()
        .unwrap_or(0);

    let tied_indices: Vec<usize> = candidates
        .iter()
        .enumerate()
        .filter(|(_, candidate)| {
            cache_score(candidate.cache_score.as_ref()) == max_score
                && predicted_cache_read_tokens(candidate.cache_score.as_ref()) == max_read_tokens
        })
        .map(|(index, _)| index)
        .collect();

    if tied_indices.len() == 1 {
        return tied_indices.first().copied();
    }

    round_robin_index(counter, tied_indices.len()).map(|index| tied_indices[index])
}

fn route_response(upstream_id: Option<String>) -> RouteResponse {
    RouteResponse {
        upstream_id,
        dialect: DialectBinding::SelfReferenced,
        upstream: UpstreamWire::AnthropicDirect,
    }
}

#[cc_lb_pdk::plugin(name = "cache-aware", version = "0.1.0")]
mod plugin {
    use super::{COUNTER, route_response, select_candidate_index};
    use cc_lb_plugin_wire::v2::{
        common::UpstreamWire,
        normalize_error::{NormalizeErrorRequest, NormalizeErrorResponse},
        route::{RouteRequest, RouteResponse},
        shape::{ShapeRequest, ShapeResponse},
    };
    use std::convert::Infallible;

    #[cc_lb_pdk::handler(name = "route", versions = [1])]
    pub(super) fn route_handler(request: RouteRequest) -> Result<RouteResponse, Infallible> {
        let upstream_id = select_candidate_index(&COUNTER, &request.candidates)
            .map(|idx| request.candidates[idx].upstream_id.clone());

        Ok(route_response(upstream_id))
    }

    #[cc_lb_pdk::handler(name = "shape", versions = [1])]
    pub(super) fn shape_handler(request: ShapeRequest) -> Result<ShapeResponse, Infallible> {
        let base_url = match &request.upstream {
            UpstreamWire::AnthropicDirect => "https://api.anthropic.com".to_string(),
        };
        let query_part = request
            .request
            .query
            .as_ref()
            .map(|query| format!("?{}", query))
            .unwrap_or_default();

        Ok(ShapeResponse {
            url: format!("{}{}{}", base_url, request.request.path, query_part),
            method: request.request.method,
            headers: request.request.headers,
            body_base64: request.request.body_base64,
        })
    }

    #[cc_lb_pdk::handler(name = "normalize_error", versions = [1])]
    pub(super) fn normalize_error_handler(
        _request: NormalizeErrorRequest,
    ) -> Result<NormalizeErrorResponse, Infallible> {
        Ok(NormalizeErrorResponse { body_base64: None })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_plugin_api::PluginManifest;
    use cc_lb_plugin_wire::v2::{
        common::{CacheScoreWire, CandidateWire, Principal, UpstreamWire},
        route::RouteRequest,
    };
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn route_picks_warm_candidate() {
        let _guard = TEST_LOCK.lock().expect("test lock");
        COUNTER.store(0, Ordering::Relaxed);

        let response = plugin::route_handler(route_request(vec![
            candidate("cold", None),
            candidate("warm", Some(4096)),
        ]))
        .expect("route ok");

        assert_eq!(response.upstream_id.as_deref(), Some("warm"));
        assert_eq!(response.dialect, DialectBinding::SelfReferenced);
        assert_eq!(response.upstream, UpstreamWire::AnthropicDirect);
        assert_eq!(COUNTER.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn route_falls_through_to_rr_when_all_cold() {
        let _guard = TEST_LOCK.lock().expect("test lock");
        COUNTER.store(0, Ordering::Relaxed);

        let first = plugin::route_handler(route_request(vec![
            candidate("cold-1", None),
            candidate("cold-2", Some(0)),
        ]))
        .expect("route ok");
        let second = plugin::route_handler(route_request(vec![
            candidate("cold-1", None),
            candidate("cold-2", Some(0)),
        ]))
        .expect("route ok");

        assert_eq!(first.upstream_id.as_deref(), Some("cold-1"));
        assert_eq!(second.upstream_id.as_deref(), Some("cold-2"));
        assert_eq!(COUNTER.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn route_tiebreak_by_predicted_read_tokens() {
        let _guard = TEST_LOCK.lock().expect("test lock");
        COUNTER.store(0, Ordering::Relaxed);

        let response = plugin::route_handler(route_request(vec![
            candidate("small-read", Some(512)),
            candidate("large-read", Some(8192)),
        ]))
        .expect("route ok");

        assert_eq!(response.upstream_id.as_deref(), Some("large-read"));
        assert_eq!(COUNTER.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn route_round_robins_exact_cache_ties() {
        let _guard = TEST_LOCK.lock().expect("test lock");
        COUNTER.store(0, Ordering::Relaxed);

        let first = plugin::route_handler(route_request(vec![
            candidate("warm-1", Some(2048)),
            candidate("warm-2", Some(2048)),
        ]))
        .expect("route ok");
        let second = plugin::route_handler(route_request(vec![
            candidate("warm-1", Some(2048)),
            candidate("warm-2", Some(2048)),
        ]))
        .expect("route ok");

        assert_eq!(first.upstream_id.as_deref(), Some("warm-1"));
        assert_eq!(second.upstream_id.as_deref(), Some("warm-2"));
        assert_eq!(COUNTER.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn handshake_declares_wire_version_2() {
        let manifest = PluginManifest {
            name: "cache-aware".to_owned(),
            artifact: "target/wasm32-unknown-unknown/release/cache_aware_router.wasm".to_owned(),
            wire_version: Some(2),
            config: cc_lb_plugin_wire::serde_json::json!({}),
            metadata: BTreeMap::new(),
        };

        assert_eq!(manifest.wire_version, Some(2));
    }

    #[test]
    fn select_candidate_index_empty_returns_none() {
        let counter = AtomicUsize::new(0);

        assert_eq!(select_candidate_index(&counter, &[]), None);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    fn route_request(candidates: Vec<CandidateWire>) -> RouteRequest {
        RouteRequest {
            request_id: "req-route".to_string(),
            headers: Vec::new(),
            method: "POST".to_string(),
            path: "/v1/messages".to_string(),
            query: None,
            body_base64: "e30=".to_string(),
            principal: Principal::dry_run_sample(),
            candidates,
            cache_breakpoints: Vec::new(),
            canonical_model_id: "claude-sonnet-4-5-20250929".to_string(),
        }
    }

    fn candidate(upstream_id: &str, predicted_cache_read_tokens: Option<u32>) -> CandidateWire {
        CandidateWire {
            upstream_id: upstream_id.to_string(),
            name: upstream_id.to_string(),
            kind: "anthropic_api_key".to_string(),
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
}
