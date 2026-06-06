use cc_lb_plugin_wire::v1::{
    common::{DialectBinding, UpstreamWire},
    route::RouteResponse,
};
use std::sync::atomic::{AtomicUsize, Ordering};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn select_candidate_index(counter: &AtomicUsize, candidate_count: usize) -> Option<usize> {
    if candidate_count == 0 {
        return None;
    }
    let current = counter.fetch_add(1, Ordering::Relaxed);
    Some(current % candidate_count)
}

fn route_response(upstream_id: Option<String>) -> RouteResponse {
    RouteResponse {
        upstream_id,
        dialect: DialectBinding::SelfReferenced,
        upstream: UpstreamWire::AnthropicDirect,
    }
}

#[cc_lb_pdk::plugin(name = "round-robin", version = "0.1.0")]
mod plugin {
    use super::{COUNTER, route_response, select_candidate_index};
    use cc_lb_plugin_wire::v1::{
        common::UpstreamWire,
        normalize_error::{NormalizeErrorRequest, NormalizeErrorResponse},
        route::{RouteRequest, RouteResponse},
        shape::{ShapeRequest, ShapeResponse},
    };
    use std::convert::Infallible;

    #[cc_lb_pdk::handler(name = "route", versions = [1])]
    pub(super) fn route_handler(request: RouteRequest) -> Result<RouteResponse, Infallible> {
        let upstream_id = select_candidate_index(&COUNTER, request.candidates.len())
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
    use cc_lb_plugin_wire::v1::{
        common::{CandidateWire, HeaderWire, Principal, RequestWire, UpstreamWire},
        normalize_error::NormalizeErrorRequest,
        route::RouteRequest,
        shape::ShapeRequest,
    };
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::thread;

    #[test]
    fn select_candidate_index_empty_returns_none() {
        let counter = AtomicUsize::new(0);
        assert_eq!(select_candidate_index(&counter, 0), None);
    }

    #[test]
    fn select_candidate_index_uniform_30_calls_3_candidates() {
        let counter = AtomicUsize::new(0);
        let mut buckets = [0_usize; 3];
        for _ in 0..30 {
            let idx = select_candidate_index(&counter, 3).expect("non-zero count");
            buckets[idx] += 1;
        }
        assert_eq!(buckets, [10, 10, 10], "uniform distribution expected");
    }

    #[test]
    fn select_candidate_index_atomic_thread_safe() {
        let counter = Arc::new(AtomicUsize::new(0));
        let threads = 100;
        let per_thread = 100;
        let mut handles = Vec::with_capacity(threads);
        for _ in 0..threads {
            let counter = Arc::clone(&counter);
            handles.push(thread::spawn(move || {
                let mut local = Vec::with_capacity(per_thread);
                for _ in 0..per_thread {
                    local.push(select_candidate_index(&counter, 7).expect("non-zero count"));
                }
                local
            }));
        }
        let mut total = 0;
        for handle in handles {
            let local = handle.join().expect("thread join");
            total += local.len();
        }
        assert_eq!(total, threads * per_thread);
        assert_eq!(counter.load(Ordering::Relaxed), threads * per_thread);
    }

    #[test]
    fn route_handler_cycles_candidate_upstream_ids() {
        COUNTER.store(0, Ordering::Relaxed);

        let first =
            plugin::route_handler(route_request(vec![candidate("up-1"), candidate("up-2")]))
                .expect("route ok");
        let second =
            plugin::route_handler(route_request(vec![candidate("up-1"), candidate("up-2")]))
                .expect("route ok");

        assert_eq!(first.upstream_id.as_deref(), Some("up-1"));
        assert_eq!(second.upstream_id.as_deref(), Some("up-2"));
        assert_eq!(first.dialect, DialectBinding::SelfReferenced);
        assert_eq!(first.upstream, UpstreamWire::AnthropicDirect);
    }

    #[test]
    fn route_handler_no_candidates_returns_no_choice() {
        COUNTER.store(0, Ordering::Relaxed);

        let response = plugin::route_handler(route_request(Vec::new())).expect("route ok");

        assert_eq!(response.upstream_id, None);
        assert_eq!(response.dialect, DialectBinding::SelfReferenced);
        assert_eq!(response.upstream, UpstreamWire::AnthropicDirect);
        assert_eq!(COUNTER.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn shape_url_for_anthropic_direct_uses_canonical_base() {
        let response = plugin::shape_handler(ShapeRequest {
            request: RequestWire {
                request_id: "req-123".to_string(),
                headers: Vec::new(),
                method: "POST".to_string(),
                path: "/v1/messages".to_string(),
                query: Some("stream=true".to_string()),
                body_base64: "e30=".to_string(),
            },
            upstream: UpstreamWire::AnthropicDirect,
            principal: Principal {
                id: "user-1".to_string(),
                kind: "api_key".to_string(),
                claims: Default::default(),
            },
        })
        .expect("shape ok");

        assert_eq!(
            response.url,
            "https://api.anthropic.com/v1/messages?stream=true"
        );
        assert_eq!(response.method, "POST");
    }

    #[test]
    fn normalize_error_returns_null_body() {
        let response = plugin::normalize_error_handler(NormalizeErrorRequest {
            status: 500,
            body_base64: "ignored".to_string(),
        })
        .expect("normalize ok");

        assert_eq!(response.body_base64, None);
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
        }
    }

    fn candidate(upstream_id: &str) -> CandidateWire {
        CandidateWire {
            upstream_id: upstream_id.to_string(),
            name: upstream_id.to_string(),
            kind: "anthropic_api_key".to_string(),
            observed_rate_limits: Vec::new(),
            subscription_quotas: Vec::new(),
            observed_at_unix_secs: 0,
        }
    }
}
