use extism_pdk::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn select_candidate_index(counter: &AtomicUsize, candidate_count: usize) -> Option<usize> {
    if candidate_count == 0 {
        return None;
    }
    let current = counter.fetch_add(1, Ordering::Relaxed);
    Some(current % candidate_count)
}

fn route_decision_no_choice() -> Value {
    json!({
        "_version": 1,
        "upstream_id": Value::Null,
        "dialect": {"kind": "self"},
        "upstream": {"kind": "anthropic_direct"}
    })
}

fn route_decision(input: &Value) -> Value {
    let candidates = match input.get("candidates").and_then(|c| c.as_array()) {
        Some(c) => c,
        None => return route_decision_no_choice(),
    };
    let Some(idx) = select_candidate_index(&COUNTER, candidates.len()) else {
        return route_decision_no_choice();
    };
    let selected_upstream_id = candidates
        .get(idx)
        .and_then(|candidate| candidate.get("upstream_id"))
        .cloned()
        .unwrap_or(Value::Null);
    json!({
        "_version": 1,
        "upstream_id": selected_upstream_id,
        "dialect": {"kind": "self"},
        "upstream": {"kind": "anthropic_direct"}
    })
}

#[plugin_fn]
pub fn route(Json(input): Json<Value>) -> FnResult<Json<Value>> {
    Ok(Json(route_decision(&input)))
}

#[derive(Clone, Debug, Deserialize)]
#[allow(dead_code)]
struct ShapeInput {
    #[serde(default)]
    _version: u32,
    request: RequestWire,
    upstream: UpstreamWire,
    #[serde(default)]
    principal: Option<PrincipalWire>,
}

#[derive(Clone, Debug, Deserialize)]
#[allow(dead_code)]
struct RequestWire {
    #[serde(default)]
    request_id: String,
    #[serde(default)]
    headers: Vec<HeaderWire>,
    method: String,
    path: String,
    query: Option<String>,
    body_base64: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HeaderWire {
    name: String,
    value_base64: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind")]
#[serde(rename_all = "snake_case")]
enum UpstreamWire {
    AnthropicDirect,
    CustomAnthropicSpec { base_url: String },
}

#[derive(Clone, Debug, Deserialize)]
#[allow(dead_code)]
struct PrincipalWire {
    #[serde(default)]
    id: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    claims: serde_json::Map<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ShapeResponse {
    #[serde(default)]
    _version: u32,
    url: String,
    method: String,
    #[serde(default)]
    headers: Vec<HeaderWire>,
    body_base64: String,
}

fn shape_response(input: &str) -> Result<ShapeResponse, serde_json::Error> {
    let shape_input: ShapeInput = serde_json::from_str(input)?;
    let base_url = match &shape_input.upstream {
        UpstreamWire::AnthropicDirect => "https://api.anthropic.com".to_string(),
        UpstreamWire::CustomAnthropicSpec { base_url } => base_url.clone(),
    };
    let query_part = shape_input
        .request
        .query
        .as_ref()
        .map(|q| format!("?{}", q))
        .unwrap_or_default();
    let url = format!("{}{}{}", base_url, shape_input.request.path, query_part);
    Ok(ShapeResponse {
        _version: 1,
        url,
        method: shape_input.request.method,
        headers: shape_input.request.headers,
        body_base64: shape_input.request.body_base64,
    })
}

#[plugin_fn]
pub fn shape(input: String) -> FnResult<String> {
    let response = shape_response(&input)?;
    Ok(serde_json::to_string(&response)?)
}

fn normalize_error_response() -> Value {
    json!({
        "_version": 1,
        "body_base64": Value::Null,
    })
}

#[plugin_fn]
pub fn normalize_error(_input: String) -> FnResult<String> {
    Ok(normalize_error_response().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
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
    fn shape_url_for_anthropic_direct_uses_canonical_base() {
        let input = json!({
            "_version": 1,
            "request": {
                "request_id": "req-123",
                "headers": [],
                "method": "POST",
                "path": "/v1/messages",
                "query": "stream=true",
                "body_base64": "e30="
            },
            "upstream": {"kind": "anthropic_direct"},
            "principal": {"id": "user-1", "kind": "api_key", "claims": {}}
        });
        let response = shape_response(&input.to_string()).expect("shape ok");
        assert_eq!(
            response.url,
            "https://api.anthropic.com/v1/messages?stream=true"
        );
        assert_eq!(response.method, "POST");
    }

    #[test]
    fn shape_url_for_custom_uses_base_url() {
        let input = json!({
            "_version": 1,
            "request": {
                "request_id": "req-456",
                "headers": [{"name": "content-type", "value_base64": "YXBwbGljYXRpb24vanNvbg=="}],
                "method": "POST",
                "path": "/v1/messages",
                "query": null,
                "body_base64": "eyJtb2RlbCI6ImNsYXVkZS0zIn0="
            },
            "upstream": {"kind": "custom_anthropic_spec", "base_url": "https://gateway.example.com"},
            "principal": {"id": "user-2", "kind": "oauth_subject", "claims": {}}
        });
        let response = shape_response(&input.to_string()).expect("shape ok");
        assert_eq!(response.url, "https://gateway.example.com/v1/messages");
        assert_eq!(response.method, "POST");
        assert_eq!(response.headers.len(), 1);
        assert_eq!(response.headers[0].name, "content-type");
    }

    #[test]
    fn normalize_error_returns_versioned_null_body() {
        let value = normalize_error_response();
        assert_eq!(value["_version"], json!(1));
        assert_eq!(value["body_base64"], Value::Null);
    }
}
