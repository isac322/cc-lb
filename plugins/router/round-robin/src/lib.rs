use extism_pdk::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

#[plugin_fn]
pub fn route(Json(input): Json<Value>) -> FnResult<Json<Value>> {
    let candidates = match input.get("candidates").and_then(|c| c.as_array()) {
        Some(c) => c,
        None => {
            return Ok(Json(json!({
                "_version": 1,
                "upstream_id": Value::Null,
                "dialect": {"kind": "self"},
                "upstream": {"kind": "anthropic_direct"}
            })));
        }
    };

    if candidates.is_empty() {
        return Ok(Json(json!({
            "_version": 1,
            "upstream_id": Value::Null,
            "dialect": {"kind": "self"},
            "upstream": {"kind": "anthropic_direct"}
        })));
    }

    let idx = {
        let current = COUNTER.fetch_add(1, Ordering::Relaxed);
        current % candidates.len()
    };

    let selected_upstream_id = candidates
        .get(idx)
        .and_then(|candidate| candidate.get("upstream_id"))
        .cloned()
        .unwrap_or(Value::Null);

    Ok(Json(json!({
        "_version": 1,
        "upstream_id": selected_upstream_id,
        "dialect": {"kind": "self"},
        "upstream": {"kind": "anthropic_direct"}
    })))
}

#[derive(Clone, Debug, Deserialize)]
struct ShapeInput {
    #[serde(default)]
    _version: u32,
    request: RequestWire,
    upstream: UpstreamWire,
    principal: PrincipalWire,
}

#[derive(Clone, Debug, Deserialize)]
struct RequestWire {
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
struct PrincipalWire {
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

#[plugin_fn]
pub fn shape(input: String) -> FnResult<String> {
    let shape_input: ShapeInput = serde_json::from_str(&input)?;

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

    let url = format!(
        "{}{}{}",
        base_url, shape_input.request.path, query_part
    );

    let response = ShapeResponse {
        _version: 1,
        url,
        method: shape_input.request.method,
        headers: shape_input.request.headers,
        body_base64: shape_input.request.body_base64,
    };

    Ok(serde_json::to_string(&response)?)
}

#[plugin_fn]
pub fn normalize_error(_input: String) -> FnResult<String> {
    Ok(String::new())
}

#[cfg(test)]
mod tests {
    use super::*;

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
            "upstream": {
                "kind": "anthropic_direct"
            },
            "principal": {
                "id": "user-1",
                "kind": "api_key",
                "claims": {}
            }
        });

        let result = serde_json::to_string(&input).unwrap();
        assert!(result.contains("anthropic_direct"));
        
        let parsed: ShapeInput = serde_json::from_str(&result).unwrap();
        let base_url = match &parsed.upstream {
            UpstreamWire::AnthropicDirect => "https://api.anthropic.com",
            UpstreamWire::CustomAnthropicSpec { .. } => "",
        };
        
        let query_part = parsed.request.query.as_ref()
            .map(|q| format!("?{}", q))
            .unwrap_or_default();
        
        let url = format!("{}{}{}", base_url, parsed.request.path, query_part);
        assert_eq!(url, "https://api.anthropic.com/v1/messages?stream=true");
        assert_eq!(parsed.request.method, "POST");
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
            "upstream": {
                "kind": "custom_anthropic_spec",
                "base_url": "https://gateway.example.com"
            },
            "principal": {
                "id": "user-2",
                "kind": "oauth_subject",
                "claims": {}
            }
        });

        let result = serde_json::to_string(&input).unwrap();
        let parsed: ShapeInput = serde_json::from_str(&result).unwrap();
        
        let base_url = match &parsed.upstream {
            UpstreamWire::AnthropicDirect => "https://api.anthropic.com",
            UpstreamWire::CustomAnthropicSpec { base_url } => base_url,
        };
        
        let query_part = parsed.request.query.as_ref()
            .map(|q| format!("?{}", q))
            .unwrap_or_default();
        
        let url = format!("{}{}{}", base_url, parsed.request.path, query_part);
        assert_eq!(url, "https://gateway.example.com/v1/messages");
        assert_eq!(parsed.request.method, "POST");
        assert_eq!(parsed.request.headers.len(), 1);
        assert_eq!(parsed.request.headers[0].name, "content-type");
    }
}
