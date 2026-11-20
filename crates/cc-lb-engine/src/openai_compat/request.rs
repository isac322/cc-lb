use cc_lb_domain::{
    CoerceOrReject, DropOrReject, EmulateDropOrReject, HostedToolHandling, MetadataHandling,
};
use serde_json::{Map, Value, json};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    Http,
    WebSocket,
}

pub struct NormalizeInput<'a> {
    pub body: &'a Value,
    pub policy: &'a cc_lb_domain::OpenaiOauthCompatPolicy,
    pub transport: Transport,
}

pub struct Normalized {
    pub body: Option<Value>,
    pub aggregate_to_json: bool,
    pub echo_metadata: Option<Value>,
    pub truncation_emulate: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiKeyOnlyFeature {
    FileSearchTool,
    CodeInterpreterTool,
    PromptTemplate,
    PreviousResponseId,
    Conversation,
    Background,
}

fn reject(p: &str) -> super::CompatReject {
    super::CompatReject::unsupported_parameter(p)
}
fn drop_or_reject(
    v: &mut Map<String, Value>,
    key: &str,
    p: DropOrReject,
) -> Result<bool, super::CompatReject> {
    if v.remove(key).is_some() {
        if p == DropOrReject::Reject {
            return Err(reject(key));
        }
        return Ok(true);
    }
    Ok(false)
}

pub fn normalize_responses_request(
    input: NormalizeInput<'_>,
) -> Result<Normalized, super::CompatReject> {
    let mut b = input.body.clone();
    let mut changed = false;
    let mut aggregate = false;
    let mut echo = None;
    let mut trunc_emulate = false;
    let obj = match b.as_object_mut() {
        Some(x) => x,
        None => {
            return Ok(Normalized {
                body: None,
                aggregate_to_json: false,
                echo_metadata: None,
                truncation_emulate: false,
            });
        }
    };
    if let Some(s) = obj.get("input").and_then(Value::as_str).map(str::to_owned) {
        obj.insert(
            "input".into(),
            json!([{"type":"message","role":"user","content":[{"type":"input_text","text":s}]}]),
        );
        changed = true;
    }
    if input.transport == Transport::Http {
        if obj.get("stream").and_then(Value::as_bool) != Some(true) {
            obj.insert("stream".into(), Value::Bool(true));
            changed = true;
            aggregate = true;
        }
        if !obj.contains_key("store") {
            obj.insert("store".into(), Value::Bool(false));
            changed = true;
        }
    }
    if obj.get("store").and_then(Value::as_bool) == Some(true) {
        match input.policy.store_true {
            CoerceOrReject::Coerce => {
                obj.insert("store".into(), Value::Bool(false));
                changed = true;
            }
            CoerceOrReject::Reject => return Err(reject("store")),
        }
    }
    for key in ["temperature", "top_p"] {
        if drop_or_reject(obj, key, input.policy.sampling)? {
            changed = true;
        }
    }
    if obj.contains_key("top_logprobs") {
        if input.policy.logprobs == DropOrReject::Reject {
            return Err(reject("top_logprobs"));
        }
        obj.remove("top_logprobs");
        changed = true;
    }
    if let Some(a) = obj.get_mut("include").and_then(Value::as_array_mut) {
        if let Some(i) = a
            .iter()
            .position(|x| x.as_str() == Some("message.output_text.logprobs"))
        {
            if input.policy.logprobs == DropOrReject::Reject {
                return Err(reject(&format!("include[{i}]")));
            }
            a.remove(i);
            changed = true;
        }
    }
    if let Some(t) = obj
        .get("truncation")
        .and_then(Value::as_str)
        .map(str::to_owned)
    {
        match t.as_str() {
            "disabled" => {
                obj.remove("truncation");
                changed = true;
            }
            "auto" => match input.policy.truncation_auto {
                EmulateDropOrReject::Emulate => {
                    obj.remove("truncation");
                    changed = true;
                    trunc_emulate = true;
                }
                EmulateDropOrReject::Drop => {
                    obj.remove("truncation");
                    changed = true;
                }
                EmulateDropOrReject::Reject => return Err(reject("truncation")),
            },
            _ => {}
        }
    }
    if obj.get("prompt_cache_retention").and_then(Value::as_str) == Some("24h") {
        obj.remove("prompt_cache_retention");
        changed = true;
    } else if obj.get("prompt_cache_retention").and_then(Value::as_str) == Some("in_memory") {
        if drop_or_reject(
            obj,
            "prompt_cache_retention",
            input.policy.prompt_cache_in_memory,
        )? {
            changed = true;
        }
    }
    if let Some(t) = obj
        .get("service_tier")
        .and_then(Value::as_str)
        .map(str::to_owned)
    {
        match t.as_str() {
            "auto" | "flex" => match input.policy.service_tier {
                CoerceOrReject::Coerce => {
                    obj.insert("service_tier".into(), json!("default"));
                    changed = true
                }
                CoerceOrReject::Reject => {
                    return Err(super::CompatReject::unsupported_value("service_tier", &t));
                }
            },
            "scale" => return Err(super::CompatReject::unsupported_value("service_tier", &t)),
            _ => {}
        }
    }
    if let Some(tools) = obj.get_mut("tools").and_then(Value::as_array_mut) {
        for (i, t) in tools.iter_mut().enumerate() {
            let Some(o) = t.as_object_mut() else { continue };
            let typ = o.get("type").and_then(Value::as_str).unwrap_or("");
            if [
                "computer",
                "computer_use_preview",
                "mcp",
                "shell",
                "apply_patch",
                "local_shell",
            ]
            .contains(&typ)
            {
                return Err(super::CompatReject::unsupported_value(
                    format!("tools[{i}].type"),
                    typ,
                ));
            }
            if [
                "web_search_preview",
                "web_search_preview_2025_03_11",
                "web_search_2025_08_26",
            ]
            .contains(&typ)
            {
                match input.policy.web_search_preview {
                    CoerceOrReject::Coerce => {
                        o.insert("type".into(), json!("web_search"));
                        changed = true
                    }
                    CoerceOrReject::Reject => {
                        return Err(super::CompatReject::unsupported_value(
                            format!("tools[{i}].type"),
                            typ,
                        ));
                    }
                }
            }
        }
    }
    if obj.contains_key("metadata")
        || obj.contains_key("user")
        || obj.contains_key("safety_identifier")
    {
        match input.policy.metadata {
            MetadataHandling::Reject => {
                return Err(reject(if obj.contains_key("metadata") {
                    "metadata"
                } else if obj.contains_key("user") {
                    "user"
                } else {
                    "safety_identifier"
                }));
            }
            MetadataHandling::Drop => {
                for k in ["metadata", "user", "safety_identifier"] {
                    if obj.remove(k).is_some() {
                        changed = true
                    }
                }
            }
            MetadataHandling::DropEcho => {
                echo = obj.remove("metadata");
                for k in ["user", "safety_identifier"] {
                    if obj.remove(k).is_some() {
                        changed = true
                    }
                }
                if echo.is_some() {
                    changed = true
                }
            }
        }
    }
    if drop_or_reject(obj, "max_tool_calls", input.policy.max_tool_calls)? {
        changed = true;
    }
    Ok(Normalized {
        body: if changed { Some(b) } else { None },
        aggregate_to_json: aggregate,
        echo_metadata: echo,
        truncation_emulate: trunc_emulate,
    })
}
pub fn degrade_without_api_key(
    body: &Value,
    features: &[ApiKeyOnlyFeature],
    policy: &cc_lb_domain::OpenaiOauthCompatPolicy,
) -> Result<Value, super::CompatReject> {
    let mut b = body.clone();
    let hosted = features.iter().any(|f| {
        matches!(
            f,
            ApiKeyOnlyFeature::FileSearchTool | ApiKeyOnlyFeature::CodeInterpreterTool
        )
    });
    if features
        .iter()
        .any(|f| matches!(f, ApiKeyOnlyFeature::PromptTemplate))
    {
        return Err(reject("prompt"));
    }
    if features
        .iter()
        .any(|f| matches!(f, ApiKeyOnlyFeature::PreviousResponseId))
    {
        return Err(reject("previous_response_id"));
    }
    if features
        .iter()
        .any(|f| matches!(f, ApiKeyOnlyFeature::Conversation))
    {
        return Err(reject("conversation"));
    }
    if features
        .iter()
        .any(|f| matches!(f, ApiKeyOnlyFeature::Background))
        && policy.background == DropOrReject::Reject
    {
        return Err(reject("background"));
    }
    if hosted && policy.hosted_tools == HostedToolHandling::Reject {
        return Err(reject("tools"));
    }
    if hosted {
        if let Some(a) = b.get_mut("tools").and_then(Value::as_array_mut) {
            a.retain(|t| {
                !matches!(
                    t.get("type").and_then(Value::as_str),
                    Some("file_search") | Some("code_interpreter")
                )
            });
        }
        let forced = match b.get("tool_choice") {
            Some(Value::String(s)) if s == "required" => true,
            Some(v) => matches!(
                v.get("type").and_then(Value::as_str),
                Some("file_search") | Some("code_interpreter")
            ),
            None => false,
        };
        if forced {
            b.as_object_mut()
                .unwrap()
                .insert("tool_choice".into(), json!("auto"));
        }
    }
    if features
        .iter()
        .any(|f| matches!(f, ApiKeyOnlyFeature::Background))
    {
        b.as_object_mut().unwrap().remove("background");
    }
    Ok(b)
}

/// Drops the oldest half of droppable input turns while preserving tool-call pairs.
pub fn trim_oldest_input(body: &Value) -> Option<Value> {
    let mut b = body.clone();
    let a = b.get_mut("input")?.as_array_mut()?;
    let mut groups: Vec<Vec<usize>> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for i in 0..a.len() {
        if seen.contains(&i)
            || matches!(
                a[i].get("role").and_then(Value::as_str),
                Some("system") | Some("developer")
            )
        {
            continue;
        }
        let typ = a[i].get("type").and_then(Value::as_str).unwrap_or("");
        if matches!(typ, "function_call" | "custom_tool_call") {
            let id = a[i].get("call_id").and_then(Value::as_str);
            let mut g = vec![i];
            if let Some(id) = id {
                for j in i + 1..a.len() {
                    if a[j].get("call_id").and_then(Value::as_str) == Some(id)
                        && matches!(
                            a[j].get("type").and_then(Value::as_str),
                            Some("function_call_output") | Some("custom_tool_call_output")
                        )
                    {
                        g.push(j);
                        break;
                    }
                }
            }
            for j in &g {
                seen.insert(*j);
            }
            groups.push(g);
        } else if !matches!(typ, "function_call_output" | "custom_tool_call_output") {
            groups.push(vec![i]);
        }
    }
    if groups.is_empty() {
        return None;
    }
    let n = (groups.len() + 1) / 2;
    let mut remove: Vec<usize> = groups.into_iter().take(n).flatten().collect();
    remove.sort_unstable_by(|x, y| y.cmp(x));
    for i in remove {
        a.remove(i);
    }
    Some(b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_domain::OpenaiOauthCompatPolicy;

    #[test]
    fn codex_body_is_passthrough() {
        let body = json!({"input":[{"type":"message","role":"user"}],"stream":true,"store":false,"include":["reasoning.encrypted_content"],"tools":[{"type":"function"}],"service_tier":"priority"});
        let n = normalize_responses_request(NormalizeInput {
            body: &body,
            policy: &OpenaiOauthCompatPolicy::default(),
            transport: Transport::Http,
        })
        .unwrap();
        assert!(n.body.is_none());
    }

    #[test]
    fn http_and_websocket_stream_store_differ() {
        let body = json!({"input":"hello"});
        let p = OpenaiOauthCompatPolicy::default();
        let h = normalize_responses_request(NormalizeInput {
            body: &body,
            policy: &p,
            transport: Transport::Http,
        })
        .unwrap();
        assert_eq!(h.body.as_ref().unwrap()["stream"], true);
        assert_eq!(h.body.as_ref().unwrap()["store"], false);
        let w = normalize_responses_request(NormalizeInput {
            body: &body,
            policy: &p,
            transport: Transport::WebSocket,
        })
        .unwrap();
        assert!(w.body.as_ref().unwrap().get("stream").is_none());
        assert!(w.body.as_ref().unwrap().get("store").is_none());
    }

    #[test]
    fn hosted_degradation_releases_forced_choice() {
        let body = json!({"tools":[{"type":"file_search"}],"tool_choice":{"type":"file_search"}});
        let out = degrade_without_api_key(
            &body,
            &[ApiKeyOnlyFeature::FileSearchTool],
            &OpenaiOauthCompatPolicy::default(),
        )
        .unwrap();
        assert_eq!(out["tool_choice"], "auto");
        assert!(out["tools"].as_array().unwrap().is_empty());
    }

    #[test]
    fn trim_preserves_call_output_pair() {
        let body = json!({"input":[{"type":"function_call","call_id":"x"},{"type":"function_call_output","call_id":"x"},{"type":"message","role":"user"}]});
        let out = trim_oldest_input(&body).unwrap();
        let a = out["input"].as_array().unwrap();
        assert!(
            !(a.iter().any(|x| x["type"] == "function_call")
                ^ a.iter().any(|x| x["type"] == "function_call_output"))
        );
    }
}
pub fn api_key_only_features(body: &Value, _transport: Transport) -> Vec<ApiKeyOnlyFeature> {
    let mut r = Vec::new();
    if let Some(a) = body.get("tools").and_then(Value::as_array) {
        for t in a {
            match t.get("type").and_then(Value::as_str) {
                Some("file_search") => r.push(ApiKeyOnlyFeature::FileSearchTool),
                Some("code_interpreter") => r.push(ApiKeyOnlyFeature::CodeInterpreterTool),
                _ => {}
            }
        }
    }
    for (k, f) in [
        ("prompt", ApiKeyOnlyFeature::PromptTemplate),
        (
            "previous_response_id",
            ApiKeyOnlyFeature::PreviousResponseId,
        ),
        ("conversation", ApiKeyOnlyFeature::Conversation),
    ] {
        if body.get(k).is_some() {
            r.push(f)
        }
    }
    if body.get("background").and_then(Value::as_bool) == Some(true) {
        r.push(ApiKeyOnlyFeature::Background)
    }
    r
}
