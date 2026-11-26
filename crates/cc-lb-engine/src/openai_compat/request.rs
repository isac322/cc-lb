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
    if v.get(key).is_some_and(|value| !value.is_null()) {
        v.remove(key);
        if p == DropOrReject::Reject {
            return Err(reject(key));
        }
        return Ok(true);
    }
    Ok(false)
}

fn has_non_null(v: &Map<String, Value>, key: &str) -> bool {
    v.get(key).is_some_and(|value| !value.is_null())
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
        if !has_non_null(obj, "store") {
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
    if has_non_null(obj, "top_logprobs") {
        if input.policy.logprobs == DropOrReject::Reject {
            return Err(reject("top_logprobs"));
        }
        obj.remove("top_logprobs");
        changed = true;
    }
    if let Some(a) = obj.get_mut("include").and_then(Value::as_array_mut)
        && let Some(i) = a
            .iter()
            .position(|x| x.as_str() == Some("message.output_text.logprobs"))
    {
        if input.policy.logprobs == DropOrReject::Reject {
            return Err(reject(&format!("include[{i}]")));
        }
        a.remove(i);
        changed = true;
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
    } else if obj.get("prompt_cache_retention").and_then(Value::as_str) == Some("in_memory")
        && drop_or_reject(
            obj,
            "prompt_cache_retention",
            input.policy.prompt_cache_in_memory,
        )?
    {
        changed = true;
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
    if has_non_null(obj, "metadata")
        || has_non_null(obj, "user")
        || has_non_null(obj, "safety_identifier")
    {
        match input.policy.metadata {
            MetadataHandling::Reject => {
                return Err(reject(if has_non_null(obj, "metadata") {
                    "metadata"
                } else if has_non_null(obj, "user") {
                    "user"
                } else {
                    "safety_identifier"
                }));
            }
            MetadataHandling::Drop => {
                for k in ["metadata", "user", "safety_identifier"] {
                    if has_non_null(obj, k) {
                        obj.remove(k);
                        changed = true
                    }
                }
            }
            MetadataHandling::DropEcho => {
                echo = obj
                    .get("metadata")
                    .filter(|value| !value.is_null())
                    .cloned();
                for k in ["metadata", "user", "safety_identifier"] {
                    if has_non_null(obj, k) {
                        obj.remove(k);
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
    if changed {
        // Public-shaped frames with unsupported hosted tool types are rejected
        // only when another normalization applies.
        if let Some((i, typ)) = obj
            .get("tools")
            .and_then(Value::as_array)
            .and_then(|tools| {
                tools.iter().enumerate().find_map(|(i, tool)| {
                    let typ = tool.get("type").and_then(Value::as_str)?;
                    [
                        "computer",
                        "computer_use_preview",
                        "mcp",
                        "shell",
                        "apply_patch",
                        "local_shell",
                    ]
                    .contains(&typ)
                    .then_some((i, typ))
                })
            })
        {
            return Err(super::CompatReject::unsupported_value(
                format!("tools[{i}].type"),
                typ,
            ));
        }
    }
    Ok(Normalized {
        body: if changed { Some(b) } else { None },
        aggregate_to_json: aggregate,
        echo_metadata: echo,
        truncation_emulate: trunc_emulate,
    })
}
fn strip_hosted_tools_from_array(tools: &mut Vec<Value>, depth: usize) {
    if depth >= 8 {
        return;
    }
    let mut kept = Vec::with_capacity(tools.len());
    for mut tool in tools.drain(..) {
        match tool.get("type").and_then(Value::as_str) {
            Some("file_search" | "code_interpreter") => continue,
            Some("namespace") => {
                if let Some(obj) = tool.as_object_mut()
                    && let Some(nested) = obj.get_mut("tools").and_then(Value::as_array_mut)
                {
                    strip_hosted_tools_from_array(nested, depth + 1);
                    if nested.is_empty() {
                        continue;
                    }
                }
            }
            _ => {}
        }
        kept.push(tool);
    }
    *tools = kept;
}

fn collect_api_key_only_tools(tools: &[Value], out: &mut Vec<ApiKeyOnlyFeature>, depth: usize) {
    if depth >= 8 {
        return;
    }
    for t in tools {
        match t.get("type").and_then(Value::as_str) {
            Some("file_search") => out.push(ApiKeyOnlyFeature::FileSearchTool),
            Some("code_interpreter") => out.push(ApiKeyOnlyFeature::CodeInterpreterTool),
            Some("namespace") => {
                if let Some(nested) = t.get("tools").and_then(Value::as_array) {
                    collect_api_key_only_tools(nested, out, depth + 1);
                }
            }
            _ => {}
        }
    }
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
        let mut has_remaining_tools = false;
        let top_tools_removed = if let Some(a) = b.get_mut("tools").and_then(Value::as_array_mut) {
            strip_hosted_tools_from_array(a, 0);
            if a.is_empty() {
                true
            } else {
                has_remaining_tools = true;
                false
            }
        } else {
            false
        };
        if let Some(input) = b.get_mut("input").and_then(Value::as_array_mut) {
            for item in input {
                let Some(obj) = item.as_object_mut() else {
                    continue;
                };
                if matches!(
                    obj.get("type").and_then(Value::as_str),
                    Some("additional_tools" | "tool_search_output")
                ) && let Some(tools) = obj.get_mut("tools").and_then(Value::as_array_mut)
                {
                    strip_hosted_tools_from_array(tools, 0);
                    if !tools.is_empty() {
                        has_remaining_tools = true;
                    }
                }
            }
        }
        let object = b.as_object_mut().expect("request body is an object");
        if top_tools_removed {
            object.remove("tools");
        }
        enum ToolChoiceAction {
            Keep,
            Remove,
            Auto,
            None,
        }
        let action = match object.get_mut("tool_choice") {
            Some(Value::String(s)) if s == "required" && !has_remaining_tools => {
                ToolChoiceAction::Remove
            }
            Some(Value::Object(choice)) => match choice.get("type").and_then(Value::as_str) {
                Some("file_search" | "code_interpreter") => {
                    if has_remaining_tools {
                        ToolChoiceAction::Auto
                    } else {
                        ToolChoiceAction::Remove
                    }
                }
                Some("allowed_tools") => {
                    let empty_after_strip = if let Some(allowed) =
                        choice.get_mut("tools").and_then(Value::as_array_mut)
                    {
                        strip_hosted_tools_from_array(allowed, 0);
                        allowed.is_empty()
                    } else {
                        false
                    };
                    let required_mode =
                        choice.get("mode").and_then(Value::as_str) == Some("required");
                    if empty_after_strip {
                        ToolChoiceAction::None
                    } else if !has_remaining_tools && required_mode {
                        ToolChoiceAction::Remove
                    } else {
                        ToolChoiceAction::Keep
                    }
                }
                _ => ToolChoiceAction::Keep,
            },
            _ => ToolChoiceAction::Keep,
        };
        match action {
            ToolChoiceAction::Keep => {}
            ToolChoiceAction::Remove => {
                object.remove("tool_choice");
            }
            ToolChoiceAction::Auto => {
                object.insert("tool_choice".into(), json!("auto"));
            }
            ToolChoiceAction::None => {
                object.insert("tool_choice".into(), json!("none"));
            }
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
            if let Some(id) = id
                && let Some(output) = a.iter().enumerate().skip(i + 1).find_map(|(j, item)| {
                    (item.get("call_id").and_then(Value::as_str) == Some(id)
                        && matches!(
                            item.get("type").and_then(Value::as_str),
                            Some("function_call_output") | Some("custom_tool_call_output")
                        ))
                    .then_some(j)
                })
            {
                g.push(output);
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
    let n = groups.len().div_ceil(2);
    let mut remove: Vec<usize> = groups.into_iter().take(n).flatten().collect();
    remove.sort_unstable_by(|x, y| y.cmp(x));
    for i in remove {
        a.remove(i);
    }
    Some(b)
}
pub fn api_key_only_features(body: &Value) -> Vec<ApiKeyOnlyFeature> {
    let mut r = Vec::new();
    if let Some(a) = body.get("tools").and_then(Value::as_array) {
        collect_api_key_only_tools(a, &mut r, 0);
    }
    if let Some(input) = body.get("input").and_then(Value::as_array) {
        for item in input {
            if matches!(
                item.get("type").and_then(Value::as_str),
                Some("additional_tools" | "tool_search_output")
            ) && let Some(tools) = item.get("tools").and_then(Value::as_array)
            {
                collect_api_key_only_tools(tools, &mut r, 0);
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
        if body.get(k).is_some_and(|value| !value.is_null()) {
            r.push(f)
        }
    }
    if body.get("background").and_then(Value::as_bool) == Some(true) {
        r.push(ApiKeyOnlyFeature::Background)
    }
    r
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
        // A choice that forced a removed hosted tool goes away with the tools.
        assert!(out.get("tool_choice").is_none());
        assert!(out.get("tools").is_none());
    }

    #[test]
    fn null_optional_fields_are_absent() {
        let body = json!({
            "input": [{"type":"message","role":"user"}],
            "stream": true,
            "store": null,
            "temperature": null,
            "top_p": null,
            "top_logprobs": null,
            "metadata": null,
            "prompt": null,
            "previous_response_id": null,
            "conversation": null
        });
        let normalized = normalize_responses_request(NormalizeInput {
            body: &body,
            policy: &OpenaiOauthCompatPolicy::default(),
            transport: Transport::WebSocket,
        })
        .unwrap();
        assert!(normalized.body.is_none());
        let http = normalize_responses_request(NormalizeInput {
            body: &body,
            policy: &OpenaiOauthCompatPolicy::default(),
            transport: Transport::Http,
        })
        .unwrap();
        assert_eq!(
            http.body.as_ref().expect("HTTP normalizes null store")["store"],
            false
        );
        assert!(api_key_only_features(&body).is_empty());
    }

    #[test]
    fn unsupported_tools_pass_through_codex_shape_but_reject_public_shape() {
        let codex = json!({
            "input": [{"type":"message","role":"user"}],
            "stream": true,
            "store": false,
            "tools": [{"type":"local_shell"}]
        });
        let policy = OpenaiOauthCompatPolicy::default();
        assert!(
            normalize_responses_request(NormalizeInput {
                body: &codex,
                policy: &policy,
                transport: Transport::WebSocket,
            })
            .unwrap()
            .body
            .is_none()
        );

        let public = json!({
            "input": [{"type":"message","role":"user"}],
            "stream": true,
            "store": false,
            "metadata": {"request":"public"},
            "tools": [{"type":"local_shell"}]
        });
        assert!(
            normalize_responses_request(NormalizeInput {
                body: &public,
                policy: &policy,
                transport: Transport::WebSocket,
            })
            .is_err()
        );
    }

    #[test]
    fn hosted_degradation_preserves_required_for_remaining_function_tool() {
        let body = json!({
            "tools": [{"type":"file_search"}, {"type":"function","name":"lookup"}],
            "tool_choice": "required"
        });
        let out = degrade_without_api_key(
            &body,
            &[ApiKeyOnlyFeature::FileSearchTool],
            &OpenaiOauthCompatPolicy::default(),
        )
        .unwrap();
        assert_eq!(out["tool_choice"], "required");
        assert_eq!(out["tools"].as_array().map(|tools| tools.len()), Some(1));
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

    #[test]
    fn api_key_only_features_detects_nested_and_input_tool_lists() {
        let additional = json!({
            "input": [{
                "type": "additional_tools",
                "tools": [{"type": "code_interpreter", "container": {"type": "auto"}}]
            }]
        });
        assert_eq!(
            api_key_only_features(&additional),
            vec![ApiKeyOnlyFeature::CodeInterpreterTool]
        );

        let tool_search = json!({
            "input": [{
                "type": "tool_search_output",
                "tools": [{"type": "namespace", "name": "files", "tools": [{"type": "file_search", "vector_store_ids": ["vs_1"]}]}]
            }]
        });
        assert_eq!(
            api_key_only_features(&tool_search),
            vec![ApiKeyOnlyFeature::FileSearchTool]
        );

        let function_only = json!({
            "input": [{
                "type": "additional_tools",
                "tools": [{"type": "function", "name": "lookup"}]
            }, {
                "type": "mcp_list_tools",
                "tools": [{"type": "code_interpreter"}]
            }]
        });
        assert!(api_key_only_features(&function_only).is_empty());
    }

    #[test]
    fn hosted_degradation_releases_required_across_input_tool_lists() {
        let policy = OpenaiOauthCompatPolicy::default();

        // 1. additional_tools with only code_interpreter (no top-level tools) + tool_choice: "required" -> clears tool_choice
        let only_additional = json!({
            "input": [{
                "type": "additional_tools",
                "tools": [{"type": "code_interpreter", "container": {"type": "auto"}}]
            }],
            "tool_choice": "required"
        });
        let out_only = degrade_without_api_key(
            &only_additional,
            &[ApiKeyOnlyFeature::CodeInterpreterTool],
            &policy,
        )
        .unwrap();
        assert!(out_only.get("tool_choice").is_none());

        // 2. additional_tools with code_interpreter + surviving function tool -> preserves tool_choice: "required"
        let mixed_additional = json!({
            "input": [{
                "type": "additional_tools",
                "tools": [
                    {"type": "code_interpreter", "container": {"type": "auto"}},
                    {"type": "function", "name": "lookup"}
                ]
            }],
            "tool_choice": "required"
        });
        let out_mixed = degrade_without_api_key(
            &mixed_additional,
            &[ApiKeyOnlyFeature::CodeInterpreterTool],
            &policy,
        )
        .unwrap();
        assert_eq!(out_mixed["tool_choice"], "required");
        assert_eq!(
            out_mixed["input"][0]["tools"]
                .as_array()
                .map(|tools| tools.len()),
            Some(1)
        );

        // 3. Top-level tools with only code_interpreter + additional_tools with surviving function -> removes top-level tools, preserves "required"
        let top_removed_input_survives = json!({
            "tools": [{"type": "code_interpreter", "container": {"type": "auto"}}],
            "input": [{
                "type": "additional_tools",
                "tools": [{"type": "function", "name": "lookup"}]
            }],
            "tool_choice": "required"
        });
        let out_top_removed = degrade_without_api_key(
            &top_removed_input_survives,
            &[ApiKeyOnlyFeature::CodeInterpreterTool],
            &policy,
        )
        .unwrap();
        assert!(out_top_removed.get("tools").is_none());
        assert_eq!(out_top_removed["tool_choice"], "required");

        // 4. Forced hosted tool_choice with additional_tools: clears when no tools remain, downgrades to "auto" when function survives
        let forced_only_additional = json!({
            "input": [{
                "type": "additional_tools",
                "tools": [{"type": "code_interpreter", "container": {"type": "auto"}}]
            }],
            "tool_choice": {"type": "code_interpreter"}
        });
        let out_forced_cleared = degrade_without_api_key(
            &forced_only_additional,
            &[ApiKeyOnlyFeature::CodeInterpreterTool],
            &policy,
        )
        .unwrap();
        assert!(out_forced_cleared.get("tool_choice").is_none());

        let forced_mixed_additional = json!({
            "input": [{
                "type": "additional_tools",
                "tools": [
                    {"type": "code_interpreter", "container": {"type": "auto"}},
                    {"type": "function", "name": "lookup"}
                ]
            }],
            "tool_choice": {"type": "code_interpreter"}
        });
        let out_forced_auto = degrade_without_api_key(
            &forced_mixed_additional,
            &[ApiKeyOnlyFeature::CodeInterpreterTool],
            &policy,
        )
        .unwrap();
        assert_eq!(out_forced_auto["tool_choice"], "auto");

        // 5. allowed_tools with mode: "required" and only code_interpreter allowed, even with an
        // outside surviving function tool -> sets tool_choice: "none" so outside tool is not called
        let allowed_only_hosted = json!({
            "tools": [{"type": "function", "name": "outside_fn"}],
            "input": [{
                "type": "additional_tools",
                "tools": [{"type": "code_interpreter", "container": {"type": "auto"}}]
            }],
            "tool_choice": {
                "type": "allowed_tools",
                "mode": "required",
                "tools": [{"type": "code_interpreter"}]
            }
        });
        let out_allowed_cleared = degrade_without_api_key(
            &allowed_only_hosted,
            &[ApiKeyOnlyFeature::CodeInterpreterTool],
            &policy,
        )
        .unwrap();
        assert_eq!(out_allowed_cleared["tool_choice"], "none");

        // 6. allowed_tools with mode: "required" and surviving lookup function in additional_tools ->
        // strips code_interpreter selector and preserves subset + mode
        let allowed_mixed = json!({
            "input": [{
                "type": "additional_tools",
                "tools": [
                    {"type": "code_interpreter", "container": {"type": "auto"}},
                    {"type": "function", "name": "lookup"}
                ]
            }],
            "tool_choice": {
                "type": "allowed_tools",
                "mode": "required",
                "tools": [{"type": "code_interpreter"}, {"type": "function", "name": "lookup"}]
            }
        });
        let out_allowed_mixed = degrade_without_api_key(
            &allowed_mixed,
            &[ApiKeyOnlyFeature::CodeInterpreterTool],
            &policy,
        )
        .unwrap();
        assert_eq!(out_allowed_mixed["tool_choice"]["mode"], "required");
        assert_eq!(
            out_allowed_mixed["tool_choice"]["tools"],
            json!([{"type": "function", "name": "lookup"}])
        );

        // 7. allowed_tools with mode: "auto": sets "none" when emptied (preserving subset ban over
        // outside functions), preserves subset + mode when lookup survives
        let allowed_auto_only = json!({
            "tools": [{"type": "function", "name": "outside_fn"}],
            "input": [{
                "type": "additional_tools",
                "tools": [{"type": "code_interpreter", "container": {"type": "auto"}}]
            }],
            "tool_choice": {
                "type": "allowed_tools",
                "mode": "auto",
                "tools": [{"type": "code_interpreter"}]
            }
        });
        let out_allowed_auto_cleared = degrade_without_api_key(
            &allowed_auto_only,
            &[ApiKeyOnlyFeature::CodeInterpreterTool],
            &policy,
        )
        .unwrap();
        assert_eq!(out_allowed_auto_cleared["tool_choice"], "none");

        let allowed_auto_mixed = json!({
            "input": [{
                "type": "additional_tools",
                "tools": [
                    {"type": "code_interpreter", "container": {"type": "auto"}},
                    {"type": "function", "name": "lookup"}
                ]
            }],
            "tool_choice": {
                "type": "allowed_tools",
                "mode": "auto",
                "tools": [{"type": "code_interpreter"}, {"type": "function", "name": "lookup"}]
            }
        });
        let out_allowed_auto_mixed = degrade_without_api_key(
            &allowed_auto_mixed,
            &[ApiKeyOnlyFeature::CodeInterpreterTool],
            &policy,
        )
        .unwrap();
        assert_eq!(out_allowed_auto_mixed["tool_choice"]["mode"], "auto");
        assert_eq!(
            out_allowed_auto_mixed["tool_choice"]["tools"],
            json!([{"type": "function", "name": "lookup"}])
        );
    }
}
