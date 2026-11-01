//! Strict Anthropic Messages API validation and prompt-cache keying for #988.

use axum::http::HeaderMap;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

/// Anthropic per-model thinking table:
/// <https://platform.claude.com/docs/en/build-with-claude/thinking#configuring-thinking>
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThinkingModelRule {
    Only,
    BetweenTools,
    AdaptiveEnabledDisabled,
    Disabled,
}

/// Returns the documented thinking union accepted by a model.
pub fn model_thinking_rule(model: &str) -> Option<ThinkingModelRule> {
    match model {
        "claude-opus-5-5" => Some(ThinkingModelRule::Only),
        "claude-sonnet-5-5" => Some(ThinkingModelRule::BetweenTools),
        "claude-opus-4-6" | "claude-sonnet-4-6" => Some(ThinkingModelRule::AdaptiveEnabledDisabled),
        "claude-haiku-5-5" | "claude-opus-5" => Some(ThinkingModelRule::Disabled),
        _ => None,
    }
}

/// Validates a Messages API request, including header-gated thinking controls.
pub fn validate_messages_request(value: &Value, headers: &HeaderMap) -> Result<(), String> {
    // /en/api/messages/create.md, Body parameters: object, model and integer max_tokens.
    let object = value
        .as_object()
        .ok_or_else(|| "request body must be an object".to_owned())?;
    let model = object
        .get("model")
        .and_then(Value::as_str)
        .ok_or_else(|| "model must be a string".to_owned())?;
    let max_tokens = object
        .get("max_tokens")
        .and_then(Value::as_u64)
        .ok_or_else(|| "max_tokens must be a non-negative integer".to_owned())?;
    if max_tokens == 0 {
        // Anthropic prompt caching limitations:
        // https://platform.claude.com/docs/en/build-with-claude/prompt-caching#limitations
        // max_tokens=0 rejects stream=true, enabled thinking, structured outputs,
        // and forced tool_choice variants. The existing pre-warming contract
        // additionally requires stream to be omitted entirely.
        if object.get("stream").is_some() {
            return Err("renewal requests must omit stream".to_owned());
        }
        if object
            .get("thinking")
            .and_then(|thinking| thinking.get("type"))
            .and_then(Value::as_str)
            == Some("enabled")
        {
            return Err("max_tokens=0 does not support thinking.type=enabled".to_owned());
        }
        if object
            .get("output_config")
            .and_then(|config| config.get("format"))
            .is_some()
        {
            return Err("max_tokens=0 does not support output_config.format".to_owned());
        }
        if object
            .get("tool_choice")
            .and_then(|choice| choice.get("type"))
            .and_then(Value::as_str)
            .is_some_and(|kind| matches!(kind, "tool" | "any"))
        {
            return Err("max_tokens=0 does not support forced tool_choice".to_owned());
        }
        validate_breakpoints(value)?;
    }
    validate_thinking(
        object.get("thinking"),
        model,
        max_tokens,
        effort(object),
        headers,
    )?;
    validate_effort(object.get("output_config"))?;
    validate_tool_choice(
        object.get("tool_choice"),
        model,
        object
            .get("thinking")
            .and_then(|thinking| thinking.get("type"))
            .and_then(Value::as_str),
    )?;
    validate_messages(object.get("messages"))?;
    Ok(())
}

/// Validates OAuth renewal headers (A7). Live requests may use x-api-key.
pub fn validate_renewal_headers(headers: &HeaderMap) -> Result<(), String> {
    // QA A7 mirrors the OAuth signer's Bearer/oauth-2025-04-20 contract.
    let auth = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| "renewal requires Authorization header".to_owned())?;
    if !auth.starts_with("Bearer ") || auth.len() <= "Bearer ".len() {
        return Err("renewal Authorization must be Bearer".to_owned());
    }
    if headers.get("x-api-key").is_some() {
        return Err("renewal must not include x-api-key".to_owned());
    }
    let beta = headers
        .get("anthropic-beta")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| "renewal requires anthropic-beta".to_owned())?;
    if !beta
        .split(',')
        .map(str::trim)
        .any(|flag| flag == "oauth-2025-04-20")
    {
        return Err("renewal anthropic-beta must include oauth-2025-04-20".to_owned());
    }
    if headers.get("anthropic-version").is_none() {
        return Err("renewal requires anthropic-version".to_owned());
    }
    Ok(())
}

/// Builds the deepest cache key required by A8.
pub fn cache_key(value: &Value) -> String {
    cache_keys(value)
        .into_iter()
        .last()
        .unwrap_or_else(|| request_key(value, &Value::Null))
}

/// Builds one key for every cache breakpoint in API order: tools, system, messages.
pub fn cache_keys(value: &Value) -> Vec<String> {
    // /en/build-with-claude/prompt-caching, How prompt caching works: prefix order.
    let mut keys = Vec::new();
    for field in ["tools", "system"] {
        if let Some(items) = value.get(field).and_then(Value::as_array) {
            for (index, item) in items.iter().enumerate() {
                if item.get("cache_control").is_some() {
                    let mut prefix = json!({});
                    if field == "system"
                        && let Some(tools) = value.get("tools")
                    {
                        prefix["tools"] = tools.clone();
                    }
                    prefix[field] = Value::Array(items[..=index].to_vec());
                    keys.push(request_key(value, &prefix));
                }
            }
        } else if value
            .get(field)
            .and_then(Value::as_object)
            .and_then(|object| object.get("cache_control"))
            .is_some()
        {
            let mut prefix = json!({});
            if field == "system"
                && let Some(tools) = value.get("tools")
            {
                prefix["tools"] = tools.clone();
            }
            prefix[field] = value[field].clone();
            keys.push(request_key(value, &prefix));
        }
    }
    if let Some(messages) = value.get("messages").and_then(Value::as_array) {
        for (message_index, message) in messages.iter().enumerate() {
            if message.get("cache_control").is_some() {
                let mut prefix = value.clone();
                prefix["messages"] = Value::Array(messages[..=message_index].to_vec());
                keys.push(request_key(value, &prefix));
            }
            if let Some(blocks) = message.get("content").and_then(Value::as_array) {
                for (block_index, block) in blocks.iter().enumerate() {
                    if block.get("cache_control").is_some() {
                        let mut prefix = value.clone();
                        let mut selected = messages[..=message_index].to_vec();
                        if let Some(content) = selected[message_index]
                            .get_mut("content")
                            .and_then(Value::as_array_mut)
                        {
                            content.truncate(block_index + 1);
                        }
                        prefix["messages"] = Value::Array(selected);
                        keys.push(request_key(value, &prefix));
                    }
                }
            }
        }
    }
    keys
}

fn request_key(value: &Value, prefix: &Value) -> String {
    let mut key = Map::new();
    key.insert(
        "model".to_owned(),
        value.get("model").cloned().unwrap_or(Value::Null),
    );
    for field in ["tools", "system", "messages"] {
        key.insert(
            field.to_owned(),
            prefix.get(field).cloned().unwrap_or(Value::Null),
        );
    }
    key.insert("thinking".to_owned(), thinking_key(value.get("thinking")));
    key.insert(
        "effort".to_owned(),
        value
            .get("output_config")
            .and_then(|config| config.get("effort"))
            .cloned()
            .unwrap_or_else(|| Value::String("default".to_owned())),
    );
    key.insert(
        "tool_choice".to_owned(),
        value.get("tool_choice").cloned().unwrap_or(Value::Null),
    );
    digest(&Value::Object(key))
}

fn validate_thinking(
    // /en/api/messages/create.md, thinking: strict tagged union and enabled budget.
    thinking: Option<&Value>,
    model: &str,
    max_tokens: u64,
    effort: Option<&str>,
    headers: &HeaderMap,
) -> Result<(), String> {
    let Some(thinking) = thinking else {
        return Ok(());
    };
    let object = thinking
        .as_object()
        .ok_or_else(|| "thinking must be an object".to_owned())?;
    let kind = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| "thinking.type must be a string".to_owned())?;
    match kind {
        "disabled" => {
            // Thinking guide line 537: disabled permits no display or other fields.
            if object.len() != 1 {
                return Err("thinking.disabled only allows type".to_owned());
            }
        }
        "enabled" => {
            // Messages reference, ThinkingConfigEnabled: type, budget_tokens and display.
            for key in object.keys() {
                if key != "type" && key != "budget_tokens" && key != "display" {
                    return Err(format!("thinking.enabled does not allow {key}"));
                }
            }
            let budget = object
                .get("budget_tokens")
                .and_then(Value::as_u64)
                .ok_or_else(|| "thinking.enabled requires integer budget_tokens".to_owned())?;
            if budget < 1024 || budget >= max_tokens {
                return Err("thinking.enabled budget_tokens must satisfy 1024 <= budget_tokens < max_tokens".to_owned());
            }
            validate_display(object.get("display"), headers)?;
        }
        "adaptive" => {
            // Thinking guide lines 507-511 and 1120: adaptive display and beta binding.
            for key in object.keys() {
                if key != "type" && key != "display" && key != "block_binding" {
                    return Err(format!("thinking.adaptive does not allow {key}"));
                }
            }
            validate_display(object.get("display"), headers)?;
            if let Some(binding) = object.get("block_binding") {
                // Thinking guide line 1120: beta-gated error/drop_block binding behavior.
                if headers
                    .get("anthropic-beta")
                    .and_then(|value| value.to_str().ok())
                    .map(|value| {
                        value
                            .split(',')
                            .map(str::trim)
                            .any(|flag| flag == "thinking-binding-controls-2026-08-01")
                    })
                    != Some(true)
                {
                    return Err("thinking.block_binding requires anthropic-beta thinking-binding-controls-2026-08-01".to_owned());
                }
                let binding = binding
                    .as_object()
                    .ok_or_else(|| "thinking.block_binding must be an object".to_owned())?;
                if binding.keys().any(|key| key != "prefix_mismatch_behavior") {
                    return Err("thinking.block_binding has unknown key".to_owned());
                }
                if !matches!(
                    binding
                        .get("prefix_mismatch_behavior")
                        .and_then(Value::as_str),
                    Some("error") | Some("drop_block")
                ) {
                    return Err(
                        "thinking.block_binding.prefix_mismatch_behavior is invalid".to_owned()
                    );
                }
            }
        }
        "between_tools" => {
            // Thinking guide line 1022: between_tools accepts only type.
            if object.len() != 1 {
                return Err("thinking.between_tools only allows type".to_owned());
            }
        }
        _ => return Err("thinking.type is invalid".to_owned()),
    }
    match model_thinking_rule(model) {
        // Thinking guide lines 83-103: per-model modes and effort restrictions.
        Some(ThinkingModelRule::Only) if kind != "adaptive" => {
            return Err(format!("{model} accepts adaptive thinking only"));
        }
        Some(ThinkingModelRule::BetweenTools)
            if kind != "adaptive"
                && !(kind == "between_tools" && effort_allows_between_tools(effort)) =>
        {
            return Err(format!("{model} rejects {kind} thinking"));
        }
        Some(ThinkingModelRule::AdaptiveEnabledDisabled) if kind == "between_tools" => {
            return Err(format!("{model} rejects between_tools thinking"));
        }
        Some(ThinkingModelRule::Disabled)
            if kind != "adaptive" && !(kind == "disabled" && effort_allows_disabled(effort)) =>
        {
            return Err(format!("{model} rejects {kind} thinking at this effort"));
        }
        _ => {}
    }
    Ok(())
}

fn effort(value: &Map<String, Value>) -> Option<&str> {
    value
        .get("output_config")
        .and_then(|config| config.get("effort"))
        .and_then(Value::as_str)
}

fn effort_allows_between_tools(effort: Option<&str>) -> bool {
    !matches!(effort, Some("xhigh" | "max"))
}

fn effort_allows_disabled(effort: Option<&str>) -> bool {
    !matches!(effort, Some("xhigh" | "max"))
}

// Thinking guide, "Controlling thinking display", lines 507-511 and 537.
fn validate_display(value: Option<&Value>, headers: &HeaderMap) -> Result<(), String> {
    match value {
        None | Some(Value::Null) => Ok(()),
        Some(value) if matches!(value.as_str(), Some("summarized" | "omitted")) => Ok(()),
        Some(value)
            if value.as_str() == Some("updates")
                && headers
                    .get("anthropic-beta")
                    .and_then(|header| header.to_str().ok())
                    .is_some_and(|header| {
                        header
                            .split(',')
                            .map(str::trim)
                            .any(|flag| flag == "thinking-display-updates-2026-08-18")
                    }) =>
        {
            Ok(())
        }
        _ => Err("thinking.display is invalid or missing its required beta".to_owned()),
    }
}

fn validate_effort(value: Option<&Value>) -> Result<(), String> {
    // /en/api/messages/create.md, output_config.effort: five supported values.
    let Some(config) = value else {
        return Ok(());
    };
    let object = config
        .as_object()
        .ok_or_else(|| "output_config must be an object".to_owned())?;
    if let Some(effort) = object.get("effort")
        && !matches!(
            effort.as_str(),
            Some("low" | "medium" | "high" | "xhigh" | "max")
        )
    {
        return Err("output_config.effort is invalid".to_owned());
    }
    Ok(())
}

fn validate_tool_choice(
    value: Option<&Value>,
    model: &str,
    thinking_kind: Option<&str>,
) -> Result<(), String> {
    // /en/api/messages/create.md, ToolChoiceAuto/Any/Tool/None: variant fields.
    let Some(value) = value else {
        return Ok(());
    };
    let object = value
        .as_object()
        .ok_or_else(|| "tool_choice must be an object".to_owned())?;
    let kind = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| "tool_choice.type is required".to_owned())?;
    if !matches!(kind, "auto" | "any" | "tool" | "none") {
        return Err("tool_choice.type is invalid".to_owned());
    }
    // https://platform.claude.com/docs/en/build-with-claude/thinking#limits-and-feature-compatibility
    // Forced tool use is incompatible with manual extended thinking. These
    // four models reject it on every request, regardless of thinking mode.
    if matches!(kind, "any" | "tool") {
        if matches!(
            model,
            "claude-opus-5-5" | "claude-sonnet-5-5" | "claude-fable-5-1" | "claude-mythos-5-1"
        ) {
            return Err(format!(
                "{model} rejects forced tool_choice on every request"
            ));
        }
        if thinking_kind == Some("enabled") {
            return Err("forced tool_choice is incompatible with thinking.type=enabled".to_owned());
        }
    }
    if kind == "tool" && object.get("name").and_then(Value::as_str).is_none() {
        return Err("tool_choice.tool requires name".to_owned());
    }
    if kind != "tool" && object.contains_key("name") {
        return Err("tool_choice.name is only valid with type tool".to_owned());
    }
    if kind == "none" && object.contains_key("disable_parallel_tool_use") {
        return Err("tool_choice.disable_parallel_tool_use is invalid with none".to_owned());
    }
    Ok(())
}

fn validate_messages(value: Option<&Value>) -> Result<(), String> {
    // Messages reference: nonempty array; QA A6 restricts these fixtures to user-ending turns.
    let messages = value
        .and_then(Value::as_array)
        .ok_or_else(|| "messages must be an array".to_owned())?;
    if messages.is_empty() {
        return Err("messages must not be empty".to_owned());
    }
    if messages
        .last()
        .and_then(|message| message.get("role"))
        .and_then(Value::as_str)
        != Some("user")
    {
        return Err("last message must have role user".to_owned());
    }
    for (index, message) in messages.iter().enumerate() {
        // /en/agents-and-tools/tool-use/handle-tool-calls: immediate next-user tool results.
        if message.get("role").and_then(Value::as_str) != Some("assistant") {
            continue;
        }
        let uses = blocks(message)
            .into_iter()
            .filter(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"))
            .collect::<Vec<_>>();
        if uses.is_empty() {
            continue;
        }
        let next = messages
            .get(index + 1)
            .ok_or_else(|| "assistant tool_use requires next user message".to_owned())?;
        if next.get("role").and_then(Value::as_str) != Some("user") {
            return Err("assistant tool_use must be followed by user".to_owned());
        }
        let results = blocks(next)
            .into_iter()
            .filter(|block| block.get("type").and_then(Value::as_str) == Some("tool_result"))
            .collect::<Vec<_>>();
        for use_block in uses {
            let id = use_block.get("id").and_then(Value::as_str);
            if !results
                .iter()
                .any(|result| result.get("tool_use_id").and_then(Value::as_str) == id)
            {
                return Err("assistant tool_use lacks matching next-message tool_result".to_owned());
            }
        }
    }
    Ok(())
}

fn validate_breakpoints(value: &Value) -> Result<(), String> {
    // /en/build-with-claude/prompt-caching, Limitations: at most four breakpoints.
    let mut count = 0_u8;
    if value.get("cache_control").is_some() {
        count += 1;
    }
    for field in ["system", "tools"] {
        count = count.saturating_add(count_breakpoints(value.get(field)));
    }
    if let Some(messages) = value.get("messages").and_then(Value::as_array) {
        for message in messages {
            count = count.saturating_add(u8::from(message.get("cache_control").is_some()));
            count = count.saturating_add(count_breakpoints(message.get("content")));
        }
    }
    if count > 4 {
        return Err("renewal has more than four cache breakpoints".to_owned());
    }
    Ok(())
}

fn count_breakpoints(value: Option<&Value>) -> u8 {
    value
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter(|value| value.get("cache_control").is_some())
                .count() as u8
        })
        .unwrap_or(0)
}

fn blocks(message: &Value) -> Vec<&Value> {
    match message.get("content") {
        Some(Value::Array(blocks)) => blocks.iter().collect(),
        _ => Vec::new(),
    }
}

fn thinking_key(value: Option<&Value>) -> Value {
    let Some(object) = value.and_then(Value::as_object) else {
        return Value::Null;
    };
    let mut key = Map::new();
    if let Some(kind) = object.get("type") {
        key.insert("type".to_owned(), kind.clone());
    }
    if let Some(budget) = object.get("budget_tokens") {
        key.insert("budget_tokens".to_owned(), budget.clone());
    }
    Value::Object(key)
}

fn digest(value: &Value) -> String {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    hex::encode(Sha256::digest(bytes))
}

/// Anthropic error JSON used for all strict validation failures.
pub fn invalid_request_body(message: impl Into<String>) -> Value {
    json!({"type":"error","error":{"type":"invalid_request_error","message":message.into()}})
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderName, HeaderValue};

    fn headers(with_binding_beta: bool) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if with_binding_beta {
            headers.insert(
                HeaderName::from_static("anthropic-beta"),
                HeaderValue::from_static("thinking-binding-controls-2026-08-01"),
            );
        }
        headers
    }

    fn base(thinking: Value) -> Value {
        json!({"model":"claude-opus-5-5","max_tokens":4096,"thinking":thinking,"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]})
    }
    fn base_for_model(model: &str, max_tokens: u64, thinking: Value) -> Value {
        json!({
            "model": model,
            "max_tokens": max_tokens,
            "thinking": thinking,
            "messages": [{"role":"user","content":[{"type":"text","text":"hi"}]}]
        })
    }

    fn apply_zero_token_case(value: &mut Value, case: usize) {
        match case {
            0 => value["stream"] = json!(true),
            1 => value["thinking"] = json!({"type":"enabled","budget_tokens":1024}),
            2 => value["output_config"] = json!({"format":{"type":"json_schema"}}),
            3 => value["tool_choice"] = json!({"type":"any"}),
            _ => unreachable!("fixed self-test case"),
        }
    }

    #[test]
    fn zero_token_limitations_reject_and_live_bodies_accept() {
        for (name, case) in [
            ("stream", 0),
            ("enabled thinking", 1),
            ("structured output", 2),
            ("forced any tool", 3),
        ] {
            let mut renewal = base_for_model("claude-opus-4-6", 0, json!({"type":"adaptive"}));
            apply_zero_token_case(&mut renewal, case);
            assert!(
                validate_messages_request(&renewal, &headers(false)).is_err(),
                "{name} must be rejected for max_tokens=0"
            );

            renewal["max_tokens"] = json!(4096);
            assert!(
                validate_messages_request(&renewal, &headers(false)).is_ok(),
                "{name} must be accepted for live traffic"
            );
        }
    }

    #[test]
    fn four_six_models_accept_adaptive_enabled_disabled_and_reject_between_tools() {
        for model in ["claude-opus-4-6", "claude-sonnet-4-6"] {
            for thinking in [
                json!({"type":"adaptive"}),
                json!({"type":"enabled","budget_tokens":1024}),
                json!({"type":"disabled"}),
            ] {
                let value = base_for_model(model, 4096, thinking.clone());
                assert!(
                    validate_messages_request(&value, &headers(false)).is_ok(),
                    "{model} should accept {thinking}"
                );
            }
            let value = base_for_model(model, 4096, json!({"type":"between_tools"}));
            assert!(
                validate_messages_request(&value, &headers(false)).is_err(),
                "{model} should reject between_tools"
            );
        }
    }

    #[test]
    fn forced_tools_reject_incompatible_models_on_live_and_renewal_requests() {
        for model in [
            "claude-opus-5-5",
            "claude-sonnet-5-5",
            "claude-fable-5-1",
            "claude-mythos-5-1",
        ] {
            for max_tokens in [0, 4096] {
                for choice in [json!({"type":"any"}), json!({"type":"tool","name":"bash"})] {
                    let mut value = base_for_model(model, max_tokens, json!({"type":"adaptive"}));
                    value["tool_choice"] = choice;
                    assert!(
                        validate_messages_request(&value, &headers(false)).is_err(),
                        "{model} must reject forced tools at max_tokens={max_tokens}"
                    );
                }
            }
        }
    }

    #[test]
    fn forced_tools_accept_adaptive_and_reject_manual_thinking_on_four_six_models() {
        for model in ["claude-opus-4-6", "claude-sonnet-4-6"] {
            for choice in [json!({"type":"any"}), json!({"type":"tool","name":"bash"})] {
                let mut value = base_for_model(model, 4096, json!({"type":"adaptive"}));
                value["tool_choice"] = choice;
                assert!(validate_messages_request(&value, &headers(false)).is_ok());
                value["thinking"] = json!({"type":"enabled","budget_tokens":1024});
                assert!(validate_messages_request(&value, &headers(false)).is_err());
                for choice in [json!({"type":"auto"}), json!({"type":"none"})] {
                    value["tool_choice"] = choice;
                    assert!(validate_messages_request(&value, &headers(false)).is_ok());
                }
            }
        }
    }

    #[test]
    fn rejects_disabled_siblings_from_986() {
        let value = base(
            json!({"type":"disabled","display":"summarized","block_binding":{"prefix_mismatch_behavior":"drop_block"}}),
        );
        assert!(validate_messages_request(&value, &headers(true)).is_err());
    }

    #[test]
    fn rejects_disabled_on_opus_55() {
        assert!(
            validate_messages_request(&base(json!({"type":"disabled"})), &headers(false)).is_err()
        );
    }

    #[test]
    fn accepts_adaptive_display_and_binding() {
        let value = base(
            json!({"type":"adaptive","display":"summarized","block_binding":{"prefix_mismatch_behavior":"drop_block"}}),
        );
        assert!(validate_messages_request(&value, &headers(true)).is_ok());
    }

    #[test]
    fn rejects_enabled_budget_at_max() {
        let value = base(json!({"type":"enabled","budget_tokens":4096}));
        assert!(validate_messages_request(&value, &headers(false)).is_err());
    }

    #[test]
    fn rejects_block_binding_without_beta() {
        let value = base(
            json!({"type":"adaptive","block_binding":{"prefix_mismatch_behavior":"drop_block"}}),
        );
        assert!(validate_messages_request(&value, &headers(false)).is_err());
    }

    #[test]
    fn adaptive_display_rejects_unknown_values() {
        let value = base(json!({"type": "adaptive", "display": "invalid"}));
        assert!(validate_messages_request(&value, &headers(false)).is_err());
    }

    #[test]
    fn display_updates_requires_its_beta() {
        let value = base(json!({"type": "adaptive", "display": "updates"}));
        let mut request_headers = headers(false);
        assert!(validate_messages_request(&value, &request_headers).is_err());
        request_headers.insert(
            HeaderName::from_static("anthropic-beta"),
            HeaderValue::from_static("thinking-display-updates-2026-08-18"),
        );
        assert!(validate_messages_request(&value, &request_headers).is_ok());
    }

    #[test]
    fn system_breakpoint_ignores_trailing_messages() {
        let mut live = base(json!({"type": "adaptive"}));
        live["tools"] = json!([{"name": "bash", "input_schema": {"type": "object"}}]);
        live["system"] = json!([{
            "type": "text",
            "text": "cached system",
            "cache_control": {"type": "ephemeral"},
        }]);
        let mut renewal = live.clone();
        renewal["max_tokens"] = json!(0);
        renewal["messages"] = json!([{"role": "user", "content": "."}]);
        assert_eq!(cache_key(&live), cache_key(&renewal));
    }

    #[test]
    fn message_breakpoint_includes_earlier_blocks() {
        let mut original = base(json!({"type": "adaptive"}));
        original["messages"] = json!([{
            "role": "user",
            "content": [
                {"type": "text", "text": "earlier block"},
                {
                    "type": "text",
                    "text": "cached endpoint",
                    "cache_control": {"type": "ephemeral"},
                },
            ],
        }]);
        let mut changed = original.clone();
        changed["messages"][0]["content"][0]["text"] = json!("changed earlier block");
        assert_ne!(cache_key(&original), cache_key(&changed));
    }
}
