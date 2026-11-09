//! OpenAI Responses GPT-5.6+ prompt-cache keepalive policy.
//!
//! This module is deliberately independent from Anthropic's cache-control
//! dialect. OpenAI's retention is a request-level policy and its usage fields
//! are nested under `input_tokens_details`.

use serde_json::Value;

pub const MIN_PREFIX_TOKENS: u64 = 1_024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Lifetime {
    ThirtyMinutes,
    TwelveHours,
}

/// The single modern OpenAI Responses interpretation shared by request
/// observation and keep-alive scheduling.
///
/// `selected_boundary` is an input path, not an arbitrary JSON traversal path.
/// It identifies the last supported explicit content block, or the latest
/// supported implicit message ending.  A breakpoint's TTL applies only to that
/// selected boundary; we intentionally do not model mixed breakpoint lifetimes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Interpretation {
    pub(crate) lifetime: Option<Lifetime>,
    pub(crate) mode: Mode,
    pub(crate) selected_boundary: Option<String>,
    pub(crate) explicit_count: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Mode {
    Explicit,
    Implicit,
}

/// Resolve the documented modern GPT-5.6+ Responses cache policy.
///
/// Only real input content blocks are inspected.  Metadata, tool schemas, and
/// other arbitrary JSON are deliberately outside this dialect.  An unknown
/// value on a real breakpoint fails closed.
pub(crate) fn interpret_modern(value: &Value) -> Option<Interpretation> {
    let options = match value.get("prompt_cache_options") {
        None => None,
        Some(Value::Object(options)) => Some(options),
        Some(_) => return None,
    };
    if let Some(retention) = value.get("prompt_cache_retention") {
        match retention {
            Value::String(raw) if matches!(raw.as_str(), "in_memory" | "24h") => {}
            _ => return None,
        }
    }
    let request_lifetime = match options.and_then(|options| options.get("ttl")) {
        None => Lifetime::ThirtyMinutes,
        Some(Value::String(raw)) => parse_lifetime_value(raw)?,
        Some(_) => return None,
    };
    let requested_mode = match options.and_then(|options| options.get("mode")) {
        None => Mode::Implicit,
        Some(Value::String(mode)) if mode.eq_ignore_ascii_case("implicit") => Mode::Implicit,
        Some(Value::String(mode)) if mode.eq_ignore_ascii_case("explicit") => Mode::Explicit,
        Some(_) => return None,
    };
    let input = value.get("input")?;
    let explicit = explicit_breakpoints(input)?;
    if explicit.too_many {
        return Some(Interpretation {
            lifetime: None,
            mode: Mode::Explicit,
            selected_boundary: None,
            explicit_count: explicit.count,
        });
    }
    if requested_mode == Mode::Explicit && explicit.count == 0 {
        return Some(Interpretation {
            lifetime: None,
            mode: Mode::Explicit,
            selected_boundary: None,
            explicit_count: 0,
        });
    }
    if let Some((item_index, field, block_index, marker_ttl)) = explicit.last {
        return Some(Interpretation {
            lifetime: Some(marker_ttl.unwrap_or(request_lifetime)),
            mode: Mode::Explicit,
            selected_boundary: Some(format!("input[{item_index}].{field}[{block_index}]")),
            explicit_count: explicit.count,
        });
    }
    let selected_boundary = implicit_boundary(input)?;
    Some(Interpretation {
        lifetime: Some(request_lifetime),
        mode: Mode::Implicit,
        selected_boundary: Some(selected_boundary),
        explicit_count: 0,
    })
}

/// Returns the number of supported explicit input boundaries, or `None` when
/// the input shape contains an invalid real breakpoint.
pub(crate) fn explicit_breakpoint_count(value: &Value) -> Option<usize> {
    let Some(input) = value.get("input") else {
        return Some(0);
    };
    Some(explicit_breakpoints(input)?.count)
}
/// Retention used by pre-GPT-5.6 Responses models. This is intentionally
/// independent from modern `prompt_cache_options.ttl`.
pub(crate) fn legacy_retention_secs(value: &Value) -> Option<Option<u64>> {
    match value.get("prompt_cache_retention") {
        None => Some(None),
        Some(Value::String(raw)) if raw == "in_memory" => Some(Some(0)),
        Some(Value::String(raw)) if raw == "24h" => Some(Some(24 * 60 * 60)),
        Some(_) => None,
    }
}

/// Resolves the renewable OpenAI Responses prefix lifetime.
#[cfg(test)]
fn parse_lifetime_conservative(value: &Value) -> Option<Lifetime> {
    interpret_modern(value)?.lifetime
}

fn parse_lifetime_value(raw: &str) -> Option<Lifetime> {
    match raw {
        "30m" => Some(Lifetime::ThirtyMinutes),
        "12h" => Some(Lifetime::TwelveHours),
        _ => None,
    }
}

#[derive(Clone, Copy)]
struct ExplicitScan {
    count: usize,
    last: Option<(usize, &'static str, usize, Option<Lifetime>)>,
    too_many: bool,
}

fn explicit_breakpoints(input: &Value) -> Option<ExplicitScan> {
    let mut scan = ExplicitScan {
        count: 0,
        last: None,
        too_many: false,
    };
    match input {
        Value::String(_) => {}
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                let tool_output = matches!(
                    item.get("type").and_then(Value::as_str),
                    Some("function_call_output" | "custom_tool_call_output")
                );
                let field = if tool_output {
                    "output"
                } else {
                    let Some(role) = item.get("role").and_then(Value::as_str) else {
                        continue;
                    };
                    if !matches!(role, "developer" | "system" | "user" | "assistant" | "tool") {
                        continue;
                    }
                    "content"
                };
                let Some(Value::Array(blocks)) = item.get(field) else {
                    continue;
                };
                for (block_index, block) in blocks.iter().enumerate() {
                    if !is_supported_content_block(block, tool_output)
                        || block.get("prompt_cache_breakpoint").is_none()
                    {
                        continue;
                    }
                    scan.count += 1;
                    if scan.count > 4 {
                        scan.too_many = true;
                        return Some(scan);
                    }
                    let lifetime = parse_content_breakpoint(block)?;
                    scan.last = Some((index, field, block_index, lifetime));
                }
            }
        }
        _ => return None,
    }
    Some(scan)
}

fn is_supported_content_block(value: &Value, tool_output: bool) -> bool {
    match value.get("type").and_then(Value::as_str) {
        Some("input_text" | "input_image" | "input_file") => true,
        Some("input_audio" | "computer_screenshot") => !tool_output,
        _ => false,
    }
}

fn parse_content_breakpoint(block: &Value) -> Option<Option<Lifetime>> {
    let marker = block.get("prompt_cache_breakpoint")?.as_object()?;
    match marker.get("ttl") {
        None => Some(None),
        Some(Value::String(raw)) => Some(Some(parse_lifetime_value(raw)?)),
        Some(_) => None,
    }
}

fn implicit_boundary(input: &Value) -> Option<String> {
    if input.is_string() {
        return Some("input".to_owned());
    }
    let messages = input.as_array()?;
    let mut selected = None;
    let mut index = 0;
    if messages
        .first()
        .and_then(|message| message.get("role"))
        .and_then(Value::as_str)
        == Some("developer")
    {
        while index < messages.len()
            && messages[index].get("role").and_then(Value::as_str) == Some("developer")
        {
            index += 1;
        }
        selected = Some(format!("input[{}]", index.saturating_sub(1)));
    }
    while index < messages.len() {
        let item = &messages[index];
        match item.get("role").and_then(Value::as_str) {
            Some("user") => selected = Some(format!("input[{index}]")),
            Some("tool") => {
                while index + 1 < messages.len()
                    && messages[index + 1].get("role").and_then(Value::as_str) == Some("tool")
                {
                    index += 1;
                }
                selected = Some(format!("input[{index}]"));
            }
            Some("developer" | "system" | "assistant") | None => {
                if is_tool_output_item(item) {
                    while index + 1 < messages.len() && is_tool_output_item(&messages[index + 1]) {
                        index += 1;
                    }
                    selected = Some(format!("input[{index}]"));
                }
            }
            Some(_) => {}
        }
        index += 1;
    }
    selected
}

fn is_tool_output_item(value: &Value) -> bool {
    value
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| {
            matches!(
                kind,
                "function_call_output"
                    | "custom_tool_call_output"
                    | "computer_call_output"
                    | "local_shell_call_output"
                    | "shell_call_output"
                    | "apply_patch_call_output"
            )
        })
}

pub fn is_gpt56_or_newer(model: &str) -> bool {
    let lower = model.to_ascii_lowercase();
    let Some(rest) = lower.strip_prefix("gpt-") else {
        return false;
    };
    let mut parts = rest.split(['.', '-']);
    let Some(major) = parts.next().and_then(|part| part.parse::<u64>().ok()) else {
        return false;
    };
    let minor = parts.next().and_then(|part| part.parse::<u64>().ok());
    major > 5 || (major == 5 && minor.is_some_and(|minor| minor >= 6))
}

pub fn eligible_prefix(model: &str, prefix_tokens: u64, lifetime: Option<Lifetime>) -> bool {
    is_gpt56_or_newer(model) && prefix_tokens >= MIN_PREFIX_TOKENS && lifetime.is_some()
}

/// Provider rates are represented as multipliers of ordinary input price.
pub fn read_multiplier(model: &str) -> f64 {
    if model.to_ascii_lowercase().contains("gpt-6.1-sol")
        || model.to_ascii_lowercase().contains("gpt-6.1-terra")
    {
        0.05
    } else {
        0.1
    }
}
pub const fn write_multiplier(lifetime: Lifetime) -> f64 {
    match lifetime {
        Lifetime::ThirtyMinutes => 1.25,
        Lifetime::TwelveHours => 2.5,
    }
}

pub fn read_break_even_reads(model: &str, lifetime: Lifetime) -> Option<u64> {
    if !is_gpt56_or_newer(model) {
        return None;
    }
    let ratio = write_multiplier(lifetime) / read_multiplier(model);
    if !ratio.is_finite() || ratio <= 0.0 {
        return None;
    }
    Some(ratio.ceil() as u64)
}

/// Suppresses refresh traffic unless the configured session budget can reach
/// the conservative read/write break-even boundary. A zero budget or unknown
/// model has undefined expected value and therefore fails closed.
pub fn should_schedule_refresh(model: &str, lifetime: Lifetime, max_refreshes: u32) -> bool {
    max_refreshes > 0
        && read_break_even_reads(model, lifetime)
            .is_some_and(|break_even| u64::from(max_refreshes) >= break_even)
}
#[cfg(test)]
pub fn cached_tokens(value: &Value) -> Option<u64> {
    value
        .get("usage")
        .and_then(|usage| usage.get("input_tokens_details"))
        .and_then(|details| details.get("cached_tokens"))
        .and_then(Value::as_u64)
}
#[cfg(test)]
pub fn cache_write_tokens(value: &Value) -> Option<(u64, u64)> {
    let details = value
        .get("usage")
        .and_then(|usage| usage.get("input_tokens_details"))?;
    let write = details.get("cache_write_tokens").and_then(Value::as_u64);
    let lifetime = details
        .get("cache_write_tokens_details")
        .and_then(|details| {
            let thirty = details
                .get("ttl_30m_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let twelve = details
                .get("ttl_12h_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            (thirty > 0 || twelve > 0).then_some(thirty.saturating_add(twelve))
        });
    match (write, lifetime) {
        (Some(total), Some(by_lifetime)) if total == by_lifetime => Some((total, by_lifetime)),
        (None, Some(by_lifetime)) => Some((by_lifetime, by_lifetime)),
        (Some(total), None) => Some((total, total)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn tool_output_marker_selects_its_lifetime_and_output_path() {
        for (mode, request_ttl, marker_ttl, expected_lifetime) in [
            ("implicit", "30m", Some("12h"), Lifetime::TwelveHours),
            ("explicit", "12h", Some("30m"), Lifetime::ThirtyMinutes),
            ("explicit", "12h", None, Lifetime::TwelveHours),
            ("explicit", "30m", None, Lifetime::ThirtyMinutes),
        ] {
            let mut marker = json!({"mode": "explicit"});
            if let Some(ttl) = marker_ttl {
                marker["ttl"] = json!(ttl);
            }
            let body = json!({
                "prompt_cache_options": {"mode": mode, "ttl": request_ttl},
                "input": [
                    {"role": "user", "content": "stable prefix"},
                    {"type": "function_call", "call_id": "call_owned", "name": "lookup", "arguments": "{}"},
                    {"type": "function_call_output", "call_id": "call_owned", "output": [
                        {"type": "input_text", "text": "stable result", "prompt_cache_breakpoint": marker},
                        {"type": "input_text", "text": "uncached suffix"}
                    ]},
                    {"role": "user", "content": "next question"}
                ]
            });
            assert_eq!(
                interpret_modern(&body),
                Some(Interpretation {
                    lifetime: Some(expected_lifetime),
                    mode: Mode::Explicit,
                    selected_boundary: Some("input[2].output[0]".to_owned()),
                    explicit_count: 1,
                }),
                "{mode}, request={request_ttl}, marker={marker_ttl:?}"
            );
        }
    }

    #[test]
    fn custom_tool_output_image_and_file_markers_preserve_order_and_default() {
        let mut body = json!({
            "prompt_cache_options": {"mode": "explicit"},
            "input": [{
                "type": "custom_tool_call_output", "call_id": "call_custom", "output": [
                    {"type": "input_image", "image_url": "https://example.invalid/image.png", "prompt_cache_breakpoint": {}},
                    {"type": "input_file", "file_data": "data:text/plain;base64,eA=="}
                ]
            }]
        });
        let inherited = interpret_modern(&body).expect("custom image marker");
        assert_eq!(inherited.lifetime, Some(Lifetime::ThirtyMinutes));
        assert_eq!(
            inherited.selected_boundary.as_deref(),
            Some("input[0].output[0]")
        );
        body["input"][0]["output"][1]["prompt_cache_breakpoint"] = json!({"ttl": "12h"});
        let selected = interpret_modern(&body).expect("custom file marker");
        assert_eq!(selected.lifetime, Some(Lifetime::TwelveHours));
        assert_eq!(selected.explicit_count, 2);
        assert_eq!(
            selected.selected_boundary.as_deref(),
            Some("input[0].output[1]")
        );
    }

    #[test]
    fn message_and_tool_output_markers_share_write_limit_and_order() {
        let mut body = json!({
            "prompt_cache_options": {"mode": "explicit"},
            "input": [
                {"role": "developer", "content": [
                    {"type": "input_text", "text": "one", "prompt_cache_breakpoint": {}},
                    {"type": "input_text", "text": "two", "prompt_cache_breakpoint": {}},
                    {"type": "input_text", "text": "three", "prompt_cache_breakpoint": {}}
                ]},
                {"type": "function_call_output", "call_id": "call_function", "output": [
                    {"type": "input_text", "text": "four", "prompt_cache_breakpoint": {"ttl": "12h"}}
                ]}
            ]
        });
        let four = interpret_modern(&body).expect("four writes");
        assert_eq!(four.explicit_count, 4);
        assert_eq!(four.lifetime, Some(Lifetime::TwelveHours));
        assert_eq!(
            four.selected_boundary.as_deref(),
            Some("input[1].output[0]")
        );
        body["input"].as_array_mut().unwrap().push(json!({
            "role": "user", "content": [{
                "type": "input_text", "text": "five", "prompt_cache_breakpoint": {"ttl": "30m"}
            }]
        }));
        let five = interpret_modern(&body).expect("over-limit decision");
        assert_eq!(five.explicit_count, 5);
        assert_eq!(five.lifetime, None);
        assert_eq!(five.selected_boundary, None);
        body["input"][0]["content"]
            .as_array_mut()
            .unwrap()
            .truncate(1);
        let later_message = interpret_modern(&body).expect("last message write");
        assert_eq!(later_message.explicit_count, 3);
        assert_eq!(later_message.lifetime, Some(Lifetime::ThirtyMinutes));
        assert_eq!(
            later_message.selected_boundary.as_deref(),
            Some("input[2].content[0]")
        );
    }

    #[test]
    fn tool_output_marker_rejects_unknown_and_non_string_lifetimes() {
        for ttl in [json!("forever"), json!(1800)] {
            let body = json!({
                "input": [
                    {"role": "user", "content": [
                        {"type": "input_text", "text": "prefix", "prompt_cache_breakpoint": {}}
                    ]},
                    {"type": "function_call_output", "call_id": "call_bad", "output": [
                        {"type": "input_text", "text": "result", "prompt_cache_breakpoint": {"ttl": ttl}}
                    ]}
                ]
            });
            assert_eq!(interpret_modern(&body), None);
        }
    }

    #[test]
    fn tool_output_lookalikes_do_not_declare_explicit_writes() {
        for item in [
            json!({"type": "function_call_output", "call_id": "call_string", "output": "{\"prompt_cache_breakpoint\":{\"ttl\":\"forever\"}}"}),
            json!({"type": "function_call", "call_id": "call_function", "output": [{"type": "input_text", "prompt_cache_breakpoint": {"ttl": "forever"}}]}),
            json!({"type": "computer_call_output", "output": [{"type": "input_text", "prompt_cache_breakpoint": {"ttl": "forever"}}]}),
            json!({"type": "additional_tools", "output": [{"type": "input_text", "prompt_cache_breakpoint": {"ttl": "forever"}}]}),
            json!({"type": "function_call_output", "call_id": "call_audio", "output": [{"type": "input_audio", "prompt_cache_breakpoint": {"ttl": "forever"}}]}),
            json!({"type": "custom_tool_call_output", "call_id": "call_computer", "output": [{"type": "computer_screenshot", "prompt_cache_breakpoint": {"ttl": "forever"}}]}),
            json!({"type": "function_call_output", "call_id": "call_output_text", "output": [{"type": "output_text", "prompt_cache_breakpoint": {"ttl": "forever"}}]}),
        ] {
            let body = json!({
                "prompt_cache_options": {"mode": "explicit"},
                "input": [{"role": "user", "content": "stable prefix"}, item]
            });
            let interpreted = interpret_modern(&body).expect("lookalike is not a real marker");
            assert_eq!(interpreted.explicit_count, 0);
            assert_eq!(interpreted.lifetime, None);
        }
    }

    #[test]
    fn openai_gpt56_cache_lifetime_parsing_uses_documented_responses_fields() {
        let input = json!({"input":[{"role":"user","content":"hello"}]});
        assert_eq!(
            parse_lifetime_conservative(&input),
            Some(Lifetime::ThirtyMinutes)
        );
        assert_eq!(
            parse_lifetime_conservative(&json!({
                "prompt_cache_options": {},
                "input": input["input"].clone()
            })),
            Some(Lifetime::ThirtyMinutes)
        );
        assert_eq!(
            parse_lifetime_conservative(&json!({
                "prompt_cache_options": {"ttl": "30m"},
                "input": input["input"].clone()
            })),
            Some(Lifetime::ThirtyMinutes)
        );
        assert_eq!(
            parse_lifetime_conservative(&json!({
                "prompt_cache_options": {"ttl": "12h"},
                "input": input["input"].clone()
            })),
            Some(Lifetime::TwelveHours)
        );
        assert_eq!(
            parse_lifetime_conservative(&json!({
                "prompt_cache_options": {"ttl": "30m"},
                "input": [{
                    "role": "user",
                    "content": [{
                        "type": "input_text",
                        "text": "hello",
                        "prompt_cache_breakpoint": {"ttl": "12h"}
                    }]
                }]
            })),
            Some(Lifetime::TwelveHours)
        );
        assert_eq!(
            parse_lifetime_conservative(&json!({
                "prompt_cache_options": {"ttl": "12h"},
                "input": [{
                    "role": "user",
                    "content": [{
                        "type": "input_text",
                        "text": "hello",
                        "prompt_cache_breakpoint": {}
                    }]
                }]
            })),
            Some(Lifetime::TwelveHours)
        );
        assert_eq!(
            parse_lifetime_conservative(&json!({
                "prompt_cache_options": {"ttl": "12h"},
                "input": [{
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "first", "prompt_cache_breakpoint": {"ttl": "12h"}},
                        {"type": "input_text", "text": "second", "prompt_cache_breakpoint": {"ttl": "30m"}}
                    ]
                }]
            })),
            Some(Lifetime::ThirtyMinutes)
        );
        assert_eq!(
            parse_lifetime_conservative(&json!({
                "prompt_cache_options": {"ttl": "forever"},
                "input": input["input"].clone()
            })),
            None
        );
        assert_eq!(
            parse_lifetime_conservative(&json!({
                "prompt_cache_options": {"ttl": 1800},
                "input": input["input"].clone()
            })),
            None
        );
        assert_eq!(
            parse_lifetime_conservative(&json!({
                "prompt_cache_options": {"ttl": "30m"},
                "input": [{
                    "role": "user",
                    "content": [{
                        "type": "input_text",
                        "text": "hello",
                        "prompt_cache_breakpoint": {"ttl": "forever"}
                    }]
                }]
            })),
            None
        );
        assert_eq!(
            parse_lifetime_conservative(&json!({
                "prompt_cache_options": {"mode": "explicit"},
                "input": input["input"].clone()
            })),
            None
        );
        assert_eq!(
            parse_lifetime_conservative(&json!({
                "input": [{
                    "type": "input_text",
                    "prompt_cache_breakpoint": {"ttl": "12h"}
                }]
            })),
            None
        );
        assert_eq!(
            parse_lifetime_conservative(&json!({
                "prompt_cache_options": {"ttl": "30m"},
                "metadata": {
                    "note": "{\"prompt_cache_options\":{\"ttl\":\"12h\"}}"
                },
                "tools": [{
                    "type": "function",
                    "name": "lookup",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "prompt_cache_breakpoint": {"type": "string", "description": "12h"}
                        }
                    }
                }],
                "input": input["input"].clone()
            })),
            Some(Lifetime::ThirtyMinutes)
        );
        assert_eq!(
            parse_lifetime_conservative(&json!({
                "prompt_cache_retention": "30m",
                "input": input["input"].clone()
            })),
            None
        );
        assert_eq!(
            parse_lifetime_conservative(&json!({
                "prompt_cache_options": {"retention": "30m"},
                "input": input["input"].clone()
            })),
            Some(Lifetime::ThirtyMinutes)
        );
    }

    #[test]
    fn openai_gpt56_minimum_prefix_eligibility() {
        assert!(!eligible_prefix(
            "gpt-5.5",
            MIN_PREFIX_TOKENS,
            Some(Lifetime::ThirtyMinutes)
        ));
        assert!(!eligible_prefix(
            "gpt-5.6",
            MIN_PREFIX_TOKENS - 1,
            Some(Lifetime::ThirtyMinutes)
        ));
        assert!(eligible_prefix(
            "gpt-5.6",
            MIN_PREFIX_TOKENS,
            Some(Lifetime::ThirtyMinutes)
        ));
    }

    #[test]
    fn openai_keepalive_read_refresh_cost_break_even() {
        assert_eq!(
            read_break_even_reads("gpt-5.6", Lifetime::TwelveHours),
            Some(25)
        );
        assert_eq!(
            read_break_even_reads("gpt-6.1-sol", Lifetime::TwelveHours),
            Some(50)
        );
    }

    #[test]
    fn openai_refresh_suppresses_unknown_or_negative_value() {
        assert!(!should_schedule_refresh(
            "gpt-5.6",
            Lifetime::TwelveHours,
            0
        ));
        assert!(!should_schedule_refresh(
            "gpt-5.5",
            Lifetime::TwelveHours,
            50
        ));
        assert!(!should_schedule_refresh(
            "gpt-5.6",
            Lifetime::TwelveHours,
            24
        ));
        assert!(should_schedule_refresh(
            "gpt-5.6",
            Lifetime::TwelveHours,
            25
        ));
        assert!(!should_schedule_refresh(
            "gpt-6.1-sol",
            Lifetime::TwelveHours,
            49
        ));
        assert!(should_schedule_refresh(
            "gpt-6.1-sol",
            Lifetime::TwelveHours,
            50
        ));
    }

    #[test]
    fn missing_or_ambiguous_usage_is_not_a_hit() {
        assert_eq!(cached_tokens(&json!({"usage":{}})), None);
        assert_eq!(
            cache_write_tokens(
                &json!({"usage":{"input_tokens_details":{"cache_write_tokens":1,"cache_write_tokens_details":{"ttl_30m_tokens":2}}}})
            ),
            None
        );
    }
}
