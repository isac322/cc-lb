//! OpenAI Responses GPT-5.6+ prompt-cache keepalive policy.
//!
//! This module is deliberately independent from Anthropic's cache-control
//! dialect. OpenAI's retention is a request-level policy and its usage fields
//! are nested under `input_tokens_details`.

use serde_json::Value;

pub const MIN_PREFIX_TOKENS: u64 = 1_024;
pub const MIN_MAX_OUTPUT_TOKENS: u64 = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Lifetime {
    ThirtyMinutes,
    TwelveHours,
}

impl Lifetime {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ThirtyMinutes => "30m",
            Self::TwelveHours => "12h",
        }
    }

    pub const fn seconds(self) -> u64 {
        match self {
            Self::ThirtyMinutes => 1_800,
            Self::TwelveHours => 43_200,
        }
    }
}

/// Parses the request-level retention option. Any mixed or unknown values
/// fail closed instead of guessing which provider lifetime was requested.
pub fn parse_lifetime(value: &Value) -> Option<Lifetime> {
    let mut declared = Vec::new();
    collect_lifetimes(value, false, &mut declared);
    declared.into_iter().next()
}

fn parse_lifetime_value(raw: &str) -> Option<Lifetime> {
    match raw.to_ascii_lowercase().as_str() {
        "30m" | "30_min" | "thirty_minutes" => Some(Lifetime::ThirtyMinutes),
        "12h" | "12_hr" | "twelve_hours" => Some(Lifetime::TwelveHours),
        _ => None,
    }
}

fn collect_lifetimes(value: &Value, in_options: bool, declared: &mut Vec<Lifetime>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if key == "prompt_cache_retention" || (in_options && key == "retention") {
                    if let Some(raw) = child.as_str() {
                        if let Some(lifetime) = parse_lifetime_value(raw) {
                            declared.push(lifetime);
                        }
                    }
                }
                collect_lifetimes(child, key == "prompt_cache_options", declared);
            }
        }
        Value::Array(values) => {
            for child in values {
                collect_lifetimes(child, in_options, declared);
            }
        }
        _ => {}
    }
}

/// Returns the lifetime only when all configured retention declarations agree.
pub fn parse_lifetime_conservative(value: &Value) -> Option<Lifetime> {
    let mut declared = Vec::new();
    collect_lifetimes(value, false, &mut declared);
    let first = declared.first().copied()?;
    declared
        .iter()
        .all(|lifetime| *lifetime == first)
        .then_some(first)
}

pub fn is_gpt56_or_newer(model: &str) -> bool {
    let lower = model.to_ascii_lowercase();
    let Some(rest) = lower.strip_prefix("gpt-") else {
        return false;
    };
    let mut parts = rest.split(['.', '-']);
    let major = parts.next().and_then(|part| part.parse::<u64>().ok());
    let minor = parts.next().and_then(|part| part.parse::<u64>().ok());
    match (major, minor) {
        (Some(major), Some(minor)) => major > 5 || (major == 5 && minor >= 6),
        _ => lower.starts_with("gpt-6"),
    }
}

pub fn eligible_prefix(model: &str, prefix_tokens: u64, lifetime: Option<Lifetime>) -> bool {
    is_gpt56_or_newer(model) && prefix_tokens >= MIN_PREFIX_TOKENS && lifetime.is_some()
}

pub fn visible_prefix_tokens(value: &Value) -> u64 {
    let input = value.get("input").or_else(|| value.get("messages"));
    let bytes = input.map_or(0, |input| {
        sonic_rs::to_vec(input).map_or(0, |raw| raw.len())
    });
    (bytes as u64 / 4).min(10_000_000)
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
    let ratio = write_multiplier(lifetime) / read_multiplier(model);
    if !ratio.is_finite() || ratio <= 0.0 {
        return None;
    }
    Some(ratio.ceil() as u64)
}

pub fn cached_tokens(value: &Value) -> Option<u64> {
    value
        .get("usage")
        .and_then(|usage| usage.get("input_tokens_details"))
        .and_then(|details| details.get("cached_tokens"))
        .and_then(Value::as_u64)
}

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
    fn openai_gpt56_cache_lifetime_parsing() {
        assert_eq!(
            parse_lifetime(&json!({"prompt_cache_retention":"30m"})),
            Some(Lifetime::ThirtyMinutes)
        );
        assert_eq!(
            parse_lifetime(&json!({"prompt_cache_retention":"12h"})),
            Some(Lifetime::TwelveHours)
        );
        assert_eq!(
            parse_lifetime_conservative(&json!({
                "prompt_cache_retention":"30m",
                "prompt_cache_options":{"retention":"12h"}
            })),
            None
        );
        assert_eq!(
            parse_lifetime_conservative(&json!({
                "prompt_cache_retention":"30m",
                "input":[{"prompt_cache_retention":"12h"}]
            })),
            None
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
    fn openai_keepalive_does_not_reanchor_12h() {
        assert_eq!(Lifetime::TwelveHours.seconds(), 43_200);
        assert_ne!(
            Lifetime::ThirtyMinutes.seconds(),
            Lifetime::TwelveHours.seconds()
        );
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
