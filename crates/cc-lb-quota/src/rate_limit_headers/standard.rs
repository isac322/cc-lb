use std::collections::BTreeMap;

use cc_lb_domain::{RateLimitKind, RateLimitObservation};
use http::HeaderMap;

const DEFAULT_WINDOW: &str = "default";
const HEADER_PREFIX: &str = "anthropic-ratelimit-";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RateLimitField {
    Limit,
    Remaining,
    Reset,
}

#[derive(Default)]
struct PartialSnapshot {
    limit: Option<u64>,
    remaining: Option<u64>,
    reset: Option<String>,
}

impl PartialSnapshot {
    fn into_snapshot(self, kind: RateLimitKind, window: String) -> Option<RateLimitObservation> {
        if self.limit.is_none() && self.remaining.is_none() && self.reset.is_none() {
            return None;
        }

        Some(RateLimitObservation {
            kind,
            window,
            limit: self.limit,
            remaining: self.remaining,
            reset: self.reset,
        })
    }
}

pub fn parse_anthropic_rate_limit_headers(headers: &HeaderMap) -> Vec<RateLimitObservation> {
    let mut snapshots = BTreeMap::<(u8, String), (RateLimitKind, PartialSnapshot)>::new();

    for (name, value) in headers {
        let Some((kind, field, window)) = parse_header_name(name.as_str()) else {
            continue;
        };
        let Ok(value) = value.to_str() else {
            continue;
        };
        let snapshot = &mut snapshots
            .entry((rate_limit_kind_order(kind), window))
            .or_insert_with(|| (kind, PartialSnapshot::default()))
            .1;
        match field {
            RateLimitField::Limit => {
                if let Some(parsed) = parse_u64(value) {
                    snapshot.limit = Some(parsed);
                }
            }
            RateLimitField::Remaining => {
                if let Some(parsed) = parse_u64(value) {
                    snapshot.remaining = Some(parsed);
                }
            }
            RateLimitField::Reset => {
                if let Some(parsed) = parse_reset(value) {
                    snapshot.reset = Some(parsed);
                }
            }
        }
    }

    snapshots
        .into_iter()
        .filter_map(|((_order, window), (kind, snapshot))| snapshot.into_snapshot(kind, window))
        .collect()
}

fn parse_header_name(name: &str) -> Option<(RateLimitKind, RateLimitField, String)> {
    let suffix = name.strip_prefix(HEADER_PREFIX)?;
    let parts = suffix
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    for (field_index, part) in parts.iter().enumerate() {
        let Some(field) = parse_field(part) else {
            continue;
        };
        let (kind, pre_field_window) = parse_kind_and_pre_field_window(&parts[..field_index])?;
        let post_field_window = &parts[field_index + 1..];
        let window = if post_field_window.is_empty() {
            window_identity(pre_field_window)
        } else {
            window_identity(post_field_window)
        };
        return Some((kind, field, window));
    }
    None
}

fn parse_field(part: &str) -> Option<RateLimitField> {
    match part {
        "limit" => Some(RateLimitField::Limit),
        "remaining" => Some(RateLimitField::Remaining),
        "reset" => Some(RateLimitField::Reset),
        _ => None,
    }
}

fn parse_kind_and_pre_field_window<'a>(
    parts: &'a [&'a str],
) -> Option<(RateLimitKind, &'a [&'a str])> {
    match parts {
        ["requests", window @ ..] | ["request", window @ ..] => {
            Some((RateLimitKind::Requests, window))
        }
        ["tokens", window @ ..] | ["token", window @ ..] => Some((RateLimitKind::Tokens, window)),
        ["input", "tokens", window @ ..] | ["input", "token", window @ ..] => {
            Some((RateLimitKind::InputTokens, window))
        }
        ["output", "tokens", window @ ..] | ["output", "token", window @ ..] => {
            Some((RateLimitKind::OutputTokens, window))
        }
        _ => None,
    }
}

fn rate_limit_kind_order(kind: RateLimitKind) -> u8 {
    match kind {
        RateLimitKind::Requests => 0,
        RateLimitKind::Tokens => 1,
        RateLimitKind::InputTokens => 2,
        RateLimitKind::OutputTokens => 3,
    }
}

fn window_identity(parts: &[&str]) -> String {
    if parts.is_empty() {
        DEFAULT_WINDOW.to_owned()
    } else {
        parts.join("-")
    }
}

fn parse_u64(value: &str) -> Option<u64> {
    value.trim().parse::<u64>().ok()
}

fn parse_reset(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}
