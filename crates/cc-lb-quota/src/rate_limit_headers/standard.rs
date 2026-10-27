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

pub fn parse_openai_rate_limit_headers(headers: &HeaderMap) -> Vec<RateLimitObservation> {
    let mut snapshots = BTreeMap::<(u8, String), (RateLimitKind, PartialSnapshot)>::new();

    for (name, value) in headers {
        let (kind, field) = match name.as_str() {
            "x-ratelimit-limit-requests" => (RateLimitKind::Requests, RateLimitField::Limit),
            "x-ratelimit-remaining-requests" => {
                (RateLimitKind::Requests, RateLimitField::Remaining)
            }
            "x-ratelimit-reset-requests" => (RateLimitKind::Requests, RateLimitField::Reset),
            "x-ratelimit-limit-tokens" => (RateLimitKind::Tokens, RateLimitField::Limit),
            "x-ratelimit-remaining-tokens" => (RateLimitKind::Tokens, RateLimitField::Remaining),
            "x-ratelimit-reset-tokens" => (RateLimitKind::Tokens, RateLimitField::Reset),
            _ => continue,
        };
        let Ok(value) = value.to_str() else {
            continue;
        };
        let snapshot = &mut snapshots
            .entry((rate_limit_kind_order(kind), DEFAULT_WINDOW.to_owned()))
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
                let value = value.trim();
                if !value.is_empty() && !value.eq_ignore_ascii_case("n/a") {
                    snapshot.reset = Some(value.to_owned());
                }
            }
        }
    }

    snapshots
        .into_iter()
        .filter_map(|((_order, window), (kind, snapshot))| snapshot.into_snapshot(kind, window))
        .collect()
}

/// Observe a bounded retry window when no provider-specific limit was parsed.
///
/// The raw delay or HTTP date remains in `reset`, just as provider reset
/// headers do. A retry hint does not imply any request limit or remaining quota.
pub fn parse_retry_after_rate_limit_headers(headers: &HeaderMap) -> Vec<RateLimitObservation> {
    let Some(value) = headers
        .get(http::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.len() <= 256)
    else {
        return Vec::new();
    };
    if !parse_anthropic_rate_limit_headers(headers).is_empty()
        || !parse_openai_rate_limit_headers(headers).is_empty()
    {
        return Vec::new();
    }
    vec![RateLimitObservation {
        kind: RateLimitKind::Requests,
        window: "retry_after".to_owned(),
        limit: None,
        remaining: None,
        reset: Some(value.to_owned()),
    }]
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

#[cfg(test)]
mod tests {
    use super::{parse_openai_rate_limit_headers, parse_retry_after_rate_limit_headers};
    use cc_lb_domain::{RateLimitKind, RateLimitObservation};
    use http::{HeaderMap, HeaderName, HeaderValue};

    #[test]
    fn retry_after_observes_delay_or_http_date_without_inventing_quota() {
        for value in ["30", "Wed, 21 Oct 2026 07:28:00 GMT"] {
            let mut headers = HeaderMap::new();
            headers.insert(http::header::RETRY_AFTER, HeaderValue::from_static(value));
            assert_eq!(
                parse_retry_after_rate_limit_headers(&headers),
                vec![RateLimitObservation {
                    kind: RateLimitKind::Requests,
                    window: "retry_after".to_owned(),
                    limit: None,
                    remaining: None,
                    reset: Some(value.to_owned()),
                }]
            );
        }
    }

    #[test]
    fn retry_after_is_absent_for_missing_empty_invalid_or_oversized_values() {
        assert!(parse_retry_after_rate_limit_headers(&HeaderMap::new()).is_empty());
        for value in [
            HeaderValue::from_static(""),
            HeaderValue::from_static("   "),
            HeaderValue::from_bytes(&[0xff]).expect("opaque header"),
            HeaderValue::from_str(&"1".repeat(257)).expect("oversized header"),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(http::header::RETRY_AFTER, value);
            assert!(parse_retry_after_rate_limit_headers(&headers).is_empty());
        }
    }

    #[test]
    fn retry_after_does_not_duplicate_provider_specific_observations() {
        for (name, value) in [
            ("anthropic-ratelimit-requests-limit", "100"),
            ("anthropic-ratelimit-tokens-reset", "2026-10-21T07:28:00Z"),
            ("x-ratelimit-remaining-requests", "0"),
            ("x-ratelimit-reset-tokens", "30s"),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(http::header::RETRY_AFTER, HeaderValue::from_static("30"));
            headers.insert(
                HeaderName::from_static(name),
                HeaderValue::from_static(value),
            );
            assert!(parse_retry_after_rate_limit_headers(&headers).is_empty());
        }
    }

    #[test]
    fn retry_after_remains_a_fallback_for_unparseable_provider_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::RETRY_AFTER, HeaderValue::from_static("30"));
        headers.insert(
            HeaderName::from_static("x-ratelimit-limit-requests"),
            HeaderValue::from_static("N/A"),
        );
        headers.insert(
            HeaderName::from_static("anthropic-ratelimit-requests-limit"),
            HeaderValue::from_static("invalid"),
        );
        assert_eq!(parse_retry_after_rate_limit_headers(&headers).len(), 1);
    }

    #[test]
    fn parses_openai_request_and_token_rate_limit_headers() {
        let mut headers = HeaderMap::new();
        for (name, value) in [
            ("x-ratelimit-limit-requests", "1000"),
            ("x-ratelimit-remaining-requests", "997"),
            ("x-ratelimit-reset-requests", "2026-05-20T00:00:01Z"),
            ("x-ratelimit-limit-tokens", "100000"),
            ("x-ratelimit-remaining-tokens", "99990"),
            ("x-ratelimit-reset-tokens", "2026-05-20T00:00:02Z"),
        ] {
            headers.insert(
                HeaderName::from_static(name),
                HeaderValue::from_static(value),
            );
        }

        assert_eq!(
            parse_openai_rate_limit_headers(&headers),
            [
                RateLimitObservation {
                    kind: RateLimitKind::Requests,
                    window: "default".to_owned(),
                    limit: Some(1000),
                    remaining: Some(997),
                    reset: Some("2026-05-20T00:00:01Z".to_owned()),
                },
                RateLimitObservation {
                    kind: RateLimitKind::Tokens,
                    window: "default".to_owned(),
                    limit: Some(100000),
                    remaining: Some(99990),
                    reset: Some("2026-05-20T00:00:02Z".to_owned()),
                },
            ]
        );
    }

    #[test]
    fn openai_na_rate_limit_values_remain_absent() {
        let mut headers = HeaderMap::new();
        for (name, value) in [
            ("x-ratelimit-limit-requests", "N/A"),
            ("x-ratelimit-remaining-requests", "N/A"),
            ("x-ratelimit-reset-requests", "N/A"),
            ("x-ratelimit-limit-tokens", "N/A"),
            ("x-ratelimit-remaining-tokens", "N/A"),
            ("x-ratelimit-reset-tokens", "N/A"),
        ] {
            headers.insert(
                HeaderName::from_static(name),
                HeaderValue::from_static(value),
            );
        }

        assert!(parse_openai_rate_limit_headers(&headers).is_empty());
    }
}
