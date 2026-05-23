use std::collections::BTreeMap;

use cc_lb_plugin_api::Principal;
use http::HeaderMap;
use serde_json::Value;

const HEADER_PREFIX: &str = "anthropic-ratelimit-";
const DEFAULT_WINDOW: &str = "default";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) enum AnthropicRateLimitKind {
    Requests,
    Tokens,
    InputTokens,
    OutputTokens,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AnthropicRateLimitSnapshot {
    pub kind: AnthropicRateLimitKind,
    pub window: String,
    pub limit: Option<u64>,
    pub remaining: Option<u64>,
    pub reset: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LimitIdentity {
    Account(String),
    Credential(String),
    Unobserved,
}

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
    fn into_snapshot(
        self,
        kind: AnthropicRateLimitKind,
        window: String,
    ) -> Option<AnthropicRateLimitSnapshot> {
        if self.limit.is_none() && self.remaining.is_none() && self.reset.is_none() {
            return None;
        }

        Some(AnthropicRateLimitSnapshot {
            kind,
            window,
            limit: self.limit,
            remaining: self.remaining,
            reset: self.reset,
        })
    }
}

pub(crate) fn parse_anthropic_rate_limit_headers(
    headers: &HeaderMap,
) -> Vec<AnthropicRateLimitSnapshot> {
    let mut snapshots = BTreeMap::<(AnthropicRateLimitKind, String), PartialSnapshot>::new();

    for (name, value) in headers {
        let Some((kind, field, window)) = parse_header_name(name.as_str()) else {
            continue;
        };
        let Ok(value) = value.to_str() else {
            continue;
        };
        let snapshot = snapshots.entry((kind, window)).or_default();
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
        .filter_map(|((kind, window), snapshot)| snapshot.into_snapshot(kind, window))
        .collect()
}

pub(crate) fn derive_limit_identity(principal: &Principal, headers: &HeaderMap) -> LimitIdentity {
    if let Some(account) = header_string(headers, "anthropic-organization-id") {
        return LimitIdentity::Account(account);
    }

    for key in [
        "anthropic_account_id",
        "account_id",
        "account_identity",
        "organization_id",
        "org_id",
    ] {
        if let Some(account) = claim_string(&principal.claims, key) {
            return LimitIdentity::Account(account);
        }
    }

    for key in [
        "credentials_ref",
        "credential_ref",
        "real_credential_storage_key",
        "oauth_credential_id",
        "oauth_provider",
    ] {
        if let Some(credential) = claim_string(&principal.claims, key) {
            return LimitIdentity::Credential(credential);
        }
    }

    LimitIdentity::Unobserved
}

fn parse_header_name(name: &str) -> Option<(AnthropicRateLimitKind, RateLimitField, String)> {
    let suffix = name.strip_prefix(HEADER_PREFIX)?;
    let parts = suffix
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    for (field_index, part) in parts.iter().enumerate() {
        let Some(field) = parse_field(part) else {
            continue;
        };
        let before = &parts[..field_index];
        let after = &parts[field_index + 1..];
        let (kind, pre_field_window) = parse_kind_and_pre_field_window(before)?;
        let window = if after.is_empty() {
            window_identity(pre_field_window)
        } else {
            window_identity(after)
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
) -> Option<(AnthropicRateLimitKind, &'a [&'a str])> {
    match parts {
        ["requests", window @ ..] | ["request", window @ ..] => {
            Some((AnthropicRateLimitKind::Requests, window))
        }
        ["tokens", window @ ..] | ["token", window @ ..] => {
            Some((AnthropicRateLimitKind::Tokens, window))
        }
        ["input", "tokens", window @ ..] | ["input", "token", window @ ..] => {
            Some((AnthropicRateLimitKind::InputTokens, window))
        }
        ["output", "tokens", window @ ..] | ["output", "token", window @ ..] => {
            Some((AnthropicRateLimitKind::OutputTokens, window))
        }
        _ => None,
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

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn claim_string(claims: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
    claims
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use cc_lb_plugin_api::{Principal, PrincipalKind};
    use http::header::{HeaderName, HeaderValue};
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_default_request_and_token_families() {
        let headers = headers(&[
            ("anthropic-ratelimit-requests-limit", "1000"),
            ("anthropic-ratelimit-requests-remaining", "997"),
            ("anthropic-ratelimit-requests-reset", "2026-05-20T00:00:01Z"),
            ("anthropic-ratelimit-tokens-limit", "100000"),
            ("anthropic-ratelimit-tokens-remaining", "99990"),
            ("anthropic-ratelimit-tokens-reset", "2026-05-20T00:00:02Z"),
        ]);

        let snapshots = parse_anthropic_rate_limit_headers(&headers);

        assert_eq!(snapshots.len(), 2);
        assert_eq!(
            snapshots[0],
            AnthropicRateLimitSnapshot {
                kind: AnthropicRateLimitKind::Requests,
                window: "default".to_owned(),
                limit: Some(1000),
                remaining: Some(997),
                reset: Some("2026-05-20T00:00:01Z".to_owned()),
            }
        );
        assert_eq!(
            snapshots[1],
            AnthropicRateLimitSnapshot {
                kind: AnthropicRateLimitKind::Tokens,
                window: "default".to_owned(),
                limit: Some(100000),
                remaining: Some(99990),
                reset: Some("2026-05-20T00:00:02Z".to_owned()),
            }
        );
    }

    #[test]
    fn preserves_post_field_window_identity_for_5h_and_weekly() {
        let headers = headers(&[
            ("anthropic-ratelimit-requests-limit-5h", "5000"),
            ("anthropic-ratelimit-requests-remaining-5h", "4999"),
            (
                "anthropic-ratelimit-tokens-reset-weekly",
                "2026-05-27T00:00:00Z",
            ),
        ]);

        let snapshots = parse_anthropic_rate_limit_headers(&headers);

        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].kind, AnthropicRateLimitKind::Requests);
        assert_eq!(snapshots[0].window, "5h");
        assert_eq!(snapshots[0].limit, Some(5000));
        assert_eq!(snapshots[0].remaining, Some(4999));
        assert_eq!(snapshots[1].kind, AnthropicRateLimitKind::Tokens);
        assert_eq!(snapshots[1].window, "weekly");
        assert_eq!(snapshots[1].reset.as_deref(), Some("2026-05-27T00:00:00Z"));
    }

    #[test]
    fn preserves_pre_field_window_identity() {
        let headers = headers(&[
            ("anthropic-ratelimit-requests-5h-limit", "5000"),
            ("anthropic-ratelimit-tokens-weekly-remaining", "42"),
        ]);

        let snapshots = parse_anthropic_rate_limit_headers(&headers);

        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].window, "5h");
        assert_eq!(snapshots[0].limit, Some(5000));
        assert_eq!(snapshots[1].window, "weekly");
        assert_eq!(snapshots[1].remaining, Some(42));
    }

    #[test]
    fn parses_input_and_output_token_families() {
        let headers = headers(&[
            ("anthropic-ratelimit-input-tokens-limit", "25000"),
            ("anthropic-ratelimit-input-tokens-remaining", "24000"),
            ("anthropic-ratelimit-output-tokens-limit-weekly", "75000"),
            (
                "anthropic-ratelimit-output-tokens-reset-weekly",
                "2026-05-28T00:00:00Z",
            ),
        ]);

        let snapshots = parse_anthropic_rate_limit_headers(&headers);

        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].kind, AnthropicRateLimitKind::InputTokens);
        assert_eq!(snapshots[0].limit, Some(25000));
        assert_eq!(snapshots[0].remaining, Some(24000));
        assert_eq!(snapshots[1].kind, AnthropicRateLimitKind::OutputTokens);
        assert_eq!(snapshots[1].window, "weekly");
        assert_eq!(snapshots[1].limit, Some(75000));
    }

    #[test]
    fn missing_and_malformed_headers_are_ignored() {
        let headers = headers(&[
            ("anthropic-ratelimit-requests-limit", "not-a-number"),
            ("anthropic-ratelimit-tokens-remaining", ""),
            ("anthropic-ratelimit-unknown-limit", "9"),
            ("x-ratelimit-requests-limit", "1"),
        ]);

        let snapshots = parse_anthropic_rate_limit_headers(&headers);

        assert!(snapshots.is_empty());
        assert!(parse_anthropic_rate_limit_headers(&HeaderMap::new()).is_empty());
    }

    #[test]
    fn derives_account_identity_from_response_header_before_claims() {
        let headers = headers(&[("anthropic-organization-id", "org_header")]);
        let principal = principal_with_claims(&[
            ("account_id", json!("org_claim")),
            ("credentials_ref", json!("credential_claim")),
        ]);

        let identity = derive_limit_identity(&principal, &headers);

        assert_eq!(identity, LimitIdentity::Account("org_header".to_owned()));
    }

    #[test]
    fn derives_credential_identity_when_account_is_unobserved() {
        let principal = principal_with_claims(&[("credentials_ref", json!("credential-a"))]);

        let identity = derive_limit_identity(&principal, &HeaderMap::new());

        assert_eq!(
            identity,
            LimitIdentity::Credential("credential-a".to_owned())
        );
    }

    #[test]
    fn marks_identity_unobserved_when_no_account_or_credential_is_known() {
        let principal = principal_with_claims(&[]);

        let identity = derive_limit_identity(&principal, &HeaderMap::new());

        assert_eq!(identity, LimitIdentity::Unobserved);
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(
                HeaderName::from_bytes(name.as_bytes()).expect("test header name parses"),
                HeaderValue::from_str(value).expect("test header value parses"),
            );
        }
        headers
    }

    fn principal_with_claims(pairs: &[(&str, Value)]) -> Principal {
        let mut claims = serde_json::Map::new();
        for (key, value) in pairs {
            claims.insert((*key).to_owned(), value.clone());
        }
        Principal {
            id: "principal-test".to_owned(),
            kind: PrincipalKind::ApiKey,
            claims,
        }
    }
}
