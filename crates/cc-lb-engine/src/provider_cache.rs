//! Provider-native prompt-cache boundaries and evidence.
//!
//! This module deliberately does not produce Anthropic breakpoint objects. It
//! only records facts that are explicit in an OpenAI Responses or Gemini
//! request. Provider-managed cache identities remain opaque and are represented
//! by bounded hashes rather than raw keys or resource names.

use std::time::Instant;

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::provider_capability::ProviderCapability;
use cc_lb_request_log::{RequestCacheEvidence, RequestNativeCacheObservation};

const MAX_EXPLICIT_WRITES: usize = 4;
#[derive(Clone, Copy, PartialEq, Eq)]
enum OpenaiCacheMode {
    Explicit,
    Implicit,
}
const MAX_LOOKBACK_BOUNDARIES: usize = 20;
const MAX_BOUNDARY_BYTES: usize = 512;
const MAX_SCOPE_BYTES: usize = 256;
const OPENAI_LONG_RETENTION_SECS: u64 = 12 * 60 * 60;
const OPENAI_MODERN_DEFAULT_RETENTION_SECS: u64 = 30 * 60;

/// Analyze only the supported provider-native request paths.
///
/// `None` means the provider/model/request form is not known well enough to
/// make a cache claim. This is intentionally fail-closed.
pub(crate) fn analyze(
    capability: ProviderCapability,
    body: &Value,
    model: &str,
    headers: &http::HeaderMap,
) -> Option<RequestNativeCacheObservation> {
    let started = Instant::now();
    let mut observation = match capability {
        ProviderCapability::OpenaiResponses => analyze_openai(body, model, headers)?,
        ProviderCapability::GeminiGenerateContent => analyze_gemini(body, model),
        _ => return None,
    };
    observation.analysis_ms = Some(started.elapsed().as_secs_f64() * 1_000.0);
    Some(observation)
}

fn analyze_openai(
    body: &Value,
    model: &str,
    headers: &http::HeaderMap,
) -> Option<RequestNativeCacheObservation> {
    if model.is_empty() {
        return None;
    }
    let scope = openai_scope(headers);
    let options = body.get("prompt_cache_options");
    if options.is_some_and(|value| !value.is_object()) {
        return None;
    }
    let requested_mode = match options.and_then(|value| value.get("mode")) {
        None => None,
        Some(Value::String(mode)) if mode.eq_ignore_ascii_case("explicit") => {
            Some(OpenaiCacheMode::Explicit)
        }
        Some(Value::String(mode)) if mode.eq_ignore_ascii_case("implicit") => {
            Some(OpenaiCacheMode::Implicit)
        }
        Some(_) => return None,
    };
    let modern = supports_modern_openai_cache(model);
    if !modern && !supports_older_openai_cache(model) {
        return None;
    }
    let breakpoints = find_openai_breakpoints(body);
    if breakpoints.len() > MAX_EXPLICIT_WRITES {
        return Some(unknown_openai(scope, "explicit_write_limit"));
    }
    if !modern && (requested_mode == Some(OpenaiCacheMode::Explicit) || !breakpoints.is_empty()) {
        return Some(unknown_openai(scope, "unsupported_older_explicit"));
    }
    let retention_secs = openai_retention_secs(body, modern, &breakpoints)?;
    let identity_hash = body
        .get("prompt_cache_key")
        .and_then(Value::as_str)
        .filter(|key| !key.is_empty())
        .map(hash_identity);
    let mode = if requested_mode == Some(OpenaiCacheMode::Explicit) || !breakpoints.is_empty() {
        "explicit"
    } else {
        "implicit"
    };
    let boundary = if !breakpoints.is_empty() {
        Some(boundary_label("explicit", breakpoints.len()))
    } else if modern || requested_mode == Some(OpenaiCacheMode::Implicit) {
        implicit_boundary(body)
    } else {
        Some("opaque_provider_boundary".to_owned())
    };
    let evidence_source = if !breakpoints.is_empty() {
        Some("request.prompt_cache_breakpoint".to_owned())
    } else {
        Some("provider_managed_implicit_boundary".to_owned())
    };
    Some(RequestNativeCacheObservation {
        provider: "openai".to_owned(),
        mode: mode.to_owned(),
        scope,
        identity_hash,
        boundary,
        evidence_source,
        cached_tokens: None,
        write_tokens: None,
        evidence: RequestCacheEvidence::Unreported,
        retention_secs,
        expires_at_unix_secs: None,
        analysis_ms: None,
    })
}

fn analyze_gemini(body: &Value, model: &str) -> RequestNativeCacheObservation {
    let explicit_resource = body
        .get("cachedContent")
        .or_else(|| body.pointer("/request/cachedContent"))
        .and_then(Value::as_str)
        .filter(|resource| !resource.is_empty());
    let supported_model = supports_gemini_cache(model);
    let mode = if explicit_resource.is_some() {
        "explicit"
    } else {
        "implicit"
    };
    if !supported_model {
        return RequestNativeCacheObservation {
            provider: "gemini".to_owned(),
            mode: mode.to_owned(),
            evidence: RequestCacheEvidence::Unreported,
            evidence_source: Some("unsupported_model_or_api".to_owned()),
            ..Default::default()
        };
    }
    let (scope, identity_hash) = explicit_resource
        .map(|resource| {
            let project = resource
                .strip_prefix("projects/")
                .and_then(|rest| rest.split('/').next())
                .filter(|project| !project.is_empty())
                .map(|project| truncate(project, MAX_SCOPE_BYTES));
            (project, Some(hash_identity(resource)))
        })
        .unwrap_or((None, None));
    RequestNativeCacheObservation {
        provider: "gemini".to_owned(),
        mode: mode.to_owned(),
        scope,
        identity_hash,
        boundary: if explicit_resource.is_some() {
            None
        } else {
            Some(format!(
                "minimum_input_tokens={}",
                gemini_minimum_tokens(model)
            ))
        },
        evidence_source: Some(if explicit_resource.is_some() {
            "request.cachedContent".to_owned()
        } else {
            "provider_usage_metadata".to_owned()
        }),
        cached_tokens: None,
        write_tokens: None,
        evidence: RequestCacheEvidence::Unreported,
        retention_secs: None,
        expires_at_unix_secs: None,
        analysis_ms: None,
    }
}

fn find_openai_breakpoints(body: &Value) -> Vec<&Value> {
    let mut found = Vec::new();
    if let Some(input) = body.get("input") {
        collect_breakpoints(input, &mut found);
    }
    found
}

fn collect_breakpoints<'a>(value: &'a Value, found: &mut Vec<&'a Value>) {
    match value {
        Value::Object(map) => {
            if map.contains_key("prompt_cache_breakpoint") {
                found.push(value);
            }
            for child in map.values() {
                collect_breakpoints(child, found);
            }
        }
        Value::Array(items) => {
            for child in items {
                collect_breakpoints(child, found);
            }
        }
        _ => {}
    }
}

fn implicit_boundary(body: &Value) -> Option<String> {
    let messages = body.get("input").and_then(Value::as_array)?;
    let eligible = messages
        .iter()
        .filter(|message| {
            message
                .get("role")
                .and_then(Value::as_str)
                .is_some_and(|role| matches!(role, "developer" | "system" | "user" | "assistant"))
        })
        .collect::<Vec<_>>();
    if eligible.is_empty() {
        return None;
    }
    let start = eligible
        .iter()
        .take_while(|message| message.get("role").and_then(Value::as_str) == Some("developer"))
        .count();
    let latest = eligible.len().saturating_sub(1);
    let lookback_start = eligible.len().saturating_sub(MAX_LOOKBACK_BOUNDARIES);
    Some(format!(
        "initial_developer={start};latest_message={latest};earlier_messages={}",
        eligible.len().saturating_sub(lookback_start)
    ))
}

fn openai_retention_secs(
    body: &Value,
    modern: bool,
    breakpoints: &[&Value],
) -> Option<Option<u64>> {
    let retention = match body.get("prompt_cache_retention") {
        None => None,
        Some(Value::String(value)) if value == "in_memory" => Some(0),
        Some(Value::String(value)) if value == "24h" && !modern => Some(24 * 60 * 60),
        Some(_) => return None,
    };
    let options_ttl = match body
        .get("prompt_cache_options")
        .and_then(|options| options.get("ttl"))
    {
        None => None,
        Some(value) => Some(parse_ttl_secs(value)?),
    };
    let mut breakpoint_ttl = None;
    for breakpoint in breakpoints {
        let marker = breakpoint
            .get("prompt_cache_breakpoint")
            .and_then(Value::as_object)?;
        let Some(value) = marker.get("ttl") else {
            continue;
        };
        let parsed = parse_ttl_secs(value)?;
        if breakpoint_ttl.is_some_and(|previous| previous != parsed) {
            return None;
        }
        breakpoint_ttl = Some(parsed);
    }
    let configured = breakpoint_ttl.or(retention).or(options_ttl);
    let metadata_only_breakpoint = body
        .get("metadata")
        .and_then(|metadata| metadata.get("prompt_cache_breakpoint"))
        .is_some();
    let supported_request_form = body
        .get("input")
        .is_some_and(|input| input.is_array() || input.is_string());
    Some(configured.or_else(|| {
        (modern && supported_request_form && !metadata_only_breakpoint)
            .then_some(OPENAI_MODERN_DEFAULT_RETENTION_SECS)
    }))
}

fn parse_ttl_secs(value: &Value) -> Option<u64> {
    if let Some(seconds) = value.as_u64() {
        return (1..=OPENAI_LONG_RETENTION_SECS)
            .contains(&seconds)
            .then_some(seconds);
    }
    let text = value.as_str()?;
    let (number, suffix) = text.split_at(text.len().saturating_sub(1));
    let number = number.parse::<u64>().ok()?;
    match suffix {
        "m" => number.checked_mul(60),
        "h" => number.checked_mul(60 * 60),
        _ => None,
    }
    .filter(|seconds| (1..=OPENAI_LONG_RETENTION_SECS).contains(seconds))
}

pub(crate) fn supports_modern_openai_cache(model: &str) -> bool {
    let lower = model.to_ascii_lowercase();
    let Some(version) = lower.strip_prefix("gpt-").and_then(|value| {
        let version = value
            .split(|ch: char| !ch.is_ascii_digit() && ch != '.')
            .next()?;
        let mut parts = version.split('.');
        let major = parts.next()?.parse::<u64>().ok()?;
        let minor = parts.next().unwrap_or("0").parse::<u64>().ok()?;
        Some((major, minor))
    }) else {
        return false;
    };
    version >= (5, 6)
}

fn supports_gemini_cache(model: &str) -> bool {
    let lower = model.to_ascii_lowercase();
    (lower.contains("gemini-2.5") || lower.contains("gemini-3")) && !lower.contains("argon")
}

fn supports_older_openai_cache(model: &str) -> bool {
    let lower = model.to_ascii_lowercase();
    ["gpt-4o", "gpt-4.1", "gpt-5", "o1", "o3", "o4"]
        .iter()
        .any(|prefix| lower.starts_with(prefix))
}
fn gemini_minimum_tokens(model: &str) -> u64 {
    if model.to_ascii_lowercase().contains("gemini-2.5") {
        2_048
    } else {
        4_096
    }
}

pub(crate) fn normalize_cached_tokens(model: Option<&str>, tokens: u64) -> u64 {
    let Some(model) = model else {
        return tokens;
    };
    if model.starts_with("gpt-") && !supports_modern_openai_cache(model) {
        tokens / 128 * 128
    } else {
        tokens
    }
}

fn openai_scope(headers: &http::HeaderMap) -> Option<String> {
    let organization = headers
        .get("openai-organization")
        .and_then(|value| value.to_str().ok());
    let region = headers
        .get("openai-processing-region")
        .and_then(|value| value.to_str().ok());
    match (organization, region) {
        (None, None) => None,
        (organization, region) => Some(truncate(
            &format!(
                "org={};region={}",
                organization.unwrap_or(""),
                region.unwrap_or("")
            ),
            MAX_SCOPE_BYTES,
        )),
    }
}

fn unknown_openai(scope: Option<String>, boundary: &str) -> RequestNativeCacheObservation {
    RequestNativeCacheObservation {
        provider: "openai".to_owned(),
        mode: "unknown".to_owned(),
        scope,
        boundary: Some(boundary.to_owned()),
        evidence_source: Some("unsupported_request_form".to_owned()),
        evidence: RequestCacheEvidence::Unreported,
        ..Default::default()
    }
}

fn boundary_label(kind: &str, count: usize) -> String {
    truncate(
        &format!("{kind}_writes={count};lookup=first_two_latest_50"),
        MAX_BOUNDARY_BYTES,
    )
}

fn hash_identity(value: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"cc-lb-provider-cache-identity-v1\0");
    digest.update(value.as_bytes());
    let digest = digest.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    format!("sha256:{encoded}")
}

fn truncate(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    value.get(..max_bytes).unwrap_or_default().to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use serde_json::json;

    #[test]
    fn modern_openai_explicit_boundaries_are_bounded_to_four_writes() {
        let body = json!({
            "model": "gpt-5.6",
            "prompt_cache_options": {"mode": "explicit", "ttl": "12h"},
            "input": [
                {"role":"user", "content":[
                    {"type":"input_text", "text":"a", "prompt_cache_breakpoint":{}},
                    {"type":"input_text", "text":"b", "prompt_cache_breakpoint":{}},
                    {"type":"input_text", "text":"c", "prompt_cache_breakpoint":{}},
                    {"type":"input_text", "text":"d", "prompt_cache_breakpoint":{}}
                ]}
            ]
        });
        let observed = analyze(
            ProviderCapability::OpenaiResponses,
            &body,
            "gpt-5.6",
            &HeaderMap::new(),
        )
        .unwrap();
        assert_eq!(observed.mode, "explicit");
        assert_eq!(observed.retention_secs, Some(12 * 60 * 60));
        assert_eq!(
            observed.boundary.as_deref(),
            Some("explicit_writes=4;lookup=first_two_latest_50")
        );
    }
    #[test]
    fn modern_openai_implicit_mode_analyzes_successfully() {
        let body = json!({
            "prompt_cache_options": {"mode": "implicit"},
            "input": [{"role":"user","content":"hello"}]
        });
        let observed = analyze(
            ProviderCapability::OpenaiResponses,
            &body,
            "gpt-5.6",
            &HeaderMap::new(),
        )
        .unwrap();
        assert_eq!(observed.mode, "implicit");
        assert_eq!(
            observed.retention_secs,
            Some(OPENAI_MODERN_DEFAULT_RETENTION_SECS)
        );
        assert_eq!(
            observed.boundary.as_deref(),
            Some("initial_developer=0;latest_message=0;earlier_messages=1")
        );
    }

    #[test]
    fn modern_openai_explicit_mode_uses_implicit_thirty_minute_retention() {
        let observed = analyze(
            ProviderCapability::OpenaiResponses,
            &json!({
                "prompt_cache_options":{"mode":"explicit"},
                "input":[{"prompt_cache_breakpoint":{}}]
            }),
            "gpt-5.6",
            &HeaderMap::new(),
        )
        .unwrap();
        assert_eq!(observed.mode, "explicit");
        assert_eq!(
            observed.retention_secs,
            Some(OPENAI_MODERN_DEFAULT_RETENTION_SECS)
        );
    }

    #[test]
    fn modern_openai_unknown_request_form_has_no_implicit_retention() {
        let observed = analyze(
            ProviderCapability::OpenaiResponses,
            &json!({"prompt_cache_options":{"mode":"implicit"}}),
            "gpt-5.6",
            &HeaderMap::new(),
        )
        .unwrap();
        assert_eq!(observed.retention_secs, None);
    }
    #[test]
    fn unsupported_openai_mode_is_unknown() {
        let body = json!({"prompt_cache_options":{"mode":"unsupported"}});
        assert!(
            analyze(
                ProviderCapability::OpenaiResponses,
                &body,
                "gpt-5.6",
                &HeaderMap::new(),
            )
            .is_none()
        );
    }

    #[test]
    fn invalid_openai_mode_type_is_unknown() {
        assert!(
            analyze(
                ProviderCapability::OpenaiResponses,
                &json!({
                    "prompt_cache_options":{"mode":42},
                    "input":[{"role":"user","content":"hello"}]
                }),
                "gpt-5.6",
                &HeaderMap::new(),
            )
            .is_none()
        );
    }

    #[test]
    fn unsupported_openai_write_count_is_unknown() {
        let body = json!({"model":"gpt-5.6","input":[
            {"prompt_cache_breakpoint":{}},{"prompt_cache_breakpoint":{}},
            {"prompt_cache_breakpoint":{}},{"prompt_cache_breakpoint":{}},
            {"prompt_cache_breakpoint":{}}
        ]});
        let observed = analyze(
            ProviderCapability::OpenaiResponses,
            &body,
            "gpt-5.6",
            &HeaderMap::new(),
        )
        .unwrap();
        assert_eq!(observed.mode, "unknown");
        assert_eq!(observed.boundary.as_deref(), Some("explicit_write_limit"));
    }

    #[test]
    fn openai_metadata_breakpoint_is_not_cache_boundary_or_retention() {
        let observed = analyze(
            ProviderCapability::OpenaiResponses,
            &json!({
                "metadata":{"prompt_cache_breakpoint":{"ttl":"2h"}},
                "input":[{"role":"user","content":"hello"}]
            }),
            "gpt-5.6",
            &HeaderMap::new(),
        )
        .unwrap();
        assert_eq!(observed.mode, "implicit");
        assert_eq!(observed.retention_secs, None);
    }

    #[test]
    fn older_o_models_are_accepted_without_the_gpt_prefix() {
        for model in ["o1", "o3-mini", "o4-mini"] {
            let observed = analyze(
                ProviderCapability::OpenaiResponses,
                &json!({"prompt_cache_key":"opaque"}),
                model,
                &HeaderMap::new(),
            )
            .unwrap();
            assert_eq!(observed.mode, "implicit");
        }
    }

    #[test]
    fn older_explicit_boundaries_are_unknown() {
        let observed = analyze(
            ProviderCapability::OpenaiResponses,
            &json!({
                "prompt_cache_options":{"mode":"explicit"},
                "input":[{"prompt_cache_breakpoint":{}}]
            }),
            "o3",
            &HeaderMap::new(),
        )
        .unwrap();
        assert_eq!(observed.mode, "unknown");
        assert_eq!(
            observed.boundary.as_deref(),
            Some("unsupported_older_explicit")
        );
    }

    #[test]
    fn modern_breakpoint_ttl_overrides_request_ttl() {
        let observed = analyze(
            ProviderCapability::OpenaiResponses,
            &json!({
                "prompt_cache_options":{"ttl":"30m"},
                "input":[{"content":[
                    {"prompt_cache_breakpoint":{"ttl":"2h"}}
                ]}]
            }),
            "gpt-5.9-preview",
            &HeaderMap::new(),
        )
        .unwrap();
        assert_eq!(observed.retention_secs, Some(2 * 60 * 60));
    }

    #[test]
    fn unsupported_openai_ttl_is_unknown() {
        assert!(
            analyze(
                ProviderCapability::OpenaiResponses,
                &json!({"prompt_cache_options":{"ttl":"forever"}}),
                "gpt-5.6",
                &HeaderMap::new(),
            )
            .is_none()
        );
    }

    #[test]
    fn gemini_explicit_resource_has_unknown_retention() {
        let body = json!({"cachedContent":"projects/p1/locations/us/models/gemini-2.5/cache/abc"});
        let observed = analyze(
            ProviderCapability::GeminiGenerateContent,
            &body,
            "gemini-2.5-pro",
            &HeaderMap::new(),
        )
        .unwrap();
        assert_eq!(observed.mode, "explicit");
        assert_eq!(observed.scope.as_deref(), Some("p1"));
        assert_eq!(observed.retention_secs, None);
        assert_eq!(observed.expires_at_unix_secs, None);
        assert!(observed.identity_hash.is_some());
    }
    #[test]
    fn older_openai_key_is_hashed_and_retention_is_preserved() {
        let body = json!({"model":"gpt-4.1","prompt_cache_key":"secret-key","prompt_cache_retention":"24h"});
        let observed = analyze(
            ProviderCapability::OpenaiResponses,
            &body,
            "gpt-4.1",
            &HeaderMap::new(),
        )
        .unwrap();
        assert_eq!(observed.mode, "implicit");
        assert_eq!(observed.retention_secs, Some(24 * 60 * 60));
        assert!(
            observed
                .identity_hash
                .as_deref()
                .unwrap()
                .starts_with("sha256:")
        );
        assert!(
            !serde_json::to_string(&observed)
                .unwrap()
                .contains("secret-key")
        );
    }

    #[test]
    fn gemini_explicit_resource_is_scoped_and_opaque() {
        let body = json!({"cachedContent":"projects/p1/locations/us/models/gemini-2.5/cache/abc"});
        let observed = analyze(
            ProviderCapability::GeminiGenerateContent,
            &body,
            "gemini-2.5-pro",
            &HeaderMap::new(),
        )
        .unwrap();
        assert_eq!(observed.mode, "explicit");
        assert_eq!(observed.scope.as_deref(), Some("p1"));
        assert_eq!(observed.retention_secs, None);
        assert_eq!(observed.expires_at_unix_secs, None);
        assert!(observed.identity_hash.is_some());
    }
}
