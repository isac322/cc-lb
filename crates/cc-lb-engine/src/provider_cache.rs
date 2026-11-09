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

const MAX_BOUNDARY_BYTES: usize = 512;
const MAX_SCOPE_BYTES: usize = 256;

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
    analyze_with_openai_interpretation(capability, body, model, headers, None)
}

pub(crate) fn analyze_with_openai_interpretation(
    capability: ProviderCapability,
    body: &Value,
    model: &str,
    headers: &http::HeaderMap,
    modern_interpretation: Option<Option<&crate::cache_keepalive::openai::Interpretation>>,
) -> Option<RequestNativeCacheObservation> {
    let started = Instant::now();
    let mut observation = match capability {
        ProviderCapability::OpenaiResponses => {
            analyze_openai(body, model, headers, modern_interpretation)?
        }
        ProviderCapability::GeminiGenerateContent => analyze_gemini(body, model),
        _ => return None,
    };
    observation.analysis_ms = Some(started.elapsed().as_secs_f64() * 1_000.0);
    Some(observation)
}

/// Project provider-native usage without borrowing Anthropic creation counters.
/// Unsupported request forms and malformed writes cannot establish cache state.
pub(crate) fn project_usage(
    native: &RequestNativeCacheObservation,
    read_evidence: RequestCacheEvidence,
    cached_tokens: u64,
    write_evidence: RequestCacheEvidence,
    write_tokens: Option<u64>,
) -> (RequestCacheEvidence, Option<u64>, Option<u64>) {
    if native.mode == "unknown"
        || native.evidence_source.as_deref() == Some("unsupported_model_or_api")
    {
        return (RequestCacheEvidence::Unreported, None, None);
    }
    if write_evidence == RequestCacheEvidence::Invalid {
        return (RequestCacheEvidence::Invalid, None, None);
    }
    let cached_tokens = match read_evidence {
        RequestCacheEvidence::Zero | RequestCacheEvidence::Positive => Some(cached_tokens),
        RequestCacheEvidence::Unreported | RequestCacheEvidence::Invalid => None,
    };
    (read_evidence, cached_tokens, write_tokens)
}

fn analyze_openai(
    body: &Value,
    model: &str,
    headers: &http::HeaderMap,
    modern_interpretation: Option<Option<&crate::cache_keepalive::openai::Interpretation>>,
) -> Option<RequestNativeCacheObservation> {
    if model.is_empty() {
        return None;
    }
    let options = match body.get("prompt_cache_options") {
        None => None,
        Some(Value::Object(options)) => Some(options),
        Some(_) => return None,
    };
    let requested_mode = match options.and_then(|options| options.get("mode")) {
        None => false,
        Some(Value::String(mode)) if mode.eq_ignore_ascii_case("implicit") => false,
        Some(Value::String(mode)) if mode.eq_ignore_ascii_case("explicit") => true,
        Some(_) => return None,
    };
    let scope = openai_scope(headers);
    let modern = supports_modern_openai_cache(model);
    if !modern && !supports_older_openai_cache(model) {
        return None;
    }
    if modern {
        let parsed = if modern_interpretation.is_none() {
            Some(crate::cache_keepalive::openai::interpret_modern(body)?)
        } else {
            None
        };
        let interpretation = match modern_interpretation {
            Some(Some(interpretation)) => interpretation,
            Some(None) => return None,
            None => parsed.as_ref()?,
        };
        if interpretation.explicit_count > 4 {
            return Some(unknown_openai(scope, "explicit_write_limit"));
        }
        let mode = match interpretation.mode {
            crate::cache_keepalive::openai::Mode::Explicit => "explicit",
            crate::cache_keepalive::openai::Mode::Implicit => "implicit",
        };
        let retention_secs = interpretation.lifetime.map(|lifetime| match lifetime {
            crate::cache_keepalive::openai::Lifetime::ThirtyMinutes => 30 * 60,
            crate::cache_keepalive::openai::Lifetime::TwelveHours => 12 * 60 * 60,
        });
        let boundary = interpretation
            .selected_boundary
            .as_deref()
            .map(|path| boundary_label(mode, interpretation.explicit_count, path, retention_secs));
        let identity_hash = body
            .get("prompt_cache_key")
            .and_then(Value::as_str)
            .filter(|key| !key.is_empty())
            .map(hash_identity);
        return Some(RequestNativeCacheObservation {
            provider: "openai".to_owned(),
            mode: mode.to_owned(),
            scope,
            identity_hash,
            boundary,
            evidence_source: Some(if interpretation.explicit_count > 0 {
                "request.prompt_cache_breakpoint".to_owned()
            } else {
                "provider_managed_implicit_boundary".to_owned()
            }),
            cached_tokens: None,
            write_tokens: None,
            evidence: RequestCacheEvidence::Unreported,
            retention_secs,
            expires_at_unix_secs: None,
            analysis_ms: None,
        });
    }
    let count = crate::cache_keepalive::openai::explicit_breakpoint_count(body)?;
    if requested_mode || count > 0 {
        return Some(unknown_openai(scope, "unsupported_older_explicit"));
    }
    let retention_secs = crate::cache_keepalive::openai::legacy_retention_secs(body)?;
    Some(RequestNativeCacheObservation {
        provider: "openai".to_owned(),
        mode: "implicit".to_owned(),
        scope,
        identity_hash: body
            .get("prompt_cache_key")
            .and_then(Value::as_str)
            .filter(|key| !key.is_empty())
            .map(hash_identity),
        boundary: Some("opaque_provider_boundary".to_owned()),
        evidence_source: Some("provider_managed_implicit_boundary".to_owned()),
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

pub(crate) fn supports_modern_openai_cache(model: &str) -> bool {
    crate::cache_keepalive::openai::is_gpt56_or_newer(model)
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

fn boundary_label(kind: &str, count: usize, path: &str, retention_secs: Option<u64>) -> String {
    truncate(
        &format!(
            "{kind}_writes={count};selected={path};ttl={}",
            retention_secs.map_or_else(|| "unknown".to_owned(), |seconds| seconds.to_string())
        ),
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
            Some("explicit_writes=4;selected=input[0].content[3];ttl=43200")
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
        assert_eq!(observed.retention_secs, Some(30 * 60));
        assert_eq!(
            observed.boundary.as_deref(),
            Some("implicit_writes=0;selected=input[0];ttl=1800")
        );
    }

    #[test]
    fn modern_openai_explicit_mode_without_marker_is_ineligible() {
        let observed = analyze(
            ProviderCapability::OpenaiResponses,
            &json!({
                "prompt_cache_options":{"mode":"explicit"},
                "input":[{"role":"user","content":"hello"}]
            }),
            "gpt-5.6",
            &HeaderMap::new(),
        )
        .unwrap();
        assert_eq!(observed.mode, "explicit");
        assert_eq!(observed.retention_secs, None);
    }

    #[test]
    fn modern_openai_unknown_request_form_is_not_observed() {
        assert!(
            analyze(
                ProviderCapability::OpenaiResponses,
                &json!({"prompt_cache_options":{"mode":"implicit"}}),
                "gpt-5.6",
                &HeaderMap::new(),
            )
            .is_none()
        );
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
        let body = json!({"model":"gpt-5.6","input":[{"role":"user","content":[
            {"type":"input_text","prompt_cache_breakpoint":{}},
            {"type":"input_text","prompt_cache_breakpoint":{}},
            {"type":"input_text","prompt_cache_breakpoint":{}},
            {"type":"input_text","prompt_cache_breakpoint":{}},
            {"type":"input_text","prompt_cache_breakpoint":{}}
        ]}]});
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
        assert_eq!(observed.retention_secs, Some(30 * 60));
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
                "input":[{"role":"user","content":[
                    {"type":"input_text","prompt_cache_breakpoint":{}}
                ]}]
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
    fn unsupported_modern_breakpoint_ttl_fails_closed() {
        assert!(
            analyze(
                ProviderCapability::OpenaiResponses,
                &json!({
                    "prompt_cache_options":{"ttl":"30m"},
                    "input":[{"role":"user","content":[
                        {"type":"input_text","prompt_cache_breakpoint":{"ttl":"2h"}}
                    ]}]
                }),
                "gpt-5.9-preview",
                &HeaderMap::new(),
            )
            .is_none()
        );
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
