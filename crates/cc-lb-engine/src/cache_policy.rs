//! Principal-owned prompt-cache policy applied to an Anthropic Messages body.
//!
//! A principal carries one [`PromptCacheMode`] that says whether its callers'
//! cache controls are honored as sent ([`PromptCacheMode::PreserveClient`]) or
//! promoted to one-hour ephemeral cache lifetimes
//! ([`PromptCacheMode::OneHourNoKeepalive`]).
//!
//! # What the policy does and does not touch
//!
//! Every location the prompt-cache contract already reads a `cache_control`
//! TTL from is rewritten in place: the top-level automatic marker, each tool
//! entry, each `system` content block, and each `messages[].content[]` block.
//! A control is promoted only when it is already a valid `ephemeral` marker;
//! arbitrary nested JSON — tool input schemas, tool arguments, user payloads —
//! is never inspected or rewritten, and a request without cache controls
//! gains none. This is a duration override, not an automatic-caching feature.
//!
//! # Why the bytes and the parsed value move together
//!
//! Cache metadata, routing candidates, cost estimates, and the outbound body
//! all have to agree on one effective request. [`effective_messages_body_bytes`]
//! returns the serialized body for that same mutated [`Value`], so the caller
//! republishes both the parsed document and its bytes. Serialization stays on
//! `serde_json` (per the repository JSON policy): these bytes are the ones
//! hashed for cache-prefix keys and sent upstream.
//!
//! # Authentication
//!
//! The real request path applies the policy through
//! [`apply_authenticated_prompt_cache_policy`], which requires the
//! [`Authenticated`] proof and therefore cannot run before the credential
//! check. The admin route preview has no proof to offer and shares only the
//! pure core.

use bytes::Bytes;
use cc_lb_storage_api::PromptCacheMode;
use serde_json::Value;

use crate::authn_rail::Authenticated;

/// The `ttl` an ephemeral cache control carries when it does not name one.
const FIVE_MINUTE_TTL: &str = "5m";

/// The `ttl` value an ephemeral cache control carries in one-hour mode.
const ONE_HOUR_TTL: &str = "1h";

/// Every `ttl` the Anthropic cache-control contract defines. An absent `ttl`
/// means [`FIVE_MINUTE_TTL`], so a control without one is a valid marker too.
const KNOWN_TTLS: [&str; 2] = [FIVE_MINUTE_TTL, ONE_HOUR_TTL];

/// The one downstream route whose body carries the Anthropic Messages
/// cache-control contract this policy rewrites.
pub const ANTHROPIC_MESSAGES_PATH: &str = "/v1/messages";

/// Whether a request is in the scope this policy may rewrite.
///
/// The proxy registers `/v1/messages` for POST only, so the method is part of
/// the scope and not decoration. Both the authenticated normalization before
/// prompt-cache analysis and the pre-sign re-assertion call this one predicate:
/// a dialect that rebuilds `/v1/messages/count_tokens` or any other proxy route
/// keeps its bytes, and cannot fail an attempt with a policy error.
pub fn is_anthropic_messages_request(method: &http::Method, path: &str) -> bool {
    method == http::Method::POST && path == ANTHROPIC_MESSAGES_PATH
}

/// Why a body could not be normalized under its principal's policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PromptCachePolicyError {
    /// The body is not a JSON document, so no cache control can be located.
    /// The caller must not fall back to the unnormalized bytes.
    #[error("request body is not a JSON document that a cache policy can address")]
    UnparsableBody,
    /// Rewriting produced a document whose bytes could not be produced. The
    /// caller must not publish the document without them.
    #[error("effective request body cannot be encoded")]
    UnencodableBody,
}

/// Serializes the *effective* request body for a principal's cache policy.
///
/// Returns `Ok(None)` when the policy changed nothing, which is the common
/// case: the caller then keeps the original bytes instead of re-encoding an
/// identical document. Callers publish the mutated document *and* the returned
/// bytes together, so an encoding failure is reported instead of being masked
/// into "nothing changed": the alternative would leave the caller analyzing a
/// one-hour document whose bytes still say five minutes.
///
/// [`PromptCachePolicyError::UnencodableBody`] is unreachable for a document a
/// JSON parser produced. `serde_json` maps a non-finite number to `null` while
/// parsing and `Number::from_f64` refuses one, every other `Value` shape encodes
/// verbatim, and writing to a `Vec` cannot fail. The signature keeps the
/// invariant checked rather than assumed.
pub fn effective_messages_body_bytes(
    mode: PromptCacheMode,
    value: &mut Value,
) -> Result<Option<Bytes>, PromptCachePolicyError> {
    if !mode.promotes_cache_control_ttl_to_one_hour() {
        return Ok(None);
    }
    if !promote_ephemeral_cache_controls(value) {
        return Ok(None);
    }
    serde_json::to_vec(value)
        .map(|bytes| Some(Bytes::from(bytes)))
        .map_err(|_| PromptCachePolicyError::UnencodableBody)
}

/// Applies the principal's cache policy on an authenticated request.
///
/// The [`Authenticated`] parameter is the ordering proof this module exists to
/// keep: policy rewriting is request work, so it can only happen on a request
/// whose credentials already checked out.
pub fn apply_authenticated_prompt_cache_policy(
    _proof: &Authenticated,
    mode: PromptCacheMode,
    value: &mut Value,
) -> Result<Option<Bytes>, PromptCachePolicyError> {
    effective_messages_body_bytes(mode, value)
}

/// Rewrites an outbound body that a dialect produced, so a shaped request can
/// never silently send a shorter TTL than its principal asked for.
///
/// The caller scopes this to [`is_anthropic_messages_request`]; the function
/// itself only speaks the Anthropic Messages body contract.
///
/// A dialect that passed the effective body through unchanged yields `None`
/// without re-encoding; only a dialect that itself reintroduced an ephemeral
/// control pays for parsing and reserializing.
pub fn normalize_outbound_messages_body(
    mode: PromptCacheMode,
    body: &Bytes,
) -> Result<Option<Bytes>, PromptCachePolicyError> {
    if !mode.promotes_cache_control_ttl_to_one_hour() {
        return Ok(None);
    }
    let mut value =
        sonic_rs::from_slice::<Value>(body).map_err(|_| PromptCachePolicyError::UnparsableBody)?;
    effective_messages_body_bytes(mode, &mut value)
}

/// Promotes every recognized ephemeral `cache_control` marker to `ttl: "1h"`.
///
/// Returns `true` when at least one marker changed.
fn promote_ephemeral_cache_controls(value: &mut Value) -> bool {
    let Some(root) = value.as_object_mut() else {
        return false;
    };
    let mut changed = promote_control(root.get_mut("cache_control"));

    if let Some(Value::Array(tools)) = root.get_mut("tools") {
        for tool in tools.iter_mut() {
            if let Some(tool) = tool.as_object_mut() {
                changed |= promote_control(tool.get_mut("cache_control"));
            }
        }
    }
    if let Some(system) = root.get_mut("system") {
        changed |= promote_content_blocks(system);
    }
    if let Some(Value::Array(messages)) = root.get_mut("messages") {
        for message in messages.iter_mut() {
            if let Some(message) = message.as_object_mut()
                && let Some(content) = message.get_mut("content")
            {
                changed |= promote_content_blocks(content);
            }
        }
    }
    changed
}

/// Promotes the controls of one `system` value or one `content` value: either
/// a bare string with nothing to rewrite, or an array of content blocks.
fn promote_content_blocks(value: &mut Value) -> bool {
    let Value::Array(blocks) = value else {
        return false;
    };
    let mut changed = false;
    for block in blocks.iter_mut() {
        if let Some(block) = block.as_object_mut() {
            changed |= promote_control(block.get_mut("cache_control"));
        }
    }
    changed
}

/// Promotes one `cache_control` marker. Anything the provider contract would
/// reject is left to the existing validation contract instead of being repaired:
/// this is a duration override for markers the contract already defines, not a
/// validator that turns an invalid request into a valid one.
fn promote_control(control: Option<&mut Value>) -> bool {
    let Some(control) = control.and_then(Value::as_object_mut) else {
        return false;
    };
    if control.get("type").and_then(Value::as_str) != Some("ephemeral") {
        return false;
    }
    if let Some(ttl) = control.get("ttl") {
        // An unknown or non-string `ttl` is invalid input: leave it exactly as
        // the client sent it for the upstream contract to reject.
        if !ttl.as_str().is_some_and(|ttl| KNOWN_TTLS.contains(&ttl)) {
            return false;
        }
        if ttl.as_str() == Some(ONE_HOUR_TTL) {
            return false;
        }
    }
    control.insert("ttl".to_owned(), Value::String(ONE_HOUR_TTL.to_owned()));
    true
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn promoted(mut value: Value) -> Value {
        assert!(promote_ephemeral_cache_controls(&mut value));
        value
    }

    #[test]
    fn promotes_every_recognized_location() {
        let value = promoted(json!({
            "model": "claude-sonnet-4-5",
            "cache_control": {"type": "ephemeral"},
            "tools": [{"name": "lookup", "cache_control": {"type": "ephemeral"}}],
            "system": [
                {"type": "text", "text": "system", "cache_control": {"type": "ephemeral", "ttl": "5m"}},
                {"type": "text", "text": "second"}
            ],
            "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": "hello", "cache_control": {"type": "ephemeral"}},
                    {"type": "tool_result", "cache_control": {"type": "ephemeral", "ttl": "1h"}}
                ]}
            ]
        }));

        assert_eq!(value["cache_control"]["ttl"], json!("1h"));
        assert_eq!(value["tools"][0]["cache_control"]["ttl"], json!("1h"));
        assert_eq!(value["system"][0]["cache_control"]["ttl"], json!("1h"));
        assert_eq!(
            value["messages"][0]["content"][0]["cache_control"]["ttl"],
            json!("1h")
        );
        assert_eq!(
            value["messages"][0]["content"][1]["cache_control"]["ttl"],
            json!("1h")
        );
        assert_eq!(value["system"][1].get("cache_control"), None);
    }

    #[test]
    fn leaves_nested_tool_data_and_user_json_untouched() {
        let original = json!({
            "model": "claude-sonnet-4-5",
            "tools": [{
                "name": "run",
                "input_schema": {
                    "type": "object",
                    "properties": {"payload": {"cache_control": {"type": "ephemeral"}}}
                }
            }],
            "messages": [{"role": "user", "content": [
                {"type": "tool_result", "content": "[{\"cache_control\":{\"type\":\"ephemeral\"}}]"}
            ]}]
        });
        let mut value = original.clone();

        assert!(!promote_ephemeral_cache_controls(&mut value));
        assert_eq!(value, original);
    }

    #[test]
    fn ignores_controls_that_are_not_valid_ephemeral_markers() {
        let mut value = json!({
            "model": "claude-sonnet-4-5",
            "cache_control": {"type": "persistent"},
            "tools": [{"name": "lookup", "cache_control": "ephemeral"}],
            "system": [{"type": "text", "text": "s", "cache_control": {"ttl": "5m"}}],
            "messages": [{"role": "user", "content": "plain string"}]
        });

        assert!(!promote_ephemeral_cache_controls(&mut value));
        assert_eq!(value["cache_control"], json!({"type": "persistent"}));
        assert_eq!(value["tools"][0]["cache_control"], json!("ephemeral"));
        assert_eq!(value["system"][0]["cache_control"], json!({"ttl": "5m"}));
    }

    #[test]
    fn leaves_a_ttl_the_provider_contract_does_not_define_untouched() {
        // The contract defines an absent ttl (five minutes), "5m", and "1h".
        // Anything else is invalid input: promoting it would repair a request
        // the upstream is supposed to reject, and would change what the client
        // asked for rather than only how long the cache lives.
        let mut value = json!({
            "model": "claude-sonnet-4-5",
            "cache_control": {"type": "ephemeral", "ttl": "2h"},
            "system": [{"type": "text", "text": "s", "cache_control": {"type": "ephemeral", "ttl": 300}}],
            "messages": [{
                "role": "user",
                "content": [{"type": "text", "text": "hi", "cache_control": {"type": "ephemeral"}}]
            }]
        });
        let original = value.clone();

        assert!(
            promote_ephemeral_cache_controls(&mut value),
            "the valid message control is still promoted"
        );
        assert_eq!(value["cache_control"], original["cache_control"]);
        assert_eq!(value["system"][0], original["system"][0]);
        assert_eq!(
            value["messages"][0]["content"][0]["cache_control"]["ttl"],
            json!("1h")
        );
    }

    #[test]
    fn requests_without_controls_gain_none() {
        let mut value = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}]
        });
        let original = value.clone();

        assert!(!promote_ephemeral_cache_controls(&mut value));
        assert_eq!(value, original);
        assert_eq!(
            effective_messages_body_bytes(PromptCacheMode::OneHourNoKeepalive, &mut value).unwrap(),
            None
        );
    }

    #[test]
    fn preserve_client_mode_returns_no_effective_bytes() {
        let mut value = json!({"cache_control": {"type": "ephemeral"}});

        assert_eq!(
            effective_messages_body_bytes(PromptCacheMode::PreserveClient, &mut value).unwrap(),
            None
        );
        assert_eq!(value["cache_control"], json!({"type": "ephemeral"}));
    }

    #[test]
    fn effective_bytes_match_the_mutated_value() {
        let mut value = json!({
            "model": "claude-sonnet-4-5",
            "system": [{"type": "text", "text": "system", "cache_control": {"type": "ephemeral"}}]
        });

        let bytes = effective_messages_body_bytes(PromptCacheMode::OneHourNoKeepalive, &mut value)
            .unwrap()
            .expect("policy rewrites a control");

        let reparsed: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(reparsed, value);
        assert_eq!(reparsed["system"][0]["cache_control"]["ttl"], json!("1h"));
    }

    #[test]
    fn outbound_normalization_repairs_a_dialect_that_reintroduced_five_minutes() {
        let shaped = Bytes::from_static(
            br#"{"system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral","ttl":"5m"}}]}"#,
        );

        let normalized =
            normalize_outbound_messages_body(PromptCacheMode::OneHourNoKeepalive, &shaped)
                .expect("shaped body parses")
                .expect("policy rewrites the dialect's control");

        let value: Value = serde_json::from_slice(&normalized).unwrap();
        assert_eq!(value["system"][0]["cache_control"]["ttl"], json!("1h"));
    }

    #[test]
    fn outbound_normalization_skips_preserve_client_and_non_json_bodies() {
        let shaped = Bytes::from_static(br#"{"cache_control":{"type":"ephemeral"}}"#);

        assert_eq!(
            normalize_outbound_messages_body(PromptCacheMode::PreserveClient, &shaped),
            Ok(None)
        );
        assert_eq!(
            normalize_outbound_messages_body(
                PromptCacheMode::OneHourNoKeepalive,
                &Bytes::from_static(b"not json")
            ),
            Err(PromptCachePolicyError::UnparsableBody)
        );
    }

    #[test]
    fn scope_is_exactly_the_post_messages_route() {
        assert!(is_anthropic_messages_request(
            &http::Method::POST,
            "/v1/messages"
        ));
        assert!(!is_anthropic_messages_request(
            &http::Method::POST,
            "/v1/messages/count_tokens"
        ));
        assert!(!is_anthropic_messages_request(
            &http::Method::GET,
            "/v1/messages"
        ));
        assert!(!is_anthropic_messages_request(
            &http::Method::POST,
            "/v1/models"
        ));
    }
}
