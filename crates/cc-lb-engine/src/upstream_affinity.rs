use std::collections::HashSet;
use std::ops::ControlFlow;

use crate::provider_capability::{ProviderCapability, safe_openai_resource_path};
use crate::usage_decoder::decode_full_body;
use crate::usage_parser::ParsedSseEvent;
use cc_lb_storage_api::{UpstreamAffinityKey, UpstreamAffinityKind};
use http::{HeaderMap, StatusCode};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

const ANTHROPIC_PROVIDER: &str = "anthropic";
const OPENAI_PROVIDER: &str = "openai";
const GEMINI_PROVIDER: &str = "gemini";
const WEB_SEARCH_TOOL_RESULT: &str = "web_search_tool_result";
const WEB_SEARCH_TOOL_PREFIX: &str = "web_search_";
const CODE_EXECUTION_TOOL_PREFIX: &str = "code_execution_";
const CODE_EXECUTION_OUTPUT_SUFFIX: &str = "code_execution_output";
const ANTHROPIC_FILE_SOURCE: &str = "file";
const ANTHROPIC_CONTAINER_UPLOAD: &str = "container_upload";
const OPENAI_RESPONSE_EVENT_PREFIX: &str = "response.";

pub(crate) const MAX_UPSTREAM_AFFINITY_KEYS_PER_REQUEST: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UpstreamAffinityExtractionError {
    TooManyKeys,
    InvalidSseData,
    InvalidResponseAffinity,
}

/// Response references can be connection-local; reusable reasoning never is.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum OpenaiResponseReferenceScope {
    Durable,
    ConnectionLocal,
}

/// Provider label stored with every affinity key derived for `capability`.
pub(crate) const fn affinity_provider(capability: ProviderCapability) -> &'static str {
    match capability.provider() {
        cc_lb_domain::Provider::Anthropic => ANTHROPIC_PROVIDER,
        cc_lb_domain::Provider::Openai => OPENAI_PROVIDER,
        cc_lb_domain::Provider::Gemini => GEMINI_PROVIDER,
    }
}

/// Returns whether a successful response to this request can create
/// provider-owned state that later requests must be pinned to.
pub(crate) fn request_observes_response_affinity(
    capability: ProviderCapability,
    value: &Value,
) -> bool {
    match capability {
        ProviderCapability::AnthropicMessages => {
            value
                .get("container")
                .is_some_and(|container| !container.is_null())
                || value
                    .get("tools")
                    .and_then(Value::as_array)
                    .is_some_and(|tools| {
                        tools.iter().any(|tool| {
                            tool.get("type")
                                .and_then(Value::as_str)
                                .is_some_and(|tool_type| {
                                    tool_type.starts_with(WEB_SEARCH_TOOL_PREFIX)
                                        || tool_type.starts_with(CODE_EXECUTION_TOOL_PREFIX)
                                })
                        })
                    })
        }
        // Every Responses call yields a response id that a later request may
        // chain through `previous_response_id`. Codex-style stateless chains
        // send `store: false` and still pin the follow-up to the owning
        // account (encrypted reasoning items are account-scoped), so the id
        // is observed regardless of `store`.
        ProviderCapability::OpenaiResponses | ProviderCapability::OpenaiResponsesCompact => true,
        ProviderCapability::GeminiCachedContentsCreate => true,
        _ => false,
    }
}

/// Extracts the provider-owned state a request references. Every returned key
/// must resolve to one upstream before the request may be routed.
pub(crate) fn extract_request_affinity_keys(
    capability: ProviderCapability,
    value: &Value,
    principal_id: &str,
) -> Result<Vec<UpstreamAffinityKey>, UpstreamAffinityExtractionError> {
    extract_request_affinity_keys_with_scope(
        capability,
        value,
        principal_id,
        OpenaiResponseReferenceScope::Durable,
    )
}

pub(crate) fn extract_request_affinity_keys_with_scope(
    capability: ProviderCapability,
    value: &Value,
    principal_id: &str,
    response_scope: OpenaiResponseReferenceScope,
) -> Result<Vec<UpstreamAffinityKey>, UpstreamAffinityExtractionError> {
    let mut collector = KeyCollector::new(capability, principal_id);
    collector.response_scope = response_scope;
    match capability {
        ProviderCapability::AnthropicMessages | ProviderCapability::AnthropicCountTokens => {
            collector.anthropic_message(value)?
        }
        ProviderCapability::OpenaiResponses
        | ProviderCapability::OpenaiResponsesInputTokens
        | ProviderCapability::OpenaiResponsesCompact => {
            if let Some(object) = value.as_object() {
                collector.openai_response_reference(object.get("previous_response_id"))?;
                collector.reference(
                    UpstreamAffinityKind::OpenaiConversationId,
                    object.get("conversation"),
                )?;
                collector.openai_reasoning_tree(object.get("input"))?;
            }
        }
        ProviderCapability::GeminiGenerateContent | ProviderCapability::GeminiCountTokens => {
            extract_gemini_cached_content(value, &mut collector)?;
        }
        _ => {}
    }
    Ok(collector.keys)
}
/// Build the affinity key for a realtime call `Location` header.
pub(crate) fn realtime_call_affinity_key(
    principal_id: &str,
    location: &str,
) -> Option<UpstreamAffinityKey> {
    let path = location.split('?').next().unwrap_or(location);
    let call_id = path
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .filter(|id| !id.is_empty())?;
    let value_sha256: [u8; 32] = Sha256::digest(call_id.as_bytes()).into();
    Some(UpstreamAffinityKey {
        principal_id: principal_id.to_owned(),
        provider: OPENAI_PROVIDER.to_owned(),
        kind: UpstreamAffinityKind::OpenaiRealtimeCall,
        value_sha256,
    })
}

/// Build the affinity key a resource path names: the realtime call a hangup
/// targets, or the response a stored-response operation reads. Returns `None`
/// for every other operation or for a malformed id segment.
pub(crate) fn path_affinity_key(
    capability: ProviderCapability,
    path: &str,
    principal_id: &str,
) -> Option<UpstreamAffinityKey> {
    let (kind, id) = match capability {
        ProviderCapability::OpenaiRealtimeHangup => (
            UpstreamAffinityKind::OpenaiRealtimeCall,
            path.strip_prefix("/v1/realtime/calls/")?
                .strip_suffix("/hangup")?,
        ),
        ProviderCapability::OpenaiStoredResponse => (
            UpstreamAffinityKind::OpenaiResponseId,
            path.strip_prefix("/v1/responses/")?.split('/').next()?,
        ),
        _ => return None,
    };
    if !safe_openai_resource_path(path) || id.is_empty() || id.contains('/') {
        return None;
    }
    Some(UpstreamAffinityKey {
        principal_id: principal_id.to_owned(),
        provider: OPENAI_PROVIDER.to_owned(),
        kind,
        value_sha256: Sha256::digest(id.as_bytes()).into(),
    })
}

/// Extracts the provider-owned state a buffered successful response creates.
pub(crate) fn extract_response_affinity_keys(
    capability: ProviderCapability,
    value: &Value,
    principal_id: &str,
) -> Result<Vec<UpstreamAffinityKey>, UpstreamAffinityExtractionError> {
    let mut collector = KeyCollector::new(capability, principal_id);
    match capability {
        ProviderCapability::AnthropicMessages => collector.anthropic_message(value)?,
        ProviderCapability::OpenaiResponses | ProviderCapability::OpenaiResponsesCompact => {
            collector.openai_response(value)?;
        }
        ProviderCapability::GeminiCachedContentsCreate => {
            let name = value
                .get("name")
                .and_then(Value::as_str)
                .filter(|name| {
                    name.strip_prefix("cachedContents/")
                        .is_some_and(|id| !id.is_empty() && !id.contains('/'))
                })
                .ok_or(UpstreamAffinityExtractionError::InvalidResponseAffinity)?;
            collector.push(UpstreamAffinityKind::GeminiCachedContent, name)?;
        }
        _ => {}
    }
    Ok(collector.keys)
}

pub(crate) fn extract_websocket_response_affinity_keys(
    response: &Value,
    output_item: Option<&Value>,
    principal_id: &str,
    response_scope: OpenaiResponseReferenceScope,
) -> Result<Vec<UpstreamAffinityKey>, UpstreamAffinityExtractionError> {
    let mut collector = KeyCollector::new(ProviderCapability::OpenaiResponses, principal_id);
    collector.response_scope = response_scope;
    collector.openai_response(response)?;
    collector.openai_reasoning_tree(output_item)?;
    Ok(collector.keys)
}

/// CachedContent output timestamps are RFC 3339, with optional fractional seconds.
pub(crate) fn gemini_cached_content_expiry(
    value: &Value,
) -> Result<u64, UpstreamAffinityExtractionError> {
    value
        .get("expireTime")
        .and_then(Value::as_str)
        .and_then(|timestamp| chrono::DateTime::parse_from_rfc3339(timestamp).ok())
        .and_then(|timestamp| u64::try_from(timestamp.timestamp()).ok())
        .ok_or(UpstreamAffinityExtractionError::InvalidResponseAffinity)
}

/// Extracts the provider-owned state a single successful SSE event creates.
pub(crate) fn extract_response_affinity_keys_from_sse_event(
    capability: ProviderCapability,
    event: &ParsedSseEvent<'_>,
    principal_id: &str,
) -> Result<Vec<UpstreamAffinityKey>, UpstreamAffinityExtractionError> {
    if !matches!(
        capability,
        ProviderCapability::AnthropicMessages
            | ProviderCapability::OpenaiResponses
            | ProviderCapability::OpenaiResponsesCompact
    ) {
        return Ok(Vec::new());
    }
    let Some(value) = event
        .value()
        .map_err(|_| UpstreamAffinityExtractionError::InvalidSseData)?
    else {
        return Ok(Vec::new());
    };
    let mut collector = KeyCollector::new(capability, principal_id);
    match (capability, event.event_type()) {
        (ProviderCapability::AnthropicMessages, Some("content_block_start")) => {
            collector.anthropic_tree(value, false)?;
        }
        (ProviderCapability::AnthropicMessages, Some("message_start")) => {
            if let Some(message) = value.get("message") {
                collector.anthropic_message(message)?;
            }
        }
        (ProviderCapability::AnthropicMessages, Some("message_delta")) => {
            collector.reference(
                UpstreamAffinityKind::AnthropicContainerId,
                value.get("delta").and_then(|delta| delta.get("container")),
            )?;
        }
        (
            ProviderCapability::OpenaiResponses | ProviderCapability::OpenaiResponsesCompact,
            Some(event_type),
        ) if event_type.starts_with(OPENAI_RESPONSE_EVENT_PREFIX) => {
            if let Some(response) = value.get("response") {
                collector.openai_response(response)?;
            }
            if event_type == "response.output_item.done" {
                collector.openai_reasoning_tree(value.get("item"))?;
            }
        }
        _ => {}
    }
    Ok(collector.keys)
}

/// Deduplicates SHA-256 digests per kind and enforces the per-request key cap.
struct KeyCollector<'a> {
    provider: &'static str,
    principal_id: &'a str,
    seen: HashSet<(UpstreamAffinityKind, [u8; 32])>,
    keys: Vec<UpstreamAffinityKey>,
    response_scope: OpenaiResponseReferenceScope,
}

fn extract_gemini_cached_content(
    value: &Value,
    collector: &mut KeyCollector<'_>,
) -> Result<(), UpstreamAffinityExtractionError> {
    collector.reference(
        UpstreamAffinityKind::GeminiCachedContent,
        value.get("cachedContent"),
    )?;
    collector.reference(
        UpstreamAffinityKind::GeminiCachedContent,
        value
            .get("generateContentRequest")
            .and_then(|request| request.get("cachedContent")),
    )
}

impl<'a> KeyCollector<'a> {
    fn new(capability: ProviderCapability, principal_id: &'a str) -> Self {
        Self {
            provider: affinity_provider(capability),
            principal_id,
            seen: HashSet::new(),
            keys: Vec::new(),
            response_scope: OpenaiResponseReferenceScope::Durable,
        }
    }

    fn push(
        &mut self,
        kind: UpstreamAffinityKind,
        value: &str,
    ) -> Result<(), UpstreamAffinityExtractionError> {
        if value.is_empty() {
            return Ok(());
        }
        let digest: [u8; 32] = Sha256::digest(value.as_bytes()).into();
        if self.seen.insert((kind, digest)) {
            if self.keys.len() == MAX_UPSTREAM_AFFINITY_KEYS_PER_REQUEST {
                return Err(UpstreamAffinityExtractionError::TooManyKeys);
            }
            self.keys.push(UpstreamAffinityKey {
                principal_id: self.principal_id.to_owned(),
                provider: self.provider.to_owned(),
                kind,
                value_sha256: digest,
            });
        }
        Ok(())
    }

    /// Accepts a state reference given either as an id string or as `{"id": ...}`.
    fn reference(
        &mut self,
        kind: UpstreamAffinityKind,
        value: Option<&Value>,
    ) -> Result<(), UpstreamAffinityExtractionError> {
        match reference_id(value) {
            Some(id) => self.push(kind, id),
            None => Ok(()),
        }
    }

    fn openai_response_reference(
        &mut self,
        value: Option<&Value>,
    ) -> Result<(), UpstreamAffinityExtractionError> {
        if self.response_scope == OpenaiResponseReferenceScope::ConnectionLocal {
            return Ok(());
        }
        self.reference(UpstreamAffinityKind::OpenaiResponseId, value)
    }

    fn openai_response(&mut self, response: &Value) -> Result<(), UpstreamAffinityExtractionError> {
        let Some(object) = response.as_object() else {
            return Ok(());
        };
        self.openai_response_reference(object.get("id"))?;
        self.reference(
            UpstreamAffinityKind::OpenaiConversationId,
            object.get("conversation"),
        )?;
        self.openai_reasoning_tree(object.get("output"))
    }

    fn openai_reasoning_tree(
        &mut self,
        value: Option<&Value>,
    ) -> Result<(), UpstreamAffinityExtractionError> {
        let Some(value) = value else {
            return Ok(());
        };
        match value {
            Value::Array(items) => {
                for item in items {
                    self.openai_reasoning_tree(Some(item))?;
                }
            }
            Value::Object(object) => {
                if matches!(
                    object.get("type").and_then(Value::as_str),
                    Some("reasoning" | "compaction" | "compaction_summary")
                ) {
                    self.reference(
                        UpstreamAffinityKind::OpenaiResponseId,
                        object.get("encrypted_content"),
                    )?;
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
        Ok(())
    }

    fn anthropic_message(
        &mut self,
        message: &Value,
    ) -> Result<(), UpstreamAffinityExtractionError> {
        self.reference(
            UpstreamAffinityKind::AnthropicContainerId,
            message.get("container"),
        )?;
        self.anthropic_tree(message, false)
    }

    fn anthropic_tree(
        &mut self,
        value: &Value,
        web_search_content_item: bool,
    ) -> Result<(), UpstreamAffinityExtractionError> {
        match value {
            Value::Array(items) => {
                for item in items {
                    if matches!(item, Value::Array(_) | Value::Object(_)) {
                        self.anthropic_tree(item, false)?;
                    }
                }
            }
            Value::Object(object) => {
                if web_search_content_item
                    && let Some(encrypted_content) =
                        object.get("encrypted_content").and_then(Value::as_str)
                {
                    self.push(
                        UpstreamAffinityKind::AnthropicWebSearchEncryptedContent,
                        encrypted_content,
                    )?;
                }
                let block_type = object.get("type").and_then(Value::as_str);
                if let Some(file_id) = anthropic_block_file_id(block_type, object) {
                    self.push(UpstreamAffinityKind::AnthropicFileId, file_id)?;
                }

                let has_web_search_content = block_type == Some(WEB_SEARCH_TOOL_RESULT);
                for (name, child) in object {
                    if has_web_search_content
                        && name == "content"
                        && let Some(items) = child.as_array()
                    {
                        for item in items {
                            self.anthropic_tree(item, true)?;
                        }
                    } else if matches!(child, Value::Array(_) | Value::Object(_)) {
                        self.anthropic_tree(child, false)?;
                    }
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
        Ok(())
    }
}

/// Files API references: `{"type": "file", "file_id"}` content sources,
/// `container_upload` blocks, and code-execution output files.
fn anthropic_block_file_id<'v>(
    block_type: Option<&str>,
    object: &'v Map<String, Value>,
) -> Option<&'v str> {
    let block_type = block_type?;
    let references_file = block_type == ANTHROPIC_FILE_SOURCE
        || block_type == ANTHROPIC_CONTAINER_UPLOAD
        || block_type.ends_with(CODE_EXECUTION_OUTPUT_SUFFIX);
    if !references_file {
        return None;
    }
    object.get("file_id").and_then(Value::as_str)
}

fn reference_id(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(|value| {
            value
                .as_str()
                .or_else(|| value.get("id").and_then(Value::as_str))
        })
        .filter(|id| !id.is_empty())
}

const fn is_openai_responses_affinity_capability(capability: ProviderCapability) -> bool {
    matches!(
        capability,
        ProviderCapability::OpenaiResponses
            | ProviderCapability::OpenaiResponsesInputTokens
            | ProviderCapability::OpenaiResponsesCompact
    )
}

fn walk_openai_reasoning_items<'a, B>(
    value: &'a Value,
    visitor: &mut impl FnMut(&'a str, &'a str) -> ControlFlow<B>,
) -> ControlFlow<B> {
    match value {
        Value::Array(items) => {
            for item in items {
                walk_openai_reasoning_items(item, visitor)?;
            }
        }
        Value::Object(object) => {
            if let Some(item_type) = object.get("type").and_then(Value::as_str)
                && matches!(item_type, "reasoning" | "compaction" | "compaction_summary")
                && let Some(encrypted_content) = reference_id(object.get("encrypted_content"))
            {
                visitor(item_type, encrypted_content)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    ControlFlow::Continue(())
}

/// Returns whether the OpenAI Responses request's ONLY opaque affinity state
/// comes from `reasoning.encrypted_content` items in an `input` array (with no
/// `previous_response_id`, `conversation`, `compaction`, or `compaction_summary`).
pub(crate) fn is_openai_unknown_reasoning_only(
    capability: ProviderCapability,
    value: &Value,
) -> bool {
    if !is_openai_responses_affinity_capability(capability) {
        return false;
    }
    let Some(object) = value.as_object() else {
        return false;
    };
    if reference_id(object.get("previous_response_id")).is_some()
        || reference_id(object.get("conversation")).is_some()
    {
        return false;
    }
    let Some(input @ Value::Array(_)) = object.get("input") else {
        return false;
    };
    let mut found_reasoning = false;
    let flow = walk_openai_reasoning_items(input, &mut |item_type, _| {
        if matches!(item_type, "compaction" | "compaction_summary") {
            return ControlFlow::Break(());
        }
        if item_type == "reasoning" {
            found_reasoning = true;
        }
        ControlFlow::Continue(())
    });
    flow.is_continue() && found_reasoning
}

/// Returns SHA-256 digests for `reasoning.encrypted_content` items present in
/// `value` whose OpenAI affinity key remained unresolved (`pending_keys`).
pub(crate) fn collect_openai_pending_reasoning_digests(
    capability: ProviderCapability,
    value: &Value,
    pending_keys: &[UpstreamAffinityKey],
) -> HashSet<[u8; 32]> {
    let mut matched = HashSet::new();
    if !is_openai_responses_affinity_capability(capability) || pending_keys.is_empty() {
        return matched;
    }
    if reference_id(value.get("previous_response_id")).is_some()
        || reference_id(value.get("conversation")).is_some()
    {
        return matched;
    }
    let Some(input) = value.get("input").filter(|v| v.is_array()) else {
        return matched;
    };
    let pending_digests: HashSet<[u8; 32]> = pending_keys
        .iter()
        .filter(|key| {
            key.provider == OPENAI_PROVIDER && key.kind == UpstreamAffinityKind::OpenaiResponseId
        })
        .map(|key| key.value_sha256)
        .collect();
    if pending_digests.is_empty() {
        return matched;
    }
    let _ = walk_openai_reasoning_items(input, &mut |item_type, encrypted_content| {
        if item_type == "reasoning" {
            let digest: [u8; 32] = Sha256::digest(encrypted_content.as_bytes()).into();
            if pending_digests.contains(&digest) {
                matched.insert(digest);
            }
        }
        ControlFlow::<()>::Continue(())
    });
    matched
}

fn strip_reasoning_array_tree(
    value: &mut Value,
    only_digests: Option<&HashSet<[u8; 32]>>,
    stripped: &mut bool,
) {
    let Value::Array(items) = value else {
        return;
    };
    items.retain_mut(|item| match item {
        Value::Object(object) => {
            let is_target_reasoning = object.get("type").and_then(Value::as_str)
                == Some("reasoning")
                && reference_id(object.get("encrypted_content")).is_some_and(|enc| {
                    only_digests.is_none_or(|digests| {
                        let digest: [u8; 32] = Sha256::digest(enc.as_bytes()).into();
                        digests.contains(&digest)
                    })
                });
            if is_target_reasoning {
                *stripped = true;
                false
            } else {
                true
            }
        }
        Value::Array(_) => {
            strip_reasoning_array_tree(item, only_digests, stripped);
            true
        }
        _ => true,
    });
}

/// Removes `reasoning` items carrying `encrypted_content` from `input` arrays.
/// When `only_digests` is `Some`, only items whose ciphertext SHA-256 digest is
/// in `only_digests` are removed, keeping already-owned reasoning intact.
pub(crate) fn strip_openai_reasoning_encrypted_items(
    value: &Value,
    only_digests: Option<&HashSet<[u8; 32]>>,
) -> Option<Value> {
    let input = value.get("input")?;
    if !input.is_array() {
        return None;
    }
    let mut cloned = value.clone();
    let mut stripped = false;
    if let Some(input_mut) = cloned.as_object_mut().and_then(|obj| obj.get_mut("input")) {
        strip_reasoning_array_tree(input_mut, only_digests, &mut stripped);
    }
    stripped.then_some(cloned)
}

/// Returns whether an OpenAI JSON error payload reports unverifiable `encrypted_content`.
pub(crate) fn is_openai_invalid_encrypted_content_error_value(value: &Value) -> bool {
    let err = value
        .get("error")
        .or_else(|| value.pointer("/response/error"))
        .unwrap_or(value);
    match err.get("code").and_then(Value::as_str) {
        Some("invalid_encrypted_content") => return true,
        Some(code) if !code.is_empty() => return false,
        _ => {}
    }
    let Some(message) = err
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| value.get("detail").and_then(Value::as_str))
    else {
        return false;
    };
    let lower = message.to_ascii_lowercase();
    (lower.contains("encrypted content") || lower.contains("encrypted_content"))
        && [
            "could not be verified",
            "cannot be verified",
            "failed to verify",
            "could not be decrypted",
            "cannot be decrypted",
            "failed to decrypt",
            "unable to decrypt",
            "encrypted content organization_id did not match",
        ]
        .iter()
        .any(|needle| lower.contains(needle))
}

/// Returns whether an HTTP 400 error body (plain or content-encoded) reports
/// unverifiable `encrypted_content`. Hot-path parsing uses `sonic_rs::from_slice`.
pub(crate) fn is_openai_invalid_encrypted_content_http_error(
    status: StatusCode,
    headers: &HeaderMap,
    body: &[u8],
) -> bool {
    if status != StatusCode::BAD_REQUEST || body.is_empty() {
        return false;
    }
    let output_budget = body.len().saturating_mul(8).clamp(64 * 1024, 512 * 1024);
    let Ok(Some(plaintext)) = decode_full_body(headers, body, output_budget) else {
        return false;
    };
    let Ok(value) = sonic_rs::from_slice::<Value>(plaintext.as_ref()) else {
        return false;
    };
    is_openai_invalid_encrypted_content_error_value(&value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_enables_anthropic_web_search(value: &Value) -> bool {
        request_observes_response_affinity(ProviderCapability::AnthropicMessages, value)
    }

    fn extract_anthropic_web_search_affinity_keys(
        value: &Value,
        principal_id: &str,
    ) -> Result<Vec<UpstreamAffinityKey>, UpstreamAffinityExtractionError> {
        extract_request_affinity_keys(ProviderCapability::AnthropicMessages, value, principal_id)
    }

    fn extract_anthropic_web_search_affinity_keys_from_sse_event(
        event: &ParsedSseEvent<'_>,
        principal_id: &str,
    ) -> Result<Vec<UpstreamAffinityKey>, UpstreamAffinityExtractionError> {
        extract_response_affinity_keys_from_sse_event(
            ProviderCapability::AnthropicMessages,
            event,
            principal_id,
        )
    }

    #[test]
    fn stored_response_path_operations_share_response_affinity_key() {
        let principal = "principal";
        let paths = [
            "/v1/responses/resp_1",
            "/v1/responses/resp_1/input_items",
            "/v1/responses/resp_1/cancel",
        ];
        let keys = paths
            .iter()
            .map(|path| {
                path_affinity_key(ProviderCapability::OpenaiStoredResponse, path, principal)
                    .expect("valid stored response path")
            })
            .collect::<Vec<_>>();

        assert_eq!(keys[0], keys[1]);
        assert_eq!(keys[0], keys[2]);
        assert_eq!(
            path_affinity_key(
                ProviderCapability::OpenaiStoredResponse,
                "/v1/responses/../x",
                principal,
            ),
            None
        );
    }

    #[test]
    fn extracts_nested_web_search_ciphertexts_and_deduplicates_by_digest() {
        let value = serde_json::json!({
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "web_search_tool_result",
                    "content": [
                        {"type": "web_search_result", "encrypted_content": "opaque-a"},
                        {"type": "web_search_result", "encrypted_content": "opaque-a"},
                        {"type": "web_search_result", "encrypted_content": "opaque-b"}
                    ]
                }]
            }],
            "unrelated": {"encrypted_content": "not-a-web-search-result"}
        });

        let keys =
            extract_anthropic_web_search_affinity_keys(&value, "principal-a").expect("extracts");

        assert_eq!(keys.len(), 2);
        assert!(keys.iter().all(|key| key.principal_id == "principal-a"));
        assert!(keys.iter().all(|key| key.provider == "anthropic"));
        assert!(
            keys.iter().all(|key| {
                key.kind == UpstreamAffinityKind::AnthropicWebSearchEncryptedContent
            })
        );
        let expected_a: [u8; 32] = Sha256::digest(b"opaque-a").into();
        let expected_b: [u8; 32] = Sha256::digest(b"opaque-b").into();
        assert_eq!(
            keys.iter()
                .map(|key| key.value_sha256)
                .collect::<HashSet<_>>(),
            HashSet::from([expected_a, expected_b])
        );
    }

    #[test]
    fn extracts_nested_tool_result_without_revisiting_scalar_leaves() {
        let value = serde_json::json!({
            "type": "web_search_tool_result",
            "content": [{
                "type": "wrapper",
                "metadata": "ignored",
                "nested": {
                    "type": "web_search_tool_result",
                    "content": [{
                        "type": "web_search_result",
                        "encrypted_content": "opaque-nested",
                    }],
                },
            }],
        });

        let keys = extract_anthropic_web_search_affinity_keys(&value, "principal-nested")
            .expect("extracts");

        assert_eq!(keys.len(), 1);
        assert_eq!(
            keys[0].value_sha256,
            <[u8; 32]>::from(Sha256::digest(b"opaque-nested"))
        );
    }

    #[test]
    fn compact_and_input_token_operations_reuse_response_owner_affinity() {
        let body =
            serde_json::json!({"previous_response_id":"resp-owner", "conversation":"conv-owner"});
        let expected =
            extract_request_affinity_keys(ProviderCapability::OpenaiResponses, &body, "principal")
                .expect("response affinity");
        for capability in [
            ProviderCapability::OpenaiResponsesCompact,
            ProviderCapability::OpenaiResponsesInputTokens,
        ] {
            let actual = extract_request_affinity_keys(capability, &body, "principal")
                .expect("operation affinity");
            assert_eq!(actual, expected);
        }
        let response = serde_json::json!({"id":"resp-compact"});
        let keys = extract_response_affinity_keys(
            ProviderCapability::OpenaiResponsesCompact,
            &response,
            "principal",
        )
        .expect("compact response affinity");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].kind, UpstreamAffinityKind::OpenaiResponseId);
        assert!(request_observes_response_affinity(
            ProviderCapability::OpenaiResponsesCompact,
            &body
        ));
        assert!(!request_observes_response_affinity(
            ProviderCapability::OpenaiResponsesInputTokens,
            &body
        ));
    }

    #[test]
    fn extracts_only_content_block_start_sse_payloads() {
        let raw = b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"web_search_tool_result\",\"content\":[{\"encrypted_content\":\"opaque-sse\"}]}}\n\n";
        let ignored = b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"content_block\":{\"type\":\"web_search_tool_result\",\"content\":[{\"encrypted_content\":\"opaque-delta\"}]}}\n\n";

        let event = crate::usage_parser::parse_sse_event(raw);
        let keys =
            extract_anthropic_web_search_affinity_keys_from_sse_event(&event, "principal-sse")
                .expect("extracts");

        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].principal_id, "principal-sse");
        assert_eq!(
            keys[0].value_sha256,
            <[u8; 32]>::from(Sha256::digest(b"opaque-sse"))
        );
        let ignored_event = crate::usage_parser::parse_sse_event(ignored);
        assert!(
            extract_anthropic_web_search_affinity_keys_from_sse_event(
                &ignored_event,
                "principal-sse",
            )
            .expect("extracts")
            .is_empty()
        );
    }

    #[test]
    fn joins_multiline_sse_data_before_parsing_json() {
        let raw = b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\ndata: \"content_block\":{\"type\":\"web_search_tool_result\",\ndata: \"content\":[{\"encrypted_content\":\"opaque-multiline\"}]}}\n\n";

        let event = crate::usage_parser::parse_sse_event(raw);
        let keys =
            extract_anthropic_web_search_affinity_keys_from_sse_event(&event, "principal-sse")
                .expect("multiline data parses");

        assert_eq!(keys.len(), 1);
        assert_eq!(
            keys[0].value_sha256,
            <[u8; 32]>::from(Sha256::digest(b"opaque-multiline"))
        );
    }

    #[test]
    fn parses_cr_only_sse_events() {
        let raw = b"event: content_block_start\rdata: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"web_search_tool_result\",\"content\":[{\"encrypted_content\":\"opaque-cr\"}]}}\r\r";

        let event = crate::usage_parser::parse_sse_event(raw);
        let keys =
            extract_anthropic_web_search_affinity_keys_from_sse_event(&event, "principal-sse")
                .expect("CR-only event parses");

        assert_eq!(keys.len(), 1);
        assert_eq!(
            keys[0].value_sha256,
            <[u8; 32]>::from(Sha256::digest(b"opaque-cr"))
        );
    }

    #[test]
    fn stops_traversal_when_unique_key_limit_is_exceeded() {
        let content = (0..=MAX_UPSTREAM_AFFINITY_KEYS_PER_REQUEST)
            .map(|index| serde_json::json!({"encrypted_content": format!("opaque-{index}")}))
            .collect::<Vec<_>>();
        let value = serde_json::json!({
            "type": "web_search_tool_result",
            "content": content,
        });

        assert_eq!(
            extract_anthropic_web_search_affinity_keys(&value, "principal-limit"),
            Err(UpstreamAffinityExtractionError::TooManyKeys)
        );
    }
    #[test]
    fn detects_native_web_search_tool_versions_only() {
        assert!(request_enables_anthropic_web_search(&serde_json::json!({
            "tools": [{"type": "web_search_20250305", "name": "web_search"}]
        })));
        assert!(!request_enables_anthropic_web_search(&serde_json::json!({
            "tools": [{"name": "web_search"}]
        })));
    }

    fn digest(value: &str) -> [u8; 32] {
        Sha256::digest(value.as_bytes()).into()
    }

    fn kinds_and_digests(
        keys: &[UpstreamAffinityKey],
    ) -> HashSet<(UpstreamAffinityKind, [u8; 32])> {
        keys.iter()
            .map(|key| (key.kind, key.value_sha256))
            .collect()
    }

    #[test]
    fn extracts_openai_previous_response_and_conversation_references() {
        let keys = extract_request_affinity_keys(
            ProviderCapability::OpenaiResponses,
            &serde_json::json!({
                "previous_response_id": "resp_1",
                "conversation": {"id": "conv_1"},
                "store": true,
                "input": [
                    {"type": "input_text", "text": "resp_ignored", "encrypted_content": "not-state"},
                    {
                        "type": "reasoning",
                        "id": "rs_1",
                        "summary": [],
                        "encrypted_content": "opaque-reasoning-1"
                    }
                ]
            }),
            "principal-openai",
        )
        .expect("extracts");

        assert!(keys.iter().all(|key| key.provider == "openai"));
        assert_eq!(
            kinds_and_digests(&keys),
            HashSet::from([
                (UpstreamAffinityKind::OpenaiResponseId, digest("resp_1")),
                (UpstreamAffinityKind::OpenaiConversationId, digest("conv_1")),
                (
                    UpstreamAffinityKind::OpenaiResponseId,
                    digest("opaque-reasoning-1")
                ),
            ])
        );
        assert!(
            extract_request_affinity_keys(
                ProviderCapability::OpenaiResponses,
                &serde_json::json!({"store": true, "previous_response_id": null}),
                "principal-openai",
            )
            .expect("extracts")
            .is_empty()
        );
    }

    #[test]
    fn binds_stored_openai_responses_from_body_and_sse_snapshots() {
        let stored = extract_response_affinity_keys(
            ProviderCapability::OpenaiResponses,
            &serde_json::json!({
                "id": "resp_2",
                "object": "response",
                "conversation": {"id": "conv_2"},
                "output": [{
                    "type": "reasoning",
                    "id": "rs_2",
                    "summary": [],
                    "encrypted_content": "opaque-reasoning-2"
                }]
            }),
            "principal-openai",
        )
        .expect("extracts");
        assert_eq!(
            kinds_and_digests(&stored),
            HashSet::from([
                (UpstreamAffinityKind::OpenaiResponseId, digest("resp_2")),
                (UpstreamAffinityKind::OpenaiConversationId, digest("conv_2")),
                (
                    UpstreamAffinityKind::OpenaiResponseId,
                    digest("opaque-reasoning-2")
                ),
            ])
        );
        let unstored = extract_response_affinity_keys(
            ProviderCapability::OpenaiResponses,
            &serde_json::json!({"id": "resp_3", "object": "response", "store": false}),
            "principal-openai",
        )
        .expect("extracts");
        assert_eq!(
            kinds_and_digests(&unstored),
            HashSet::from([(UpstreamAffinityKind::OpenaiResponseId, digest("resp_3"))])
        );
        assert!(request_observes_response_affinity(
            ProviderCapability::OpenaiResponses,
            &serde_json::json!({"store": false, "previous_response_id": "resp_2"}),
        ));
        assert!(!request_observes_response_affinity(
            ProviderCapability::GeminiGenerateContent,
            &serde_json::json!({"contents": []}),
        ));

        let created = crate::usage_parser::parse_sse_event(
            b"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_sse\",\"object\":\"response\"}}\n\n",
        );
        let keys = extract_response_affinity_keys_from_sse_event(
            ProviderCapability::OpenaiResponses,
            &created,
            "principal-openai",
        )
        .expect("extracts");
        assert_eq!(
            kinds_and_digests(&keys),
            HashSet::from([(UpstreamAffinityKind::OpenaiResponseId, digest("resp_sse"))])
        );
        let reasoning = crate::usage_parser::parse_sse_event(
            b"event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"reasoning\",\"encrypted_content\":\"opaque-sse-reasoning\"}}\n\n",
        );
        let keys = extract_response_affinity_keys_from_sse_event(
            ProviderCapability::OpenaiResponses,
            &reasoning,
            "principal-openai",
        )
        .expect("extracts");
        assert_eq!(
            kinds_and_digests(&keys),
            HashSet::from([(
                UpstreamAffinityKind::OpenaiResponseId,
                digest("opaque-sse-reasoning")
            )])
        );
        let delta = crate::usage_parser::parse_sse_event(
            b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"delta\":\"hi\"}\n\n",
        );
        assert!(
            extract_response_affinity_keys_from_sse_event(
                ProviderCapability::OpenaiResponses,
                &delta,
                "principal-openai",
            )
            .expect("extracts")
            .is_empty()
        );
    }

    #[test]
    fn extracts_top_level_and_nested_gemini_cached_content() {
        let keys = extract_request_affinity_keys(
            ProviderCapability::GeminiGenerateContent,
            &serde_json::json!({
                "cachedContent": "cachedContents/abc",
                "generateContentRequest": {"cachedContent": "cachedContents/def"},
                "contents": [{"parts": [{"text": "hi", "thoughtSignature": "sig"}]}]
            }),
            "principal-gemini",
        )
        .expect("extracts");
        assert!(keys.iter().all(|key| key.provider == "gemini"));
        assert_eq!(
            kinds_and_digests(&keys),
            HashSet::from([
                (
                    UpstreamAffinityKind::GeminiCachedContent,
                    digest("cachedContents/abc")
                ),
                (
                    UpstreamAffinityKind::GeminiCachedContent,
                    digest("cachedContents/def")
                ),
            ])
        );
        assert!(!request_observes_response_affinity(
            ProviderCapability::GeminiGenerateContent,
            &serde_json::json!({"cachedContent": "cachedContents/abc"})
        ));
        assert!(
            extract_response_affinity_keys(
                ProviderCapability::GeminiGenerateContent,
                &serde_json::json!({"candidates": [{"content": {"parts": [{"thoughtSignature": "sig"}]}}]}),
                "principal-gemini",
            )
            .expect("extracts")
            .is_empty()
        );
    }
    #[test]
    fn qa_aff_1_vertex_cached_content_digest_uses_client_visible_name() {
        let keys = extract_response_affinity_keys(
            ProviderCapability::GeminiCachedContentsCreate,
            &serde_json::json!({
                "name": "cachedContents/vertex-id",
                "model": "models/gemini-2.5-flash",
                "expireTime": "2026-12-01T00:00:00Z"
            }),
            "principal-vertex",
        )
        .expect("extracts client-visible cache name");
        assert_eq!(
            kinds_and_digests(&keys),
            HashSet::from([(
                UpstreamAffinityKind::GeminiCachedContent,
                digest("cachedContents/vertex-id")
            )])
        );
    }

    #[test]
    fn qa_aff_2_vertex_generate_and_count_requests_pin_same_cache() {
        for capability in [
            ProviderCapability::GeminiGenerateContent,
            ProviderCapability::GeminiCountTokens,
        ] {
            let keys = extract_request_affinity_keys(
                capability,
                &serde_json::json!({"cachedContent": "cachedContents/vertex-id"}),
                "principal-vertex",
            )
            .expect("extracts cache reference");
            assert_eq!(
                kinds_and_digests(&keys),
                HashSet::from([(
                    UpstreamAffinityKind::GeminiCachedContent,
                    digest("cachedContents/vertex-id")
                )])
            );
        }
    }

    #[test]
    fn extracts_gemini_count_tokens_top_level_cached_content() {
        let keys = extract_request_affinity_keys(
            ProviderCapability::GeminiCountTokens,
            &serde_json::json!({"cachedContent": "cachedContents/count-top"}),
            "principal-gemini-count",
        )
        .expect("extracts");
        assert_eq!(
            kinds_and_digests(&keys),
            HashSet::from([(
                UpstreamAffinityKind::GeminiCachedContent,
                digest("cachedContents/count-top")
            )])
        );
    }

    #[test]
    fn extracts_gemini_count_tokens_nested_cached_content() {
        let keys = extract_request_affinity_keys(
            ProviderCapability::GeminiCountTokens,
            &serde_json::json!({
                "generateContentRequest": {"cachedContent": "cachedContents/count-nested"}
            }),
            "principal-gemini-count",
        )
        .expect("extracts");
        assert_eq!(
            kinds_and_digests(&keys),
            HashSet::from([(
                UpstreamAffinityKind::GeminiCachedContent,
                digest("cachedContents/count-nested")
            )])
        );
    }

    #[test]
    fn extracts_structural_anthropic_files_and_containers() {
        let keys = extract_anthropic_web_search_affinity_keys(
            &serde_json::json!({
                "container": "container_1",
                "messages": [{
                    "role": "user",
                    "content": [
                        {"type": "document", "source": {"type": "file", "file_id": "file_doc"}},
                        {"type": "container_upload", "file_id": "file_upload"},
                        {"type": "tool_result", "metadata": {"file_id": "file_untyped"}},
                        {
                            "type": "bash_code_execution_tool_result",
                            "content": {
                                "type": "bash_code_execution_result",
                                "content": [{"type": "bash_code_execution_output", "file_id": "file_out"}]
                            }
                        }
                    ]
                }]
            }),
            "principal-anthropic",
        )
        .expect("extracts");
        assert!(keys.iter().all(|key| key.provider == "anthropic"));
        assert_eq!(
            kinds_and_digests(&keys),
            HashSet::from([
                (
                    UpstreamAffinityKind::AnthropicContainerId,
                    digest("container_1")
                ),
                (UpstreamAffinityKind::AnthropicFileId, digest("file_doc")),
                (UpstreamAffinityKind::AnthropicFileId, digest("file_upload")),
                (UpstreamAffinityKind::AnthropicFileId, digest("file_out")),
            ])
        );
        assert!(request_enables_anthropic_web_search(&serde_json::json!({
            "tools": [{"type": "code_execution_20250825", "name": "code_execution"}]
        })));
    }

    #[test]
    fn preserves_anthropic_count_tokens_file_and_container_affinity() {
        let body = serde_json::json!({
            "container": "container_count_tokens",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "document", "source": {"type": "file", "file_id": "file_count_tokens"}},
                    {"type": "container_upload", "file_id": "upload_count_tokens"}
                ]
            }]
        });
        let message_keys = extract_request_affinity_keys(
            ProviderCapability::AnthropicMessages,
            &body,
            "principal",
        )
        .expect("extracts message affinity");
        let count_tokens_keys = extract_request_affinity_keys(
            ProviderCapability::AnthropicCountTokens,
            &body,
            "principal",
        )
        .expect("extracts count_tokens affinity");

        assert_eq!(count_tokens_keys, message_keys);
        assert_eq!(
            kinds_and_digests(&count_tokens_keys),
            HashSet::from([
                (
                    UpstreamAffinityKind::AnthropicContainerId,
                    digest("container_count_tokens")
                ),
                (
                    UpstreamAffinityKind::AnthropicFileId,
                    digest("file_count_tokens")
                ),
                (
                    UpstreamAffinityKind::AnthropicFileId,
                    digest("upload_count_tokens")
                ),
            ])
        );
    }

    #[test]
    fn extracts_anthropic_container_from_message_start_and_delta_sse() {
        let start = crate::usage_parser::parse_sse_event(
            b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"container\":{\"id\":\"container_start\"},\"content\":[]}}\n\n",
        );
        let delta = crate::usage_parser::parse_sse_event(
            b"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"container\":{\"id\":\"container_delta\",\"expires_at\":\"2026-10-18T00:00:00Z\"}}}\n\n",
        );
        let start_keys =
            extract_anthropic_web_search_affinity_keys_from_sse_event(&start, "principal-sse")
                .expect("extracts");
        let delta_keys =
            extract_anthropic_web_search_affinity_keys_from_sse_event(&delta, "principal-sse")
                .expect("extracts");
        assert_eq!(
            kinds_and_digests(&start_keys),
            HashSet::from([(
                UpstreamAffinityKind::AnthropicContainerId,
                digest("container_start")
            )])
        );
        assert_eq!(
            kinds_and_digests(&delta_keys),
            HashSet::from([(
                UpstreamAffinityKind::AnthropicContainerId,
                digest("container_delta")
            )])
        );
    }

    #[test]
    fn key_cap_is_shared_across_provider_state_kinds() {
        let mut content = (0..MAX_UPSTREAM_AFFINITY_KEYS_PER_REQUEST)
            .map(|index| {
                serde_json::json!({"type": "container_upload", "file_id": format!("file_{index}")})
            })
            .collect::<Vec<_>>();
        let at_cap = serde_json::json!({"messages": [{"content": content.clone()}]});
        assert_eq!(
            extract_anthropic_web_search_affinity_keys(&at_cap, "principal-limit")
                .expect("exactly at cap")
                .len(),
            MAX_UPSTREAM_AFFINITY_KEYS_PER_REQUEST
        );
        content.push(serde_json::json!({"type": "container_upload", "file_id": "file_0"}));
        let duplicate = serde_json::json!({"messages": [{"content": content}]});
        assert!(extract_anthropic_web_search_affinity_keys(&duplicate, "principal-limit").is_ok());
        let over_cap = serde_json::json!({
            "container": "container_over",
            "messages": [{"content": at_cap["messages"][0]["content"].clone()}]
        });
        assert_eq!(
            extract_anthropic_web_search_affinity_keys(&over_cap, "principal-limit"),
            Err(UpstreamAffinityExtractionError::TooManyKeys)
        );
    }

    #[test]
    fn cached_contents_response_requires_canonical_name_and_expiry() {
        let response = serde_json::json!({
            "name": "cachedContents/abc",
            "expireTime": "2026-11-09T00:00:00Z",
        });
        let keys = extract_response_affinity_keys(
            ProviderCapability::GeminiCachedContentsCreate,
            &response,
            "principal-gemini",
        )
        .expect("canonical cached content response");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].kind, UpstreamAffinityKind::GeminiCachedContent);
        assert_eq!(keys[0].value_sha256, digest("cachedContents/abc"));
        assert_eq!(
            gemini_cached_content_expiry(&response).expect("expiry"),
            1_794_182_400
        );

        for invalid in [
            serde_json::json!({}),
            serde_json::json!({"name": ""}),
            serde_json::json!({"name": "cachedContents/"}),
            serde_json::json!({"name": "cachedContents/a/b"}),
            serde_json::json!({"name": 7}),
        ] {
            assert_eq!(
                extract_response_affinity_keys(
                    ProviderCapability::GeminiCachedContentsCreate,
                    &invalid,
                    "principal-gemini",
                ),
                Err(UpstreamAffinityExtractionError::InvalidResponseAffinity)
            );
        }
        assert_eq!(
            gemini_cached_content_expiry(&serde_json::json!({"name": "cachedContents/abc"})),
            Err(UpstreamAffinityExtractionError::InvalidResponseAffinity)
        );
    }

    #[test]
    fn realtime_call_affinity_key_extracts_call_id_and_ignores_query() {
        let plain = realtime_call_affinity_key("principal-openai", "/v1/realtime/calls/rtc_123")
            .expect("plain location");
        let nested = realtime_call_affinity_key(
            "principal-openai",
            "/v1/realtime/calls/calls/rtc_123?intent=quicksilver",
        )
        .expect("nested location with query");
        assert_eq!(plain, nested);
        assert_eq!(plain.kind, UpstreamAffinityKind::OpenaiRealtimeCall);
        assert_eq!(plain.value_sha256, digest("rtc_123"));

        let uuid_key = realtime_call_affinity_key(
            "principal-openai",
            "/v1/realtime/calls/019eb97d-8e9a-7ff3-94b0-ea019babd5d7/",
        )
        .expect("uuid location with trailing slash");
        assert_eq!(
            uuid_key.value_sha256,
            digest("019eb97d-8e9a-7ff3-94b0-ea019babd5d7")
        );
        assert_eq!(realtime_call_affinity_key("principal-openai", "/"), None);
    }

    #[test]
    fn unknown_reasoning_only_classification_and_pending_digests_respect_boundaries() {
        let base_reasoning = serde_json::json!({
            "model": "gpt-5-codex",
            "include": ["reasoning.encrypted_content"],
            "input": [
                {"type": "reasoning", "id": "rs_1", "summary": [], "encrypted_content": "gAAAAABforeign_1"},
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]},
            ]
        });
        assert!(is_openai_unknown_reasoning_only(
            ProviderCapability::OpenaiResponses,
            &base_reasoning
        ));
        assert!(is_openai_unknown_reasoning_only(
            ProviderCapability::OpenaiResponsesCompact,
            &base_reasoning
        ));
        assert!(!is_openai_unknown_reasoning_only(
            ProviderCapability::AnthropicMessages,
            &base_reasoning
        ));

        for excluded in [
            serde_json::json!({
                "previous_response_id": "resp_1",
                "input": [{"type": "reasoning", "encrypted_content": "gAAAAABforeign_1"}]
            }),
            serde_json::json!({
                "conversation": "conv_1",
                "input": [{"type": "reasoning", "encrypted_content": "gAAAAABforeign_1"}]
            }),
            serde_json::json!({
                "input": [
                    {"type": "reasoning", "encrypted_content": "gAAAAABforeign_1"},
                    {"type": "compaction", "id": "cmp_1", "encrypted_content": "gAAAAABcmp_1"},
                ]
            }),
            serde_json::json!({
                "input": [
                    {"type": "reasoning", "encrypted_content": "gAAAAABforeign_1"},
                    {"type": "compaction_summary", "id": "cmp_2", "encrypted_content": "gAAAAABcmp_2"},
                ]
            }),
            serde_json::json!({
                "input": "just a string"
            }),
            serde_json::json!({
                "input": [{"type": "message", "role": "user", "content": "hi"}]
            }),
        ] {
            assert!(
                !is_openai_unknown_reasoning_only(ProviderCapability::OpenaiResponses, &excluded),
                "unexpected classification for {excluded}"
            );
        }

        let keys = vec![
            UpstreamAffinityKey {
                principal_id: "p".to_owned(),
                provider: OPENAI_PROVIDER.to_owned(),
                kind: UpstreamAffinityKind::OpenaiResponseId,
                value_sha256: digest("gAAAAABforeign_1"),
            },
            UpstreamAffinityKey {
                principal_id: "p".to_owned(),
                provider: OPENAI_PROVIDER.to_owned(),
                kind: UpstreamAffinityKind::OpenaiConversationId,
                value_sha256: digest("conv_ignored"),
            },
        ];
        let matched = collect_openai_pending_reasoning_digests(
            ProviderCapability::OpenaiResponses,
            &base_reasoning,
            &keys,
        );
        assert_eq!(matched, HashSet::from([digest("gAAAAABforeign_1")]));
    }

    #[test]
    fn strip_openai_reasoning_preserves_messages_compaction_schemas_and_owned_items() {
        let mixed = serde_json::json!({
            "model": "gpt-5-codex",
            "include": ["reasoning.encrypted_content"],
            "input": [
                {"type": "reasoning", "id": "rs_foreign", "summary": [], "encrypted_content": "gAAAAABforeign"},
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "q1"}]},
                {"type": "reasoning", "id": "rs_owned", "summary": [], "encrypted_content": "gAAAAABowned"},
                {"type": "reasoning", "id": "rs_plain", "summary": [{"type": "summary_text", "text": "thought"}]},
                {"type": "additional_tools", "tools": [{"parameters": {"default": {"type": "reasoning", "encrypted_content": "schema-literal"}}}]},
                {"type": "compaction", "id": "cmp_1", "encrypted_content": "gAAAAABcmp"},
            ]
        });

        let selective_digests = HashSet::from([digest("gAAAAABforeign")]);
        let selective =
            strip_openai_reasoning_encrypted_items(&mixed, Some(&selective_digests)).unwrap();
        let selective_items = selective["input"].as_array().unwrap();
        assert_eq!(selective_items.len(), 5);
        assert_eq!(selective_items[0]["role"], "user");
        assert_eq!(selective_items[1]["id"], "rs_owned");
        assert_eq!(selective_items[2]["id"], "rs_plain");
        assert_eq!(selective_items[3]["type"], "additional_tools");
        assert_eq!(selective_items[4]["type"], "compaction");
        assert_eq!(
            selective["include"],
            serde_json::json!(["reasoning.encrypted_content"])
        );

        let all = strip_openai_reasoning_encrypted_items(&mixed, None).unwrap();
        assert_eq!(all["input"].as_array().unwrap().len(), 4);
        assert_eq!(
            strip_openai_reasoning_encrypted_items(
                &mixed,
                Some(&HashSet::from([digest("missing")]))
            ),
            None
        );
    }

    #[test]
    fn invalid_encrypted_content_error_matchers_handle_plain_and_gzip_responses() {
        use std::io::Write;

        let by_code = serde_json::json!({
            "error": {
                "type": "invalid_request_error",
                "code": "invalid_encrypted_content",
                "message": "opaque error"
            }
        });
        assert!(is_openai_invalid_encrypted_content_error_value(&by_code));

        let by_ws_message = serde_json::json!({
            "type": "error",
            "status": 400,
            "error": {
                "message": "The encrypted content for item rs_1 could not be verified."
            }
        });
        assert!(is_openai_invalid_encrypted_content_error_value(
            &by_ws_message
        ));

        let unrelated = serde_json::json!({
            "error": {
                "type": "invalid_request_error",
                "code": "invalid_value",
                "message": "Invalid encrypted_content must be omitted? No, bad model id"
            }
        });
        assert!(!is_openai_invalid_encrypted_content_error_value(&unrelated));
        let unrelated_no_code = serde_json::json!({
            "error": {
                "type": "invalid_request_error",
                "message": "Invalid encrypted_content must be omitted for an unsupported model flag"
            }
        });
        assert!(!is_openai_invalid_encrypted_content_error_value(
            &unrelated_no_code
        ));
        let unrelated_model = serde_json::json!({
            "error": {
                "type": "invalid_request_error",
                "code": "model_not_found",
                "message": "model does not exist"
            }
        });
        assert!(!is_openai_invalid_encrypted_content_error_value(
            &unrelated_model
        ));

        let payload = serde_json::to_vec(&by_code).unwrap();
        assert!(is_openai_invalid_encrypted_content_http_error(
            StatusCode::BAD_REQUEST,
            &HeaderMap::new(),
            &payload
        ));
        assert!(!is_openai_invalid_encrypted_content_http_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &HeaderMap::new(),
            &payload
        ));

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&payload).unwrap();
        let compressed = encoder.finish().unwrap();
        let mut gzip_headers = HeaderMap::new();
        gzip_headers.insert(
            http::header::CONTENT_ENCODING,
            http::HeaderValue::from_static("gzip"),
        );
        assert!(is_openai_invalid_encrypted_content_http_error(
            StatusCode::BAD_REQUEST,
            &gzip_headers,
            &compressed
        ));
    }
}
