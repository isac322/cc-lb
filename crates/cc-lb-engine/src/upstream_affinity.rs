use std::collections::HashSet;

use crate::provider_capability::ProviderCapability;
use crate::usage_parser::ParsedSseEvent;
use cc_lb_storage_api::{UpstreamAffinityKey, UpstreamAffinityKind};
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
}

/// Provider label stored with every affinity key derived for `capability`.
pub(crate) const fn affinity_provider(capability: ProviderCapability) -> &'static str {
    match capability {
        ProviderCapability::AnthropicMessages => ANTHROPIC_PROVIDER,
        ProviderCapability::OpenaiResponses => OPENAI_PROVIDER,
        ProviderCapability::GeminiGenerateContent => GEMINI_PROVIDER,
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
        ProviderCapability::OpenaiResponses => true,
        // generateContent never creates `cachedContents`; `thoughtSignature`
        // is forwarded without affinity.
        ProviderCapability::GeminiGenerateContent => false,
    }
}

/// Extracts the provider-owned state a request references. Every returned key
/// must resolve to one upstream before the request may be routed.
pub(crate) fn extract_request_affinity_keys(
    capability: ProviderCapability,
    value: &Value,
    principal_id: &str,
) -> Result<Vec<UpstreamAffinityKey>, UpstreamAffinityExtractionError> {
    let mut collector = KeyCollector::new(capability, principal_id);
    match capability {
        ProviderCapability::AnthropicMessages => collector.anthropic_message(value)?,
        ProviderCapability::OpenaiResponses => {
            if let Some(object) = value.as_object() {
                collector.reference(
                    UpstreamAffinityKind::OpenaiResponseId,
                    object.get("previous_response_id"),
                )?;
                collector.reference(
                    UpstreamAffinityKind::OpenaiConversationId,
                    object.get("conversation"),
                )?;
            }
        }
        ProviderCapability::GeminiGenerateContent => {
            collector.reference(
                UpstreamAffinityKind::GeminiCachedContent,
                value.get("cachedContent"),
            )?;
        }
    }
    Ok(collector.keys)
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
        ProviderCapability::OpenaiResponses => collector.openai_response(value)?,
        ProviderCapability::GeminiGenerateContent => {}
    }
    Ok(collector.keys)
}

/// Extracts the provider-owned state a single successful SSE event creates.
pub(crate) fn extract_response_affinity_keys_from_sse_event(
    capability: ProviderCapability,
    event: &ParsedSseEvent<'_>,
    principal_id: &str,
) -> Result<Vec<UpstreamAffinityKey>, UpstreamAffinityExtractionError> {
    if capability == ProviderCapability::GeminiGenerateContent {
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
        (ProviderCapability::OpenaiResponses, Some(event_type))
            if event_type.starts_with(OPENAI_RESPONSE_EVENT_PREFIX) =>
        {
            if let Some(response) = value.get("response") {
                collector.openai_response(response)?;
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
}

impl<'a> KeyCollector<'a> {
    fn new(capability: ProviderCapability, principal_id: &'a str) -> Self {
        Self {
            provider: affinity_provider(capability),
            principal_id,
            seen: HashSet::new(),
            keys: Vec::new(),
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
        let id = value.and_then(|value| {
            value
                .as_str()
                .or_else(|| value.get("id").and_then(Value::as_str))
        });
        match id {
            Some(id) => self.push(kind, id),
            None => Ok(()),
        }
    }

    fn openai_response(&mut self, response: &Value) -> Result<(), UpstreamAffinityExtractionError> {
        let Some(object) = response.as_object() else {
            return Ok(());
        };
        self.reference(UpstreamAffinityKind::OpenaiResponseId, object.get("id"))?;
        self.reference(
            UpstreamAffinityKind::OpenaiConversationId,
            object.get("conversation"),
        )
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
                "input": [{"type": "input_text", "text": "resp_ignored"}]
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
            &serde_json::json!({"id": "resp_2", "object": "response", "conversation": {"id": "conv_2"}}),
            "principal-openai",
        )
        .expect("extracts");
        assert_eq!(
            kinds_and_digests(&stored),
            HashSet::from([
                (UpstreamAffinityKind::OpenaiResponseId, digest("resp_2")),
                (UpstreamAffinityKind::OpenaiConversationId, digest("conv_2")),
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
    fn extracts_only_top_level_gemini_cached_content() {
        let keys = extract_request_affinity_keys(
            ProviderCapability::GeminiGenerateContent,
            &serde_json::json!({
                "cachedContent": "cachedContents/abc",
                "contents": [{"parts": [{"text": "hi", "thoughtSignature": "sig"}]}]
            }),
            "principal-gemini",
        )
        .expect("extracts");
        assert!(keys.iter().all(|key| key.provider == "gemini"));
        assert_eq!(
            kinds_and_digests(&keys),
            HashSet::from([(
                UpstreamAffinityKind::GeminiCachedContent,
                digest("cachedContents/abc")
            )])
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
}
