use std::collections::HashSet;

use crate::usage_parser::ParsedSseEvent;
use cc_lb_storage_api::{UpstreamAffinityKey, UpstreamAffinityKind};
use serde_json::Value;
use sha2::{Digest, Sha256};

const ANTHROPIC_PROVIDER: &str = "anthropic";
const WEB_SEARCH_TOOL_RESULT: &str = "web_search_tool_result";
const WEB_SEARCH_TOOL_PREFIX: &str = "web_search_";

pub(crate) const MAX_UPSTREAM_AFFINITY_KEYS_PER_REQUEST: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UpstreamAffinityExtractionError {
    TooManyKeys,
    InvalidSseData,
}
pub(crate) fn request_enables_anthropic_web_search(value: &Value) -> bool {
    value
        .get("tools")
        .and_then(Value::as_array)
        .is_some_and(|tools| {
            tools.iter().any(|tool| {
                tool.get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|tool_type| tool_type.starts_with(WEB_SEARCH_TOOL_PREFIX))
            })
        })
}

pub(crate) fn extract_anthropic_web_search_affinity_keys(
    value: &Value,
    principal_id: &str,
) -> Result<Vec<UpstreamAffinityKey>, UpstreamAffinityExtractionError> {
    let mut seen = HashSet::new();
    let mut keys = Vec::new();
    visit_value(value, false, principal_id, &mut seen, &mut keys)?;
    Ok(keys)
}

pub(crate) fn extract_anthropic_web_search_affinity_keys_from_sse_event(
    event: &ParsedSseEvent<'_>,
    principal_id: &str,
) -> Result<Vec<UpstreamAffinityKey>, UpstreamAffinityExtractionError> {
    let Some(value) = event
        .value()
        .map_err(|_| UpstreamAffinityExtractionError::InvalidSseData)?
    else {
        return Ok(Vec::new());
    };
    if event.event_type() != Some("content_block_start") {
        return Ok(Vec::new());
    }

    let mut seen = HashSet::new();
    let mut keys = Vec::new();
    visit_value(value, false, principal_id, &mut seen, &mut keys)?;
    Ok(keys)
}

fn visit_value(
    value: &Value,
    affinity_content_item: bool,
    principal_id: &str,
    seen: &mut HashSet<[u8; 32]>,
    keys: &mut Vec<UpstreamAffinityKey>,
) -> Result<(), UpstreamAffinityExtractionError> {
    match value {
        Value::Array(items) => {
            for item in items {
                if matches!(item, Value::Array(_) | Value::Object(_)) {
                    visit_value(item, false, principal_id, seen, keys)?;
                }
            }
        }
        Value::Object(object) => {
            if affinity_content_item
                && let Some(encrypted_content) =
                    object.get("encrypted_content").and_then(Value::as_str)
            {
                let digest: [u8; 32] = Sha256::digest(encrypted_content.as_bytes()).into();
                if seen.insert(digest) {
                    if keys.len() == MAX_UPSTREAM_AFFINITY_KEYS_PER_REQUEST {
                        return Err(UpstreamAffinityExtractionError::TooManyKeys);
                    }
                    keys.push(UpstreamAffinityKey {
                        principal_id: principal_id.to_owned(),
                        provider: ANTHROPIC_PROVIDER.to_owned(),
                        kind: UpstreamAffinityKind::AnthropicWebSearchEncryptedContent,
                        value_sha256: digest,
                    });
                }
            }

            let has_web_search_content =
                object.get("type").and_then(Value::as_str) == Some(WEB_SEARCH_TOOL_RESULT);
            for (name, child) in object {
                if has_web_search_content && name == "content" {
                    if let Some(items) = child.as_array() {
                        for item in items {
                            visit_value(item, true, principal_id, seen, keys)?;
                        }
                    } else if matches!(child, Value::Array(_) | Value::Object(_)) {
                        visit_value(child, false, principal_id, seen, keys)?;
                    }
                } else if matches!(child, Value::Array(_) | Value::Object(_)) {
                    visit_value(child, false, principal_id, seen, keys)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn invalid_joined_multiline_rejects_legacy_observer_fallback() {
        let raw = b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"web_search_tool_result\",\"content\":[{\"encrypted_content\":\"opaque-a\"}]}}\ndata: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"web_search_tool_result\",\"content\":[{\"encrypted_content\":\"opaque-b\"}]}}\n\n";
        let event = crate::usage_parser::parse_sse_event(raw);

        assert_eq!(
            extract_anthropic_web_search_affinity_keys_from_sse_event(&event, "principal-sse"),
            Err(UpstreamAffinityExtractionError::InvalidSseData)
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
}
