use std::collections::HashSet;

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
    visit_value(value, principal_id, &mut seen, &mut keys)?;
    Ok(keys)
}

pub(crate) fn extract_anthropic_web_search_affinity_keys_from_sse_event(
    raw_event: &[u8],
    principal_id: &str,
) -> Result<Vec<UpstreamAffinityKey>, UpstreamAffinityExtractionError> {
    let event_name = crate::usage_parser::sse_event_name(raw_event);
    let text = std::str::from_utf8(raw_event)
        .map_err(|_| UpstreamAffinityExtractionError::InvalidSseData)?;
    let mut payload = String::new();
    let mut has_data = false;
    for line in text.split(['\r', '\n']) {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        if has_data {
            payload.push('\n');
        }
        payload.push_str(data.strip_prefix(' ').unwrap_or(data));
        has_data = true;
    }
    if !has_data {
        return Ok(Vec::new());
    }
    if payload.trim() == "[DONE]" {
        return Ok(Vec::new());
    }

    let value = sonic_rs::from_str::<Value>(&payload)
        .map_err(|_| UpstreamAffinityExtractionError::InvalidSseData)?;
    let event_type = value
        .get("type")
        .and_then(Value::as_str)
        .map(str::as_bytes)
        .or(event_name);
    if event_type != Some(b"content_block_start") {
        return Ok(Vec::new());
    }

    let mut seen = HashSet::new();
    let mut keys = Vec::new();
    visit_value(&value, principal_id, &mut seen, &mut keys)?;
    Ok(keys)
}

fn visit_value(
    value: &Value,
    principal_id: &str,
    seen: &mut HashSet<[u8; 32]>,
    keys: &mut Vec<UpstreamAffinityKey>,
) -> Result<(), UpstreamAffinityExtractionError> {
    match value {
        Value::Array(items) => {
            for item in items {
                visit_value(item, principal_id, seen, keys)?;
            }
        }
        Value::Object(object) => {
            if object.get("type").and_then(Value::as_str) == Some(WEB_SEARCH_TOOL_RESULT)
                && let Some(content) = object.get("content").and_then(Value::as_array)
            {
                for item in content {
                    let Some(encrypted_content) =
                        item.get("encrypted_content").and_then(Value::as_str)
                    else {
                        continue;
                    };
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
            }
            for child in object.values() {
                visit_value(child, principal_id, seen, keys)?;
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
    fn extracts_only_content_block_start_sse_payloads() {
        let raw = b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"web_search_tool_result\",\"content\":[{\"encrypted_content\":\"opaque-sse\"}]}}\n\n";
        let ignored = b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"content_block\":{\"type\":\"web_search_tool_result\",\"content\":[{\"encrypted_content\":\"opaque-delta\"}]}}\n\n";

        let keys = extract_anthropic_web_search_affinity_keys_from_sse_event(raw, "principal-sse")
            .expect("extracts");

        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].principal_id, "principal-sse");
        assert_eq!(
            keys[0].value_sha256,
            <[u8; 32]>::from(Sha256::digest(b"opaque-sse"))
        );
        assert!(
            extract_anthropic_web_search_affinity_keys_from_sse_event(ignored, "principal-sse")
                .expect("extracts")
                .is_empty()
        );
    }

    #[test]
    fn joins_multiline_sse_data_before_parsing_json() {
        let raw = b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\ndata: \"content_block\":{\"type\":\"web_search_tool_result\",\ndata: \"content\":[{\"encrypted_content\":\"opaque-multiline\"}]}}\n\n";

        let keys = extract_anthropic_web_search_affinity_keys_from_sse_event(raw, "principal-sse")
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

        let keys = extract_anthropic_web_search_affinity_keys_from_sse_event(raw, "principal-sse")
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
