use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::fmt;
use thiserror::Error;
use url::Url;
use uuid::Uuid;

use cc_lb_storage_api::CacheTtl;

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("shaped body exceeds snapshot_max_bytes ({actual} > {max})")]
    TooLarge { actual: usize, max: usize },
    #[error("shaped body is not valid JSON")]
    InvalidJson,
    #[error("shaped body missing top-level object")]
    NotAnObject,
    #[error("persisted snapshot contains invalid url")]
    InvalidUrl,
    #[error("persisted snapshot contains invalid method")]
    InvalidMethod,
    #[error("persisted snapshot contains invalid header")]
    InvalidHeader,
}

#[derive(Clone, Debug)]
pub struct RequestSnapshot {
    pub url: Url,
    pub method: Method,
    pub headers: HeaderMap,
    pub body: Bytes,
    pub upstream_id: Uuid,
    pub ttl: CacheTtl,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct PersistedRequestSnapshot {
    pub url: String,
    pub method: String,
    pub headers: Vec<PersistedHeader>,
    pub body: Vec<u8>,
    pub upstream_id: Uuid,
    pub ttl: CacheTtl,
}

impl fmt::Debug for PersistedRequestSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PersistedRequestSnapshot")
            .field("url", &self.url)
            .field("method", &self.method)
            .field("headers", &self.headers)
            .field(
                "body",
                &format_args!("<{} bytes redacted>", self.body.len()),
            )
            .field("upstream_id", &self.upstream_id)
            .field("ttl", &self.ttl)
            .finish()
    }
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct PersistedHeader {
    pub name: String,
    pub value: Vec<u8>,
}

impl fmt::Debug for PersistedHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PersistedHeader")
            .field("name", &self.name)
            .field(
                "value",
                &format_args!("<{} bytes redacted>", self.value.len()),
            )
            .finish()
    }
}

impl RequestSnapshot {
    pub fn capture(
        url: Url,
        method: Method,
        headers: HeaderMap,
        body: Bytes,
        upstream_id: Uuid,
        ttl: CacheTtl,
        max_bytes: usize,
    ) -> Result<Self, SnapshotError> {
        if body.is_empty() {
            return Err(SnapshotError::InvalidJson);
        }
        let mut value: Value = match sonic_rs::from_slice(&body) {
            Ok(value) => value,
            Err(_) if body.len() > max_bytes => {
                return Err(SnapshotError::TooLarge {
                    actual: body.len(),
                    max: max_bytes,
                });
            }
            Err(_) => return Err(SnapshotError::InvalidJson),
        };
        let map = value.as_object_mut().ok_or(SnapshotError::NotAnObject)?;
        let body = if trim_uncached_messages_for_keepalive(map) {
            let trimmed = serde_json::to_vec(&value).map_err(|_| SnapshotError::InvalidJson)?;
            Bytes::from(trimmed)
        } else {
            body
        };
        if body.len() > max_bytes {
            return Err(SnapshotError::TooLarge {
                actual: body.len(),
                max: max_bytes,
            });
        }
        Ok(Self {
            url,
            method,
            headers,
            body,
            upstream_id,
            ttl,
        })
    }

    pub fn build_keepalive_body(&self) -> Result<Bytes, SnapshotError> {
        let mut value: Value =
            sonic_rs::from_slice(&self.body).map_err(|_| SnapshotError::InvalidJson)?;
        let map = value.as_object_mut().ok_or(SnapshotError::NotAnObject)?;
        transform_for_keepalive(map);
        let serialized = serde_json::to_vec(&value).map_err(|_| SnapshotError::InvalidJson)?;
        Ok(Bytes::from(serialized))
    }

    pub fn to_persisted(&self) -> PersistedRequestSnapshot {
        PersistedRequestSnapshot {
            url: self.url.to_string(),
            method: self.method.as_str().to_owned(),
            headers: self
                .headers
                .iter()
                .map(|(name, value)| PersistedHeader {
                    name: name.as_str().to_owned(),
                    value: value.as_bytes().to_vec(),
                })
                .collect(),
            body: self.body.to_vec(),
            upstream_id: self.upstream_id,
            ttl: self.ttl,
        }
    }

    pub fn from_persisted(persisted: PersistedRequestSnapshot) -> Result<Self, SnapshotError> {
        let url = Url::parse(&persisted.url).map_err(|_| SnapshotError::InvalidUrl)?;
        let method = persisted
            .method
            .parse::<Method>()
            .map_err(|_| SnapshotError::InvalidMethod)?;
        let mut headers = HeaderMap::new();
        for header in persisted.headers {
            let name = HeaderName::from_bytes(header.name.as_bytes())
                .map_err(|_| SnapshotError::InvalidHeader)?;
            let value =
                HeaderValue::from_bytes(&header.value).map_err(|_| SnapshotError::InvalidHeader)?;
            headers.insert(name, value);
        }
        Self::capture(
            url,
            method,
            headers,
            Bytes::from(persisted.body),
            persisted.upstream_id,
            persisted.ttl,
            usize::MAX,
        )
    }
}

/// Shapes a captured request for a zero-output cache keepalive.
///
/// Only `max_tokens`, `stream`, and uncached trailing messages are modified so
/// `thinking`, `output_config`, and `tool_choice` remain identical to the live
/// request and hit the same prompt-cache prefix.
fn transform_for_keepalive(map: &mut Map<String, Value>) {
    map.insert("max_tokens".into(), Value::from(0));
    map.remove("stream");
    trim_uncached_messages_for_keepalive(map);
}

fn trim_uncached_messages_for_keepalive(map: &mut Map<String, Value>) -> bool {
    if map.get("cache_control").is_some() {
        return false;
    }

    let explicit_system_or_tools = ["system", "tools"]
        .iter()
        .filter_map(|key| map.get(*key))
        .any(value_has_cache_control);
    if !map.get("messages").is_some_and(Value::is_array) {
        if explicit_system_or_tools {
            map.insert(
                "messages".to_owned(),
                serde_json::json!([{"role": "user", "content": "."}]),
            );
            return true;
        }
        return false;
    }
    let messages = map
        .get_mut("messages")
        .and_then(Value::as_array_mut)
        .expect("messages is an array");
    let last_msg_idx = messages
        .iter()
        .enumerate()
        .filter(|(_, message)| message_has_cache_control(message))
        .map(|(index, _)| index)
        .next_back();

    let Some(last_msg_idx) = last_msg_idx else {
        if explicit_system_or_tools {
            let minimal = vec![serde_json::json!({"role": "user", "content": "."})];
            if *messages != minimal {
                *messages = minimal;
                return true;
            }
        }
        return false;
    };

    let mut changed = false;
    let role = messages[last_msg_idx].get("role").and_then(Value::as_str);
    match role {
        Some("user") => {
            if messages.len() > last_msg_idx + 1 {
                messages.truncate(last_msg_idx + 1);
                changed = true;
            }
            let mut required_ids = assistant_tool_use_ids(
                last_msg_idx
                    .checked_sub(1)
                    .and_then(|idx| messages.get(idx)),
            );
            let message = &mut messages[last_msg_idx];
            if message.get("cache_control").is_none()
                && let Some(content) = message.get_mut("content").and_then(Value::as_array_mut)
                && let Some(last_blk_idx) = content
                    .iter()
                    .enumerate()
                    .filter(|(_, block)| block.get("cache_control").is_some())
                    .map(|(index, _)| index)
                    .next_back()
                && content.len() > last_blk_idx + 1
            {
                for block in &content[..=last_blk_idx] {
                    if block.get("type").and_then(Value::as_str) == Some("tool_result")
                        && let Some(id) = block.get("tool_use_id").and_then(Value::as_str)
                    {
                        required_ids.retain(|req| req != id);
                    }
                }
                let tail = content.split_off(last_blk_idx + 1);
                let tail_len = tail.len();
                let kept_tail = retain_matching_tool_results(tail, &mut required_ids);
                if kept_tail.len() != tail_len {
                    changed = true;
                }
                content.extend(kept_tail);
            }
        }
        Some("assistant") => {
            let mut required_ids = assistant_tool_use_ids(messages.get(last_msg_idx));
            if !required_ids.is_empty()
                && messages
                    .get(last_msg_idx + 1)
                    .and_then(|message| message.get("role").and_then(Value::as_str))
                    == Some("user")
            {
                if messages.len() > last_msg_idx + 2 {
                    messages.truncate(last_msg_idx + 2);
                    changed = true;
                }
                let kept_results = messages[last_msg_idx + 1]
                    .get_mut("content")
                    .and_then(Value::as_array_mut)
                    .map(|content| {
                        let old_len = content.len();
                        let drained = std::mem::take(content);
                        let kept = retain_matching_tool_results(drained, &mut required_ids);
                        if kept.len() != old_len {
                            changed = true;
                        }
                        *content = kept;
                        !content.is_empty()
                    })
                    .unwrap_or(false);
                if !kept_results {
                    messages.truncate(last_msg_idx + 1);
                    messages.push(serde_json::json!({"role": "user", "content": "."}));
                    changed = true;
                }
            } else {
                messages.truncate(last_msg_idx + 1);
                messages.push(serde_json::json!({"role": "user", "content": "."}));
                changed = true;
            }
        }
        _ => {
            if messages.len() > last_msg_idx + 1 {
                messages.truncate(last_msg_idx + 1);
                changed = true;
            }
        }
    }
    changed
}

fn assistant_tool_use_ids(message: Option<&Value>) -> Vec<String> {
    let Some(message) = message else {
        return Vec::new();
    };
    if message.get("role").and_then(Value::as_str) != Some("assistant") {
        return Vec::new();
    }
    let Some(content) = message.get("content").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut ids = Vec::new();
    for block in content {
        if block.get("type").and_then(Value::as_str) == Some("tool_use")
            && let Some(id) = block.get("id").and_then(Value::as_str)
        {
            ids.push(id.to_owned());
        }
    }
    ids
}

fn retain_matching_tool_results(blocks: Vec<Value>, required_ids: &mut Vec<String>) -> Vec<Value> {
    let mut kept = Vec::new();
    for block in blocks {
        if block.get("type").and_then(Value::as_str) == Some("tool_result")
            && let Some(id) = block.get("tool_use_id").and_then(Value::as_str)
            && let Some(pos) = required_ids.iter().position(|req| req == id)
        {
            required_ids.swap_remove(pos);
            kept.push(block);
        }
    }
    kept
}

fn message_has_cache_control(message: &Value) -> bool {
    message.get("cache_control").is_some()
        || message
            .get("content")
            .and_then(Value::as_array)
            .is_some_and(|content| {
                content
                    .iter()
                    .any(|block| block.get("cache_control").is_some())
            })
}

fn value_has_cache_control(value: &Value) -> bool {
    match value {
        Value::Object(map) => {
            map.contains_key("cache_control") || map.values().any(value_has_cache_control)
        }
        Value::Array(values) => values.iter().any(value_has_cache_control),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use serde_json::json;

    fn empty_snapshot(body: Bytes) -> RequestSnapshot {
        RequestSnapshot {
            url: "https://api.anthropic.com/v1/messages".parse().unwrap(),
            method: Method::POST,
            headers: HeaderMap::new(),
            body,
            upstream_id: Uuid::nil(),
            ttl: CacheTtl::Ttl5m,
        }
    }

    #[test]
    fn capture_rejects_too_large_body() {
        let body = Bytes::from(vec![b'a'; 100]);
        let err = RequestSnapshot::capture(
            "https://x/".parse().unwrap(),
            Method::POST,
            HeaderMap::new(),
            body,
            Uuid::nil(),
            CacheTtl::Ttl5m,
            10,
        )
        .unwrap_err();
        assert!(matches!(err, SnapshotError::TooLarge { .. }));
    }

    #[test]
    fn capture_rejects_non_json_body() {
        let body = Bytes::from_static(b"not-json");
        let err = RequestSnapshot::capture(
            "https://x/".parse().unwrap(),
            Method::POST,
            HeaderMap::new(),
            body,
            Uuid::nil(),
            CacheTtl::Ttl5m,
            1_000_000,
        )
        .unwrap_err();
        assert!(matches!(err, SnapshotError::InvalidJson));
    }

    #[test]
    fn build_keepalive_body_sets_max_tokens_zero_trims_messages_and_preserves_fields() {
        let original = json!({
            "model": "claude-haiku-4-5",
            "max_tokens": 4096,
            "stream": true,
            "thinking": {"type": "enabled", "budget_tokens": 1024},
            "output_config": {"format": {"type": "json_schema", "json_schema": {}}},
            "tool_choice": {"type": "any"},
            "system": [{"type": "text", "text": "hi", "cache_control": {"type": "ephemeral"}}],
            "messages": [{"role": "user", "content": "hello"}]
        });
        let body = Bytes::from(serde_json::to_vec(&original).unwrap());
        let snap = empty_snapshot(body);
        let new_body = snap.build_keepalive_body().unwrap();
        let round: Value = serde_json::from_slice(&new_body).unwrap();
        assert_eq!(round.get("max_tokens"), Some(&Value::from(0)));
        assert!(round.get("stream").is_none());
        assert_eq!(round.get("thinking"), original.get("thinking"));
        assert_eq!(round.get("output_config"), original.get("output_config"));
        assert_eq!(round.get("tool_choice"), original.get("tool_choice"));
        assert_eq!(round.get("system"), original.get("system"));
        assert_eq!(
            round.get("messages"),
            Some(&json!([{"role": "user", "content": "."}]))
        );
    }

    #[test]
    fn build_keepalive_body_preserves_adaptive_thinking_effort_and_tool_choice() {
        let original = json!({
            "model": "claude-opus-5-5",
            "max_tokens": 8193,
            "stream": true,
            "thinking": {
                "type": "adaptive",
                "display": "summarized",
                "block_binding": {"prefix_mismatch_behavior": "drop_block"}
            },
            "output_config": {
                "effort": "medium",
                "format": {"type": "json_schema"}
            },
            "tool_choice": {"type": "tool", "name": "bash"},
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": "cached",
                    "cache_control": {"type": "ephemeral"}
                }]
            }]
        });
        let snapshot = empty_snapshot(Bytes::from(serde_json::to_vec(&original).unwrap()));

        let round: Value =
            serde_json::from_slice(&snapshot.build_keepalive_body().unwrap()).unwrap();

        assert_eq!(round["thinking"], original["thinking"]);
        assert_eq!(round["output_config"], original["output_config"]);
        assert_eq!(round["tool_choice"], original["tool_choice"]);
        assert_eq!(round["max_tokens"], json!(0));
        assert!(round.get("stream").is_none());
    }

    #[test]
    fn build_keepalive_body_leaves_non_object_thinking() {
        for thinking in [Value::Null, Value::String("adaptive".to_owned())] {
            let original = json!({
                "model": "claude-opus-5-5",
                "thinking": thinking,
                "messages": [{
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": "cached",
                        "cache_control": {"type": "ephemeral"}
                    }]
                }]
            });
            let snapshot = empty_snapshot(Bytes::from(serde_json::to_vec(&original).unwrap()));

            let round: Value =
                serde_json::from_slice(&snapshot.build_keepalive_body().unwrap()).unwrap();

            assert_eq!(round["thinking"], original["thinking"]);
        }
    }

    #[test]
    fn capture_preserves_thinking_object_for_later_dispatch() {
        let original = json!({
            "model": "claude-opus-5-5",
            "thinking": {
                "type": "adaptive",
                "display": "summarized",
                "block_binding": {"prefix_mismatch_behavior": "drop_block"}
            },
            "output_config": {
                "effort": "medium",
                "format": {"type": "json_schema"}
            },
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": "cached",
                    "cache_control": {"type": "ephemeral"}
                }]
            }]
        });
        let body = Bytes::from(serde_json::to_vec(&original).unwrap());
        let snapshot = RequestSnapshot::capture(
            "https://api.anthropic.com/v1/messages".parse().unwrap(),
            Method::POST,
            HeaderMap::new(),
            body,
            Uuid::nil(),
            CacheTtl::Ttl5m,
            usize::MAX,
        )
        .unwrap();

        let captured: Value = serde_json::from_slice(&snapshot.body).unwrap();
        assert_eq!(captured["thinking"], original["thinking"]);
        assert_eq!(
            captured["output_config"]["format"],
            original["output_config"]["format"]
        );

        let round = RequestSnapshot::from_persisted(snapshot.to_persisted()).unwrap();
        let persisted: Value = serde_json::from_slice(&round.body).unwrap();
        assert_eq!(persisted["thinking"], original["thinking"]);
        assert_eq!(
            persisted["output_config"]["format"],
            original["output_config"]["format"]
        );

        let keepalive: Value =
            serde_json::from_slice(&round.build_keepalive_body().unwrap()).unwrap();
        assert_eq!(keepalive["thinking"], original["thinking"]);
        assert_eq!(keepalive["output_config"], original["output_config"]);
    }

    #[test]
    fn build_keepalive_body_preserves_auto_tool_choice() {
        let original = json!({
            "model": "m",
            "max_tokens": 8,
            "tool_choice": {"type": "auto"},
        });
        let body = Bytes::from(serde_json::to_vec(&original).unwrap());
        let new_body = empty_snapshot(body).build_keepalive_body().unwrap();
        let round: Value = serde_json::from_slice(&new_body).unwrap();
        assert_eq!(
            round.get("tool_choice").unwrap().get("type").unwrap(),
            &Value::String("auto".into())
        );
    }
    #[test]
    fn trims_messages_and_uncached_user_blocks_after_last_breakpoint() {
        let original = json!({
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "cached", "cache_control": {"type": "ephemeral"}},
                        {"type": "text", "text": "uncached"}
                    ]
                },
                {"role": "assistant", "content": [{"type": "text", "text": "trailing"}]}
            ]
        });
        let round: Value = serde_json::from_slice(
            &empty_snapshot(Bytes::from(serde_json::to_vec(&original).unwrap()))
                .build_keepalive_body()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            round["messages"],
            json!([{
                "role": "user",
                "content": [
                    {"type": "text", "text": "cached", "cache_control": {"type": "ephemeral"}}
                ]
            }])
        );
    }

    #[test]
    fn system_tools_breakpoints_trim_history_but_top_level_breakpoint_preserves_messages() {
        let system_only = json!({
            "system": [{"type": "text", "text": "system", "cache_control": {"type": "ephemeral"}}],
            "messages": [{"role": "user", "content": "old"}]
        });
        let round: Value = serde_json::from_slice(
            &empty_snapshot(Bytes::from(serde_json::to_vec(&system_only).unwrap()))
                .build_keepalive_body()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(round["messages"], json!([{"role": "user", "content": "."}]));

        let top_level = json!({
            "cache_control": {"type": "ephemeral"},
            "messages": [{"role": "user", "content": "old"}]
        });
        let round: Value = serde_json::from_slice(
            &empty_snapshot(Bytes::from(serde_json::to_vec(&top_level).unwrap()))
                .build_keepalive_body()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(round["messages"], top_level["messages"]);
    }

    #[test]
    fn assistant_breakpoints_end_with_user_and_preserve_tool_results() {
        let assistant = json!({
            "messages": [{
                "role": "assistant",
                "content": [{"type": "text", "text": "cached", "cache_control": {"type": "ephemeral"}}]
            }]
        });
        let round: Value = serde_json::from_slice(
            &empty_snapshot(Bytes::from(serde_json::to_vec(&assistant).unwrap()))
                .build_keepalive_body()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            round["messages"],
            json!([
                {
                    "role": "assistant",
                    "content": [{"type": "text", "text": "cached", "cache_control": {"type": "ephemeral"}}]
                },
                {"role": "user", "content": "."}
            ])
        );

        let with_tool_use = json!({
            "messages": [
                {
                    "role": "assistant",
                    "content": [{"type": "tool_use", "id": "t1", "name": "bash", "cache_control": {"type": "ephemeral"}}]
                },
                {
                    "role": "user",
                    "content": [{"type": "tool_result", "tool_use_id": "t1", "content": "ok"}]
                },
                {"role": "assistant", "content": "uncached"}
            ]
        });
        let round: Value = serde_json::from_slice(
            &empty_snapshot(Bytes::from(serde_json::to_vec(&with_tool_use).unwrap()))
                .build_keepalive_body()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            round["messages"],
            json!([
                {
                    "role": "assistant",
                    "content": [{"type": "tool_use", "id": "t1", "name": "bash", "cache_control": {"type": "ephemeral"}}]
                },
                {
                    "role": "user",
                    "content": [{"type": "tool_result", "tool_use_id": "t1", "content": "ok"}]
                }
            ])
        );

        let tool_results = json!({
            "messages": [
                {
                    "role": "assistant",
                    "content": [
                        {"type": "tool_use", "id": "t1", "name": "bash", "input": {}},
                        {"type": "tool_use", "id": "t2", "name": "bash", "input": {}}
                    ]
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "tool_result", "tool_use_id": "t1", "cache_control": {"type": "ephemeral"}},
                        {"type": "tool_result", "tool_use_id": "t2", "content": "needed"},
                        {"type": "tool_result", "tool_use_id": "t_extra", "content": "drop"},
                        {"type": "text", "text": "x".repeat(2_048)}
                    ]
                }
            ]
        });
        let round: Value = serde_json::from_slice(
            &empty_snapshot(Bytes::from(serde_json::to_vec(&tool_results).unwrap()))
                .build_keepalive_body()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            round["messages"][1]["content"],
            json!([
                {"type": "tool_result", "tool_use_id": "t1", "cache_control": {"type": "ephemeral"}},
                {"type": "tool_result", "tool_use_id": "t2", "content": "needed"}
            ])
        );

        let assistant_mixed_follow = json!({
            "messages": [
                {
                    "role": "assistant",
                    "content": [{"type": "tool_use", "id": "t1", "name": "bash", "cache_control": {"type": "ephemeral"}}]
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "tool_result", "tool_use_id": "t1", "content": "ok"},
                        {"type": "tool_result", "tool_use_id": "unmatched", "content": "drop"},
                        {"type": "text", "text": "x".repeat(2_048)}
                    ]
                }
            ]
        });
        let round: Value = serde_json::from_slice(
            &empty_snapshot(Bytes::from(
                serde_json::to_vec(&assistant_mixed_follow).unwrap(),
            ))
            .build_keepalive_body()
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            round["messages"][1]["content"],
            json!([{"type": "tool_result", "tool_use_id": "t1", "content": "ok"}])
        );

        let first_user_unrelated_tool_result = json!({
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "cached", "cache_control": {"type": "ephemeral"}},
                    {"type": "tool_result", "tool_use_id": "t_orphan", "content": "x".repeat(2_048)},
                    {"type": "text", "text": "x".repeat(2_048)}
                ]
            }]
        });
        let round: Value = serde_json::from_slice(
            &empty_snapshot(Bytes::from(
                serde_json::to_vec(&first_user_unrelated_tool_result).unwrap(),
            ))
            .build_keepalive_body()
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            round["messages"][0]["content"],
            json!([{"type": "text", "text": "cached", "cache_control": {"type": "ephemeral"}}])
        );

        let consumed_in_prefix_drops_trailing_tool_result = json!({
            "messages": [
                {
                    "role": "assistant",
                    "content": [{"type": "tool_use", "id": "t1", "name": "bash", "input": {}}]
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "tool_result", "tool_use_id": "t1", "cache_control": {"type": "ephemeral"}},
                        {"type": "tool_result", "tool_use_id": "t2", "content": "x".repeat(2_048)}
                    ]
                }
            ]
        });
        let round: Value = serde_json::from_slice(
            &empty_snapshot(Bytes::from(
                serde_json::to_vec(&consumed_in_prefix_drops_trailing_tool_result).unwrap(),
            ))
            .build_keepalive_body()
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            round["messages"][1]["content"],
            json!([{"type": "tool_result", "tool_use_id": "t1", "cache_control": {"type": "ephemeral"}}])
        );
    }

    #[test]
    fn capture_allows_raw_body_over_cap_when_uncached_suffix_is_trimmed() {
        let body = serde_json::to_vec(&json!({
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": "cached", "cache_control": {"type": "ephemeral"}}]
                },
                {"role": "assistant", "content": "x".repeat(2_000)}
            ]
        }))
        .unwrap();
        let max_bytes = 256;
        assert!(body.len() > max_bytes);
        let snapshot = RequestSnapshot::capture(
            "https://x/".parse().unwrap(),
            Method::POST,
            HeaderMap::new(),
            Bytes::from(body),
            Uuid::nil(),
            CacheTtl::Ttl5m,
            max_bytes,
        )
        .unwrap();
        assert!(snapshot.body.len() <= max_bytes);
    }

    #[test]
    fn persisted_snapshot_round_trips_without_debug_auth_material() {
        let mut headers = HeaderMap::new();
        headers.insert("anthropic-version", "2023-06-01".parse().unwrap());
        let snapshot = RequestSnapshot {
            url: "https://api.anthropic.com/v1/messages".parse().unwrap(),
            method: Method::POST,
            headers,
            body: Bytes::from_static(b"{\"model\":\"m\",\"max_tokens\":0}"),
            upstream_id: Uuid::from_u128(7),
            ttl: CacheTtl::Ttl5m,
        };

        let round = RequestSnapshot::from_persisted(snapshot.to_persisted()).unwrap();

        assert_eq!(round.url, snapshot.url);
        assert_eq!(round.method, snapshot.method);
        assert_eq!(round.headers, snapshot.headers);
        assert_eq!(round.body, snapshot.body);
        assert_eq!(round.upstream_id, snapshot.upstream_id);
        assert_eq!(round.ttl, snapshot.ttl);
    }

    #[test]
    fn persisted_snapshot_debug_redacts_body_and_header_values() {
        let snapshot = PersistedRequestSnapshot {
            url: "https://api.anthropic.com/v1/messages".to_owned(),
            method: "POST".to_owned(),
            headers: vec![PersistedHeader {
                name: "anthropic-beta".to_owned(),
                value: b"secret-beta-value".to_vec(),
            }],
            body: b"plaintext prompt".to_vec(),
            upstream_id: Uuid::from_u128(7),
            ttl: CacheTtl::Ttl5m,
        };

        let debug = format!("{snapshot:?}");

        assert!(!debug.contains("plaintext prompt"));
        assert!(!debug.contains("secret-beta-value"));
        assert!(debug.contains("redacted"));
    }
}
