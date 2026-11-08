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
    pub(crate) prefix_fingerprint: Option<String>,
    pub(crate) prefix_hash: Option<String>,
    pub(crate) prefix_policy_version: Option<String>,
    pub(crate) prefix_schema_version: Option<String>,
    keepalive_body: Bytes,
    keepalive_model: String,
    pub(crate) surface: Option<String>,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct PersistedRequestSnapshot {
    pub url: String,
    pub method: String,
    pub headers: Vec<PersistedHeader>,
    pub body: Vec<u8>,
    pub upstream_id: Uuid,
    pub ttl: CacheTtl,
    #[serde(default)]
    pub prefix_fingerprint: Option<String>,
    #[serde(default)]
    pub prefix_hash: Option<String>,
    #[serde(default)]
    pub prefix_policy_version: Option<String>,
    #[serde(default)]
    pub prefix_schema_version: Option<String>,
    #[serde(default)]
    pub surface: Option<String>,
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
        let value: Value = match sonic_rs::from_slice(&body) {
            Ok(value) => value,
            Err(_) if body.len() > max_bytes => {
                return Err(SnapshotError::TooLarge {
                    actual: body.len(),
                    max: max_bytes,
                });
            }
            Err(_) => return Err(SnapshotError::InvalidJson),
        };
        let map = value.as_object().ok_or(SnapshotError::NotAnObject)?;
        let messages = trim_uncached_messages_for_keepalive(map, &body)?;
        let body = if let Some(messages) = messages {
            Bytes::from(rewrite_object(&body, &[("messages", Some(&messages))])?)
        } else {
            body
        };
        if body.len() > max_bytes {
            return Err(SnapshotError::TooLarge {
                actual: body.len(),
                max: max_bytes,
            });
        }
        let keepalive_body = transform_for_keepalive(&body)?;
        let keepalive_model = value
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        Ok(Self {
            url,
            method,
            headers,
            body,
            upstream_id,
            ttl,
            prefix_fingerprint: None,
            prefix_hash: None,
            prefix_policy_version: None,
            prefix_schema_version: None,
            keepalive_body: Bytes::from(keepalive_body),
            keepalive_model,
            surface: None,
        })
    }

    /// Captures an OpenAI Responses request without applying Anthropic
    /// message trimming or cache-control rewriting.
    pub fn capture_openai(
        url: Url,
        method: Method,
        headers: HeaderMap,
        body: Bytes,
        upstream_id: Uuid,
        ttl: CacheTtl,
        max_bytes: usize,
    ) -> Result<Self, SnapshotError> {
        if body.len() > max_bytes {
            return Err(SnapshotError::TooLarge {
                actual: body.len(),
                max: max_bytes,
            });
        }
        let value: Value = sonic_rs::from_slice(&body).map_err(|_| SnapshotError::InvalidJson)?;
        if !value.is_object() {
            return Err(SnapshotError::NotAnObject);
        }
        let keepalive_body = rewrite_object(
            &body,
            &[
                ("max_output_tokens", Some(br#"16"#)),
                ("stream", Some(br#"false"#)),
            ],
        )?;
        let keepalive_model = value
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let surface = Some("responses".to_owned());
        Ok(Self {
            url,
            method,
            headers,
            body,
            upstream_id,
            ttl,
            prefix_fingerprint: None,
            prefix_hash: None,
            prefix_policy_version: None,
            prefix_schema_version: None,
            keepalive_body: Bytes::from(keepalive_body),
            keepalive_model,
            surface,
        })
    }
    pub(crate) fn with_prefix_evidence(
        mut self,
        prefix_fingerprint: Option<String>,
        prefix_hash: Option<String>,
        prefix_policy_version: Option<String>,
        prefix_schema_version: Option<String>,
    ) -> Self {
        self.prefix_fingerprint = prefix_fingerprint;
        self.prefix_hash = prefix_hash;
        self.prefix_policy_version = prefix_policy_version;
        self.prefix_schema_version = prefix_schema_version;
        self
    }

    pub fn keepalive_model(&self) -> &str {
        &self.keepalive_model
    }

    pub fn prefix_fingerprint(&self) -> Option<&str> {
        self.prefix_fingerprint.as_deref()
    }

    pub fn prefix_hash(&self) -> Option<&str> {
        self.prefix_hash.as_deref()
    }

    pub fn prefix_policy_version(&self) -> Option<&str> {
        self.prefix_policy_version.as_deref()
    }

    pub fn prefix_schema_version(&self) -> Option<&str> {
        self.prefix_schema_version.as_deref()
    }

    pub fn build_keepalive_body(&self) -> Result<Bytes, SnapshotError> {
        Ok(self.keepalive_body.clone())
    }
    pub fn cache_input_token_estimate(&self) -> u64 {
        if self.surface.as_deref() == Some("responses") {
            self.body.len().saturating_div(4) as u64
        } else {
            4_000
        }
    }
    pub fn surface(&self) -> Option<&str> {
        self.surface.as_deref()
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
            prefix_fingerprint: self.prefix_fingerprint.clone(),
            prefix_hash: self.prefix_hash.clone(),
            prefix_policy_version: self.prefix_policy_version.clone(),
            prefix_schema_version: self.prefix_schema_version.clone(),
            surface: self.surface.clone(),
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
        let prefix_fingerprint = persisted.prefix_fingerprint;
        let prefix_hash = persisted.prefix_hash;
        let prefix_policy_version = persisted.prefix_policy_version;
        let prefix_schema_version = persisted.prefix_schema_version;
        let surface = persisted.surface;
        let body = Bytes::from(persisted.body);
        let mut snapshot = if surface.as_deref() == Some("responses") {
            Self::capture_openai(
                url,
                method,
                headers,
                body,
                persisted.upstream_id,
                persisted.ttl,
                usize::MAX,
            )?
        } else {
            Self::capture(
                url,
                method,
                headers,
                body,
                persisted.upstream_id,
                persisted.ttl,
                usize::MAX,
            )?
        };
        snapshot.surface = surface;
        Ok(snapshot.with_prefix_evidence(
            prefix_fingerprint,
            prefix_hash,
            prefix_policy_version,
            prefix_schema_version,
        ))
    }
}

/// Shapes a captured request without reserializing prompt-cache prefix values.
/// Only `max_tokens` and `stream` change; all other values retain their raw JSON.
fn transform_for_keepalive(body: &[u8]) -> Result<Vec<u8>, SnapshotError> {
    rewrite_object(body, &[("max_tokens", Some(b"0")), ("stream", None)])
}

/// Rewrites selected fields in source order, copying every other value verbatim.
/// A mutable sonic object uses a hash map, so it cannot preserve wire key order.
fn rewrite_object<const N: usize>(
    raw: &[u8],
    replacements: &[(&str, Option<&[u8]>); N],
) -> Result<Vec<u8>, SnapshotError> {
    let mut output = Vec::with_capacity(raw.len());
    output.push(b'{');
    let mut written = false;
    let mut seen = [false; N];
    for field in sonic_rs::to_object_iter(raw) {
        let (key, value) = field.map_err(|_| SnapshotError::InvalidJson)?;
        let replacement = replacements.iter().position(|(name, _)| *name == key);
        let value = if let Some(index) = replacement {
            seen[index] = true;
            let Some(value) = replacements[index].1 else {
                continue;
            };
            value
        } else {
            value.as_raw_str().as_bytes()
        };
        if written {
            output.push(b',');
        }
        written = true;
        serde_json::to_writer(&mut output, key.as_ref()).map_err(|_| SnapshotError::InvalidJson)?;
        output.push(b':');
        output.extend_from_slice(value);
    }
    for ((key, value), seen) in replacements.iter().zip(seen) {
        if !seen && let Some(value) = value {
            if written {
                output.push(b',');
            }
            written = true;
            serde_json::to_writer(&mut output, key).map_err(|_| SnapshotError::InvalidJson)?;
            output.push(b':');
            output.extend_from_slice(value);
        }
    }
    output.push(b'}');
    Ok(output)
}

const MINIMAL_USER_MESSAGE: &str = r#"{"role":"user","content":"."}"#;
const MINIMAL_MESSAGES: &[u8] = br#"[{"role":"user","content":"."}]"#;

fn raw_array(raw: &[u8]) -> Result<Vec<sonic_rs::LazyValue<'_>>, SnapshotError> {
    sonic_rs::to_array_iter(raw)
        .collect::<Result<_, _>>()
        .map_err(|_| SnapshotError::InvalidJson)
}

fn raw_field<'a>(raw: &'a [u8], name: &str) -> Result<sonic_rs::LazyValue<'a>, SnapshotError> {
    let mut found = None;
    for field in sonic_rs::to_object_iter(raw) {
        let (key, value) = field.map_err(|_| SnapshotError::InvalidJson)?;
        if key == name {
            found = Some(value);
        }
    }
    // Match serde_json's last-value-wins semantics for duplicate object keys.
    found.ok_or(SnapshotError::InvalidJson)
}

fn serialize_raw_array(values: &[&str]) -> Vec<u8> {
    let mut output = Vec::with_capacity(
        2 + values.iter().map(|value| value.len()).sum::<usize>() + values.len().saturating_sub(1),
    );
    output.push(b'[');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push(b',');
        }
        output.extend_from_slice(value.as_bytes());
    }
    output.push(b']');
    output
}

fn trim_uncached_messages_for_keepalive(
    map: &Map<String, Value>,
    body: &[u8],
) -> Result<Option<Vec<u8>>, SnapshotError> {
    if map.get("cache_control").is_some() {
        return Ok(None);
    }

    let explicit_system_or_tools = ["system", "tools"]
        .iter()
        .filter_map(|key| map.get(*key))
        .any(value_has_cache_control);
    let Some(messages) = map.get("messages").and_then(Value::as_array) else {
        return Ok(explicit_system_or_tools.then(|| MINIMAL_MESSAGES.to_vec()));
    };
    let last_msg_idx = messages.iter().rposition(message_has_cache_control);
    let Some(last_msg_idx) = last_msg_idx else {
        if !explicit_system_or_tools {
            return Ok(None);
        }
        let already_minimal = messages.len() == 1
            && messages[0] == serde_json::json!({"role": "user", "content": "."});
        return Ok((!already_minimal).then(|| MINIMAL_MESSAGES.to_vec()));
    };

    let keep_len;
    let mut append_minimal = false;
    let mut content_replacement = None;
    match messages[last_msg_idx].get("role").and_then(Value::as_str) {
        Some("user") => {
            keep_len = last_msg_idx + 1;
            let mut required_ids = assistant_tool_use_ids(
                last_msg_idx
                    .checked_sub(1)
                    .and_then(|index| messages.get(index)),
            );
            let message = &messages[last_msg_idx];
            if message.get("cache_control").is_none()
                && let Some(content) = message.get("content").and_then(Value::as_array)
                && let Some(last_blk_idx) = content
                    .iter()
                    .rposition(|block| block.get("cache_control").is_some())
                && content.len() > last_blk_idx + 1
            {
                for block in &content[..=last_blk_idx] {
                    if block.get("type").and_then(Value::as_str) == Some("tool_result")
                        && let Some(id) = block.get("tool_use_id").and_then(Value::as_str)
                    {
                        required_ids.retain(|required| required != id);
                    }
                }
                let mut kept: Vec<usize> = (0..=last_blk_idx).collect();
                kept.extend(
                    (last_blk_idx + 1..content.len())
                        .filter(|&index| matching_tool_result(&content[index], &mut required_ids)),
                );
                if kept.len() != content.len() {
                    content_replacement = Some((last_msg_idx, kept));
                }
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
                let next = last_msg_idx + 1;
                let content = messages[next].get("content").and_then(Value::as_array);
                let kept: Vec<usize> = content
                    .map(|content| {
                        content
                            .iter()
                            .enumerate()
                            .filter(|(_, block)| matching_tool_result(block, &mut required_ids))
                            .map(|(index, _)| index)
                            .collect()
                    })
                    .unwrap_or_default();
                if kept.is_empty() {
                    keep_len = last_msg_idx + 1;
                    append_minimal = true;
                } else {
                    keep_len = last_msg_idx + 2;
                    if kept.len() != content.expect("nonempty results have content").len() {
                        content_replacement = Some((next, kept));
                    }
                }
            } else {
                keep_len = last_msg_idx + 1;
                append_minimal = true;
            }
        }
        _ => {
            keep_len = last_msg_idx + 1;
        }
    }
    if keep_len == messages.len() && !append_minimal && content_replacement.is_none() {
        return Ok(None);
    }

    let raw_messages = raw_field(body, "messages")?;
    let raw_messages = raw_array(raw_messages.as_raw_str().as_bytes())?;
    let replaced_message = if let Some((index, kept)) = &content_replacement {
        let raw_content = raw_field(raw_messages[*index].as_raw_str().as_bytes(), "content")?;
        let raw_content = raw_array(raw_content.as_raw_str().as_bytes())?;
        let content = serialize_raw_array(
            &kept
                .iter()
                .map(|&index| raw_content[index].as_raw_str())
                .collect::<Vec<_>>(),
        );
        Some(rewrite_object(
            raw_messages[*index].as_raw_str().as_bytes(),
            &[("content", Some(&content))],
        )?)
    } else {
        None
    };
    let mut kept: Vec<&str> = raw_messages[..keep_len]
        .iter()
        .map(sonic_rs::LazyValue::as_raw_str)
        .collect();
    if let Some((index, _)) = content_replacement {
        kept[index] = std::str::from_utf8(replaced_message.as_ref().expect("content was replaced"))
            .map_err(|_| SnapshotError::InvalidJson)?;
    }
    if append_minimal {
        kept.push(MINIMAL_USER_MESSAGE);
    }
    Ok(Some(serialize_raw_array(&kept)))
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

fn matching_tool_result(block: &Value, required_ids: &mut Vec<String>) -> bool {
    if block.get("type").and_then(Value::as_str) == Some("tool_result")
        && let Some(id) = block.get("tool_use_id").and_then(Value::as_str)
        && let Some(index) = required_ids.iter().position(|required| required == id)
    {
        required_ids.swap_remove(index);
        return true;
    }
    false
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
        RequestSnapshot::capture(
            "https://api.anthropic.com/v1/messages".parse().unwrap(),
            Method::POST,
            HeaderMap::new(),
            body,
            Uuid::nil(),
            CacheTtl::Ttl5m,
            usize::MAX,
        )
        .unwrap()
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
        let snapshot = RequestSnapshot::capture(
            "https://api.anthropic.com/v1/messages".parse().unwrap(),
            Method::POST,
            headers,
            Bytes::from_static(b"{\"model\":\"m\",\"max_tokens\":0}"),
            Uuid::from_u128(7),
            CacheTtl::Ttl5m,
            usize::MAX,
        )
        .unwrap();

        let round = RequestSnapshot::from_persisted(snapshot.to_persisted()).unwrap();

        assert_eq!(round.url, snapshot.url);
        assert_eq!(round.method, snapshot.method);
        assert_eq!(round.headers, snapshot.headers);
        assert_eq!(round.body, snapshot.body);
        assert_eq!(round.upstream_id, snapshot.upstream_id);
        assert_eq!(round.ttl, snapshot.ttl);

        assert_eq!(round.keepalive_model(), "m");
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
            prefix_fingerprint: None,
            prefix_hash: None,
            prefix_policy_version: None,
            prefix_schema_version: None,
            surface: None,
        };
        let debug = format!("{snapshot:?}");

        assert!(!debug.contains("plaintext prompt"));
        assert!(!debug.contains("secret-beta-value"));
        assert!(debug.contains("redacted"));
    }

    const ORDERED_NO_TRIM: &str = r#"{"model":"m","stream":true,"tools":[{"name":"t","input_schema":{"type":"object","properties":{"z":{"type":"string"},"a":{"type":"string"}}},"cache_control":{"type":"ephemeral"}}],"thinking":{"type":"enabled","budget_tokens":1024},"output_config":{"format":{"type":"json_schema","json_schema":{}},"effort":"low"},"tool_choice":{"type":"auto","disable_parallel_tool_use":false},"system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral","ttl":"1h"}}],"messages":[{"role":"user","content":[{"type":"text","text":"x","cache_control":{"type":"ephemeral","ttl":"1h"}}]}],"max_tokens":8}"#;
    const ORDERED_NO_TRIM_KEEPALIVE: &str = r#"{"model":"m","tools":[{"name":"t","input_schema":{"type":"object","properties":{"z":{"type":"string"},"a":{"type":"string"}}},"cache_control":{"type":"ephemeral"}}],"thinking":{"type":"enabled","budget_tokens":1024},"output_config":{"format":{"type":"json_schema","json_schema":{}},"effort":"low"},"tool_choice":{"type":"auto","disable_parallel_tool_use":false},"system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral","ttl":"1h"}}],"messages":[{"role":"user","content":[{"type":"text","text":"x","cache_control":{"type":"ephemeral","ttl":"1h"}}]}],"max_tokens":0}"#;
    const ORDERED_TRIM: &str = r#"{"model":"m","stream":true,"thinking":{"type":"enabled","budget_tokens":1024},"system":[{"type":"text","text":"s"}],"messages":[{"role":"user","content":[{"type":"text","text":"x","cache_control":{"type":"ephemeral","ttl":"1h"}}]},{"role":"assistant","content":[{"type":"text","text":"uncached tail"}]}],"max_tokens":8}"#;
    const ORDERED_TRIM_BODY: &str = r#"{"model":"m","stream":true,"thinking":{"type":"enabled","budget_tokens":1024},"system":[{"type":"text","text":"s"}],"messages":[{"role":"user","content":[{"type":"text","text":"x","cache_control":{"type":"ephemeral","ttl":"1h"}}]}],"max_tokens":8}"#;
    const ORDERED_TRIM_KEEPALIVE: &str = r#"{"model":"m","thinking":{"type":"enabled","budget_tokens":1024},"system":[{"type":"text","text":"s"}],"messages":[{"role":"user","content":[{"type":"text","text":"x","cache_control":{"type":"ephemeral","ttl":"1h"}}]}],"max_tokens":0}"#;

    fn capture_raw(raw: &str) -> RequestSnapshot {
        empty_snapshot(Bytes::from(raw.to_owned()))
    }

    fn assert_bytes_eq(actual: &[u8], expected: &str, what: &str) {
        assert_eq!(
            std::str::from_utf8(actual).unwrap(),
            expected,
            "{what}: JSON object key order must match the live request"
        );
    }

    #[test]
    fn keepalive_body_preserves_raw_key_order_without_trim() {
        let snapshot = capture_raw(ORDERED_NO_TRIM);
        assert_bytes_eq(&snapshot.body, ORDERED_NO_TRIM, "captured body");
        assert_bytes_eq(
            &snapshot.build_keepalive_body().unwrap(),
            ORDERED_NO_TRIM_KEEPALIVE,
            "keepalive body",
        );
    }

    #[test]
    fn captured_and_keepalive_bodies_preserve_raw_key_order_with_trim() {
        let snapshot = capture_raw(ORDERED_TRIM);
        assert_bytes_eq(&snapshot.body, ORDERED_TRIM_BODY, "trimmed body");
        assert_bytes_eq(
            &snapshot.build_keepalive_body().unwrap(),
            ORDERED_TRIM_KEEPALIVE,
            "keepalive body",
        );
    }

    #[test]
    fn persisted_round_trip_preserves_raw_key_order() {
        for (raw, body, keepalive) in [
            (ORDERED_NO_TRIM, ORDERED_NO_TRIM, ORDERED_NO_TRIM_KEEPALIVE),
            (ORDERED_TRIM, ORDERED_TRIM_BODY, ORDERED_TRIM_KEEPALIVE),
        ] {
            let persisted = capture_raw(raw).to_persisted();
            assert_bytes_eq(&persisted.body, body, "persisted body");
            let round = RequestSnapshot::from_persisted(persisted).unwrap();
            assert_bytes_eq(&round.body, body, "round-trip body");
            assert_bytes_eq(
                &round.build_keepalive_body().unwrap(),
                keepalive,
                "round-trip keepalive body",
            );
        }
    }

    #[test]
    fn raw_user_content_trim_preserves_required_results_and_value_encoding() {
        let raw = r#"{"model":"m","messages":[{"role":"assistant","content":[{"type":"tool_use","id":"t1"},{"type":"tool_use","id":"t2"}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","cache_control":{"ttl":"1h","type":"ephemeral"}},{"type":"tool_result","tool_use_id":"extra","content":"drop"},{"type":"tool_result","tool_use_id":"t2","content":{"z":1e+04,"a":"\u0061"}},{"type":"text","text":"drop"}],"metadata":{"z":2,"a":1}}],"stream":true,"max_tokens":8}"#;
        let expected = r#"{"model":"m","messages":[{"role":"assistant","content":[{"type":"tool_use","id":"t1"},{"type":"tool_use","id":"t2"}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","cache_control":{"ttl":"1h","type":"ephemeral"}},{"type":"tool_result","tool_use_id":"t2","content":{"z":1e+04,"a":"\u0061"}}],"metadata":{"z":2,"a":1}}],"stream":true,"max_tokens":8}"#;
        let snapshot = capture_raw(raw);
        assert_bytes_eq(&snapshot.body, expected, "required tool results");
        let keepalive = r#"{"model":"m","messages":[{"role":"assistant","content":[{"type":"tool_use","id":"t1"},{"type":"tool_use","id":"t2"}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","cache_control":{"ttl":"1h","type":"ephemeral"}},{"type":"tool_result","tool_use_id":"t2","content":{"z":1e+04,"a":"\u0061"}}],"metadata":{"z":2,"a":1}}],"max_tokens":0}"#;
        assert_bytes_eq(
            &snapshot.build_keepalive_body().unwrap(),
            keepalive,
            "required tool results keepalive",
        );
        let round = RequestSnapshot::from_persisted(snapshot.to_persisted()).unwrap();
        assert_bytes_eq(&round.body, expected, "required results round-trip");
    }

    #[test]
    fn raw_assistant_content_trim_preserves_following_user_field_order() {
        let raw = r#"{"max_tokens":8,"stream":true,"messages":[{"content":[{"id":"t1","type":"tool_use","cache_control":{"type":"ephemeral"}}],"role":"assistant"},{"metadata":{"z":2,"a":1},"content":[{"type":"text","text":"drop"},{"tool_use_id":"t1","type":"tool_result","content":{"z":-0.0,"a":"\/"}},{"type":"tool_result","tool_use_id":"extra"}],"role":"user"},{"role":"assistant","content":"drop"}],"model":"m"}"#;
        let expected = r#"{"max_tokens":8,"stream":true,"messages":[{"content":[{"id":"t1","type":"tool_use","cache_control":{"type":"ephemeral"}}],"role":"assistant"},{"metadata":{"z":2,"a":1},"content":[{"tool_use_id":"t1","type":"tool_result","content":{"z":-0.0,"a":"\/"}}],"role":"user"}],"model":"m"}"#;
        let snapshot = capture_raw(raw);
        assert_bytes_eq(&snapshot.body, expected, "assistant tool results");
        let keepalive = r#"{"max_tokens":0,"messages":[{"content":[{"id":"t1","type":"tool_use","cache_control":{"type":"ephemeral"}}],"role":"assistant"},{"metadata":{"z":2,"a":1},"content":[{"tool_use_id":"t1","type":"tool_result","content":{"z":-0.0,"a":"\/"}}],"role":"user"}],"model":"m"}"#;
        assert_bytes_eq(
            &snapshot.build_keepalive_body().unwrap(),
            keepalive,
            "assistant tool results keepalive",
        );
    }

    #[test]
    fn raw_minimal_user_insertion_preserves_existing_fields_and_appends_missing_fields() {
        for (raw, expected) in [
            (
                r#"{"system":[{"text":"s","type":"text","cache_control":{"type":"ephemeral"}}],"stream":true,"model":"m"}"#,
                r#"{"system":[{"text":"s","type":"text","cache_control":{"type":"ephemeral"}}],"model":"m","messages":[{"role":"user","content":"."}],"max_tokens":0}"#,
            ),
            (
                r#"{"messages":null,"stream":true,"tools":[{"cache_control":{"type":"ephemeral"},"name":"t"}],"model":"m"}"#,
                r#"{"messages":[{"role":"user","content":"."}],"tools":[{"cache_control":{"type":"ephemeral"},"name":"t"}],"model":"m","max_tokens":0}"#,
            ),
            (
                r#"{"model":"m","messages":[{"content":[{"text":"s","cache_control":{"type":"ephemeral"},"type":"text"}],"role":"assistant"}],"stream":true}"#,
                r#"{"model":"m","messages":[{"content":[{"text":"s","cache_control":{"type":"ephemeral"},"type":"text"}],"role":"assistant"},{"role":"user","content":"."}],"max_tokens":0}"#,
            ),
        ] {
            let snapshot = capture_raw(raw);
            assert_bytes_eq(
                &snapshot.build_keepalive_body().unwrap(),
                expected,
                "minimal user insertion",
            );
            let round = RequestSnapshot::from_persisted(snapshot.to_persisted()).unwrap();
            assert_bytes_eq(
                &round.build_keepalive_body().unwrap(),
                expected,
                "minimal user insertion round-trip",
            );
        }
    }

    #[test]
    fn raw_trimmed_snapshot_enforces_exact_stored_byte_limit() {
        for (max_bytes, accepted) in [
            (ORDERED_TRIM_BODY.len(), true),
            (ORDERED_TRIM_BODY.len() - 1, false),
        ] {
            let result = RequestSnapshot::capture(
                "https://x/".parse().unwrap(),
                Method::POST,
                HeaderMap::new(),
                Bytes::from_static(ORDERED_TRIM.as_bytes()),
                Uuid::nil(),
                CacheTtl::Ttl5m,
                max_bytes,
            );
            if accepted {
                assert_bytes_eq(&result.unwrap().body, ORDERED_TRIM_BODY, "at byte limit");
            } else {
                assert!(matches!(
                    result,
                    Err(SnapshotError::TooLarge { actual, max })
                        if actual == ORDERED_TRIM_BODY.len() && max == max_bytes
                ));
            }
        }
    }
}
