use bytes::Bytes;
use http::{HeaderMap, Method};
use serde_json::{Map, Value};
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
        if body.len() > max_bytes {
            return Err(SnapshotError::TooLarge {
                actual: body.len(),
                max: max_bytes,
            });
        }
        let parsed: Value = sonic_rs::from_slice(&body).map_err(|_| SnapshotError::InvalidJson)?;
        if !parsed.is_object() {
            return Err(SnapshotError::NotAnObject);
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
}

fn transform_for_keepalive(map: &mut Map<String, Value>) {
    map.insert("max_tokens".into(), Value::from(0));
    map.remove("stream");
    if let Some(thinking) = map.get_mut("thinking")
        && let Some(thinking_map) = thinking.as_object_mut()
    {
        thinking_map.insert("type".into(), Value::String("disabled".into()));
        thinking_map.remove("budget_tokens");
    }
    if let Some(output_config) = map.get_mut("output_config")
        && let Some(output_map) = output_config.as_object_mut()
    {
        output_map.remove("format");
    }
    if let Some(tool_choice) = map.get("tool_choice").cloned()
        && let Some(kind) = tool_choice.get("type").and_then(Value::as_str)
        && matches!(kind, "tool" | "any")
    {
        map.insert(
            "tool_choice".into(),
            Value::Object(Map::from_iter([(
                "type".into(),
                Value::String("auto".into()),
            )])),
        );
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
    fn build_keepalive_body_sets_max_tokens_zero_and_strips_forbidden_fields() {
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
        assert_eq!(
            round.get("thinking").unwrap().get("type").unwrap(),
            &Value::String("disabled".into()),
        );
        assert!(round.get("output_config").unwrap().get("format").is_none());
        assert_eq!(
            round.get("tool_choice").unwrap().get("type").unwrap(),
            &Value::String("auto".into()),
        );
        assert_eq!(round.get("system"), original.get("system"));
        assert_eq!(round.get("messages"), original.get("messages"));
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
}
