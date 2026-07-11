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

    #[test]
    fn persisted_snapshot_round_trips_without_debug_auth_material() {
        let mut headers = HeaderMap::new();
        headers.insert("anthropic-version", "2023-06-01".parse().unwrap());
        let snapshot = RequestSnapshot {
            url: "https://api.anthropic.com/v1/messages".parse().unwrap(),
            method: Method::POST,
            headers,
            body: Bytes::from_static(b"{\"model\":\"m\",\"max_tokens\":1}"),
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
