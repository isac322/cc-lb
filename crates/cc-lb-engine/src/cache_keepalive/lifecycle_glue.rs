use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_control::dynamic_view::DynamicViewHolder;
use cc_lb_plugin_api::Principal;
use http::HeaderMap;
use serde_json::Value;
use sha2::{Digest, Sha256};
use url::Url;
use uuid::Uuid;

use cc_lb_storage_api::CacheTtl;

use crate::lifecycle::RequestCacheMetadata;

use super::{CancelReason, HeuristicClassifier, RequestSnapshot, SessionKey};

#[derive(Clone)]
pub struct CacheKeepaliveEnqueueRequest {
    pub session_key_hash: String,
    pub principal_id: String,
    pub cache_anchor_age: Duration,
    pub params: super::ScheduleParams,
    pub snapshot: RequestSnapshot,
}

#[derive(Clone, Debug)]
pub struct CacheKeepaliveCancelRequest {
    pub session_key_hash: String,
    pub reason: CancelReason,
}

impl fmt::Debug for CacheKeepaliveEnqueueRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CacheKeepaliveEnqueueRequest")
            .field("session_key_hash", &self.session_key_hash)
            .field("principal_id", &self.principal_id)
            .field("cache_anchor_age", &self.cache_anchor_age)
            .field("params", &self.params)
            .field("snapshot", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("cache keepalive enqueue failed: {0}")]
pub struct CacheKeepaliveEnqueueError(pub String);

#[async_trait]
pub trait CacheKeepaliveEnqueuer: Send + Sync {
    async fn enqueue_cache_keepalive(
        &self,
        request: CacheKeepaliveEnqueueRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError>;

    async fn cancel_cache_keepalive(
        &self,
        request: CacheKeepaliveCancelRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError>;
}

#[derive(Clone)]
pub(crate) struct LifecycleKeepaliveContext {
    pub(crate) principal: Principal,
    pub(crate) cache_metadata: RequestCacheMetadata,
    pub(crate) upstream_id: Uuid,
    pub(crate) shaped_body: Bytes,
    pub(crate) downstream_headers: HeaderMap,
}

#[derive(Clone)]
pub(crate) struct LifecycleKeepalive {
    enqueuer: Option<Arc<dyn CacheKeepaliveEnqueuer>>,
    dynamic_view: Arc<DynamicViewHolder>,
}

impl LifecycleKeepalive {
    pub(crate) fn new(
        enqueuer: Option<Arc<dyn CacheKeepaliveEnqueuer>>,
        dynamic_view: Arc<DynamicViewHolder>,
    ) -> Self {
        Self {
            enqueuer,
            dynamic_view,
        }
    }

    /// Detaches classification and the serial keep-alive storage writes onto a
    /// background task so the client response path never awaits them.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_response_completed(
        &self,
        response_body_json: &serde_json::Value,
        principal: &Principal,
        cache_metadata: &RequestCacheMetadata,
        upstream_id: uuid::Uuid,
        shaped_body: Bytes,
        downstream_headers: &HeaderMap,
        cache_anchor_age: Duration,
    ) {
        if self.enqueuer.is_none() {
            return;
        }
        let this = self.clone();
        let response_body_json = response_body_json.clone();
        let principal = principal.clone();
        let cache_metadata = cache_metadata.clone();
        let downstream_headers = downstream_headers.clone();
        tokio::spawn(async move {
            this.persist_completion(
                &response_body_json,
                &principal,
                &cache_metadata,
                upstream_id,
                shaped_body,
                &downstream_headers,
                cache_anchor_age,
            )
            .await;
        });
    }

    #[allow(clippy::too_many_arguments)]
    async fn persist_completion(
        &self,
        response_body_json: &serde_json::Value,
        principal: &Principal,
        cache_metadata: &RequestCacheMetadata,
        upstream_id: uuid::Uuid,
        shaped_body: Bytes,
        downstream_headers: &HeaderMap,
        cache_anchor_age: Duration,
    ) {
        let Some(enqueuer) = self.enqueuer.as_ref() else {
            return;
        };
        let view = self.dynamic_view.load();
        let Some(cached) = view.principal_view.get(&principal.id) else {
            return;
        };
        let Some(config) = cached.cache_keepalive().filter(|config| config.enabled) else {
            return;
        };
        let Some(request_json) = cache_metadata.request_json.as_ref() else {
            return;
        };
        let Some(first_breakpoint) = cache_metadata.cache_breakpoints.first() else {
            return;
        };
        let ttl = CacheTtl::from_ttl_str(first_breakpoint.ttl.as_deref());
        let Some(session_key) = cache_keepalive_session_key(principal, cache_metadata) else {
            return;
        };
        let principal_name: Arc<str> = Arc::from(principal.id.as_str());
        let url =
            Url::parse(KEEPALIVE_SNAPSHOT_URL).expect("cache keep-alive snapshot URL is valid");
        let snapshot = match RequestSnapshot::capture(
            url,
            http::Method::POST,
            downstream_auth_headers(downstream_headers),
            shaped_body,
            upstream_id,
            ttl,
            config.snapshot_max_bytes as usize,
        ) {
            Ok(snapshot) => snapshot,
            Err(super::SnapshotError::TooLarge { actual, max }) => {
                tracing::warn!(
                    target: "cache_keepalive",
                    principal_id = principal.id.as_str(),
                    actual,
                    max,
                    "skipping cache keep-alive snapshot: shaped body exceeds cap"
                );
                super::record_cancelled(&principal_name, CancelReason::SnapshotTooLarge);
                return;
            }
            Err(error) => {
                tracing::warn!(
                    target: "cache_keepalive",
                    principal_id = principal.id.as_str(),
                    %error,
                    "skipping cache keep-alive snapshot capture"
                );
                return;
            }
        };
        let classifier = HeuristicClassifier::new(&config.classifier);
        match classifier.classify(request_json, response_body_json) {
            super::TurnDecision::AgentInTurn => {
                let params =
                    super::ScheduleParams::from_cache_anchor_age(config, ttl, cache_anchor_age);
                if let Err(error) = enqueuer
                    .enqueue_cache_keepalive(CacheKeepaliveEnqueueRequest {
                        session_key_hash: session_key.to_string(),
                        principal_id: principal.id.clone(),
                        cache_anchor_age,
                        params,
                        snapshot,
                    })
                    .await
                {
                    tracing::warn!(
                        target: "cache_keepalive",
                        principal_id = principal.id.as_str(),
                        %error,
                        "durable cache keep-alive enqueue failed"
                    );
                }
            }
            super::TurnDecision::UserTurn => {
                self.cancel_session(&session_key, principal, CancelReason::UserTurnDetected)
                    .await;
            }
            super::TurnDecision::Ambiguous if config.classifier.llm_judge.is_some() => {
                tracing::warn!(
                    target: "cache_keepalive",
                    principal_id = principal.id.as_str(),
                    "cache keep-alive llm_judge is configured but unsupported in this release; treating ambiguous response as user turn"
                );
                self.cancel_session(&session_key, principal, CancelReason::UserTurnDetected)
                    .await;
            }
            super::TurnDecision::Ambiguous => {
                self.cancel_session(&session_key, principal, CancelReason::UserTurnDetected)
                    .await;
            }
        }
    }

    async fn cancel_session(
        &self,
        session_key: &SessionKey,
        principal: &Principal,
        reason: CancelReason,
    ) {
        if let Some(enqueuer) = self.enqueuer.as_ref()
            && let Err(error) = enqueuer
                .cancel_cache_keepalive(CacheKeepaliveCancelRequest {
                    session_key_hash: session_key.to_string(),
                    reason,
                })
                .await
        {
            tracing::warn!(
                target: "cache_keepalive",
                principal_id = principal.id.as_str(),
                reason = reason.as_str(),
                %error,
                "durable cache keep-alive cancellation failed"
            );
        }
    }
}

#[derive(Default)]
pub(crate) struct StreamingKeepaliveResponse {
    message: Option<Value>,
    content_blocks: Vec<Value>,
    stop_reason: Option<String>,
}

impl StreamingKeepaliveResponse {
    pub(crate) fn observe(&mut self, event_name: Option<&[u8]>, raw: &[u8]) {
        match event_name {
            Some(b"message_start") => {
                if let Some(value) = sse_json_value(raw)
                    && let Some(message) = value.get("message")
                {
                    self.message = Some(message.clone());
                }
            }
            Some(b"content_block_start") => {
                if let Some(value) = sse_json_value(raw)
                    && let Some(block) = value.get("content_block")
                {
                    self.content_blocks.push(block.clone());
                }
            }
            Some(b"message_delta") => {
                if let Some(value) = sse_json_value(raw)
                    && let Some(stop_reason) = value
                        .get("delta")
                        .and_then(|delta| delta.get("stop_reason"))
                        .and_then(Value::as_str)
                {
                    self.stop_reason = Some(stop_reason.to_owned());
                }
            }
            Some(b"message_stop") => {}
            _ => {}
        }
    }

    pub(crate) fn into_value(self) -> Option<Value> {
        let mut value = self.message?;
        let map = value.as_object_mut()?;
        if !self.content_blocks.is_empty() {
            map.insert("content".to_owned(), Value::Array(self.content_blocks));
        }
        if let Some(stop_reason) = self.stop_reason {
            map.insert("stop_reason".to_owned(), Value::String(stop_reason));
        }
        Some(value)
    }
}

fn sse_json_value(raw: &[u8]) -> Option<Value> {
    let text = std::str::from_utf8(raw).ok()?;
    for line in text.lines() {
        let Some(payload) = line.strip_prefix("data:").map(str::trim_start) else {
            continue;
        };
        if let Ok(value) = sonic_rs::from_str::<Value>(payload) {
            return Some(value);
        }
    }
    None
}

const KEEPALIVE_FORWARD_HEADERS: &[&str] = &["anthropic-version", "anthropic-beta"];

fn downstream_auth_headers(headers: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::new();
    for name in KEEPALIVE_FORWARD_HEADERS {
        if let Some(value) = headers.get(*name)
            && let Ok(name) = http::HeaderName::from_bytes(name.as_bytes())
        {
            out.insert(name, value.clone());
        }
    }
    out
}

fn cache_keepalive_session_key(
    principal: &Principal,
    cache_metadata: &RequestCacheMetadata,
) -> Option<SessionKey> {
    let principal_id = uuid::Uuid::parse_str(&principal.id)
        .unwrap_or_else(|_| cache_keepalive_principal_uuid(&principal.id));
    if let Some(thread_id) = cache_metadata.thread_id.clone() {
        return Some(SessionKey::from_thread_id(principal_id, thread_id));
    }
    cache_metadata
        .cache_prefix_hash
        .clone()
        .map(|prefix_hash| SessionKey::from_cache_prefix_hash(principal_id, prefix_hash))
}

fn cache_keepalive_principal_uuid(principal_id: &str) -> Uuid {
    let digest = Sha256::digest(principal_id.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

const KEEPALIVE_SNAPSHOT_URL: &str = "https://api.anthropic.com/v1/messages";

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn downstream_auth_headers_preserves_protocol_headers_without_auth_secrets() {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_static("secret-key"));
        headers.insert("authorization", HeaderValue::from_static("Bearer secret"));
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        headers.insert("anthropic-beta", HeaderValue::from_static("prompt-caching"));

        let forwarded = downstream_auth_headers(&headers);

        assert!(forwarded.get("x-api-key").is_none());
        assert!(forwarded.get("authorization").is_none());
        assert_eq!(
            forwarded.get("anthropic-version"),
            Some(&HeaderValue::from_static("2023-06-01"))
        );
        assert_eq!(
            forwarded.get("anthropic-beta"),
            Some(&HeaderValue::from_static("prompt-caching"))
        );
    }

    #[test]
    fn streaming_message_start_becomes_response_body_for_classifier() {
        let mut response = StreamingKeepaliveResponse::default();
        response.observe(
            Some(b"message_start"),
            b"event: message_start\ndata: {\"message\":{\"id\":\"m\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"cache_read_input_tokens\":2}}}\n\n",
        );
        response.observe(
            Some(b"message_delta"),
            b"event: message_delta\ndata: {\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n",
        );

        let value = response.into_value().expect("message_start captured");

        assert_eq!(
            value.get("stop_reason"),
            Some(&Value::String("tool_use".to_owned()))
        );
        assert_eq!(
            value
                .get("usage")
                .and_then(|usage| usage.get("cache_read_input_tokens"))
                .and_then(Value::as_i64),
            Some(2)
        );
    }
}
