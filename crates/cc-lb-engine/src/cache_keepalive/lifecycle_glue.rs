use std::sync::Arc;

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

use super::{CancelReason, HeuristicClassifier, KeepaliveScheduler, RequestSnapshot, SessionKey};

#[derive(Clone)]
pub(crate) struct LifecycleKeepaliveContext {
    pub(crate) request_body: Bytes,
    pub(crate) principal: Principal,
    pub(crate) cache_metadata: RequestCacheMetadata,
    pub(crate) upstream_id: Uuid,
    pub(crate) shaped_body: Bytes,
    pub(crate) downstream_headers: HeaderMap,
}

#[derive(Clone)]
pub(crate) struct LifecycleKeepalive {
    scheduler: Option<Arc<KeepaliveScheduler>>,
    dynamic_view: Arc<DynamicViewHolder>,
}

impl LifecycleKeepalive {
    pub(crate) fn new(
        scheduler: Option<Arc<KeepaliveScheduler>>,
        dynamic_view: Arc<DynamicViewHolder>,
    ) -> Self {
        Self {
            scheduler,
            dynamic_view,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_response_completed(
        &self,
        request_body: &[u8],
        response_body_json: &serde_json::Value,
        principal: &Principal,
        cache_metadata: &RequestCacheMetadata,
        upstream_id: uuid::Uuid,
        shaped_body: Bytes,
        downstream_headers: &HeaderMap,
    ) {
        let Some(scheduler) = self.scheduler.as_ref() else {
            return;
        };
        let _ = request_body;
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
        let Some(url) = keepalive_snapshot_url() else {
            return;
        };
        let snapshot = match RequestSnapshot::capture(
            url,
            http::Method::POST,
            downstream_auth_headers(downstream_headers),
            shaped_body,
            upstream_id,
            ttl,
            config.snapshot_max_bytes as usize,
        ) {
            Ok(snapshot) => Arc::new(snapshot),
            Err(super::SnapshotError::TooLarge { actual, max }) => {
                tracing::warn!(
                    target: "cache_keepalive",
                    principal_id = principal.id.as_str(),
                    actual,
                    max,
                    "skipping cache keep-alive snapshot: shaped body exceeds cap"
                );
                if !scheduler.cancel(&session_key, CancelReason::SnapshotTooLarge) {
                    super::record_cancelled(&principal_name, CancelReason::SnapshotTooLarge);
                }
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
            super::TurnDecision::AgentInTurn => scheduler.schedule_or_replace(
                session_key,
                snapshot,
                super::ScheduleParams::from(config, ttl),
                principal_name,
            ),
            super::TurnDecision::UserTurn => {
                scheduler.cancel(&session_key, CancelReason::UserTurnDetected);
            }
            super::TurnDecision::Ambiguous if config.classifier.llm_judge.is_some() => {
                tracing::warn!(
                    target: "cache_keepalive",
                    principal_id = principal.id.as_str(),
                    "cache keep-alive llm_judge is configured but unsupported in this release; treating ambiguous response as user turn"
                );
                scheduler.cancel(&session_key, CancelReason::UserTurnDetected);
            }
            super::TurnDecision::Ambiguous => {
                scheduler.cancel(&session_key, CancelReason::UserTurnDetected);
            }
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

const KEEPALIVE_FORWARD_HEADERS: &[&str] = &[
    "x-api-key",
    "authorization",
    "anthropic-version",
    "anthropic-beta",
];

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

fn keepalive_snapshot_url() -> Option<Url> {
    Url::parse("https://api.anthropic.com/v1/messages").ok()
}
