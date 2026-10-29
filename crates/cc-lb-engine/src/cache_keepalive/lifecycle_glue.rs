use std::collections::HashMap;
use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_control::dynamic_view::DynamicViewHolder;
use cc_lb_domain::Principal;
use http::HeaderMap;
use serde_json::Value;
use sha2::{Digest, Sha256};
use url::Url;
use uuid::Uuid;

use cc_lb_storage_api::{CacheKeepaliveConfigSnapshot, CacheTtl};

use crate::lifecycle::RequestCacheMetadata;
use crate::usage_parser::ParsedSseEvent;

use super::display_reason::CacheKeepaliveDisplayReason;
use super::{CancelReason, HeuristicClassifier, RequestSnapshot, SessionKey};

#[derive(Clone)]
pub struct CacheKeepaliveEnqueueRequest {
    pub session_key_hash: String,
    pub principal_id: String,
    pub accounting_key_id: Option<String>,
    pub cache_anchor_age: Duration,
    pub params: super::ScheduleParams,
    pub display_reason: String,
    pub config_snapshot: CacheKeepaliveConfigSnapshot,
    pub snapshot: RequestSnapshot,
}

#[derive(Clone, Debug)]
pub struct CacheKeepaliveCancelRequest {
    pub session_key_hash: String,
    pub reason: CancelReason,
}

#[derive(Clone)]
pub struct CacheKeepaliveNotTrackedRequest {
    pub session_key_hash: String,
    pub principal_id: String,
    pub upstream_id: Uuid,
    pub ttl: CacheTtl,
    pub config_snapshot: CacheKeepaliveConfigSnapshot,
    pub reason: String,
}

impl fmt::Debug for CacheKeepaliveEnqueueRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CacheKeepaliveEnqueueRequest")
            .field("session_key_hash", &self.session_key_hash)
            .field("principal_id", &self.principal_id)
            .field("accounting_key_id", &self.accounting_key_id)
            .field("cache_anchor_age", &self.cache_anchor_age)
            .field("params", &self.params)
            .field("display_reason", &self.display_reason)
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

    async fn record_cache_keepalive_not_tracked(
        &self,
        request: CacheKeepaliveNotTrackedRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError>;
}

#[derive(Clone)]
pub(crate) struct LifecycleKeepaliveContext {
    pub(crate) principal: Principal,
    pub(crate) accounting_key_id: String,
    pub(crate) cache_metadata: RequestCacheMetadata,
    pub(crate) upstream_id: Uuid,
    pub(crate) shaped_body: Bytes,
    pub(crate) downstream_headers: HeaderMap,
}

const SESSION_SHARD_COUNT: usize = 64;
const MAX_TRACKED_SESSIONS_PER_SHARD: usize = 512;

#[derive(Clone)]
pub(crate) struct LifecycleKeepalive {
    enqueuer: Option<Arc<dyn CacheKeepaliveEnqueuer>>,
    dynamic_view: Arc<DynamicViewHolder>,
    next_completion_seq: Arc<AtomicU64>,
    issued_shards: Arc<[std::sync::Mutex<HashMap<SessionKey, u64>>; SESSION_SHARD_COUNT]>,
    session_shards: Arc<[tokio::sync::Mutex<HashMap<SessionKey, u64>>; SESSION_SHARD_COUNT]>,
}

impl LifecycleKeepalive {
    pub(crate) fn new(
        enqueuer: Option<Arc<dyn CacheKeepaliveEnqueuer>>,
        dynamic_view: Arc<DynamicViewHolder>,
    ) -> Self {
        Self {
            enqueuer,
            dynamic_view,
            next_completion_seq: Arc::new(AtomicU64::new(0)),
            issued_shards: Arc::new(std::array::from_fn(|_| {
                std::sync::Mutex::new(HashMap::new())
            })),
            session_shards: Arc::new(std::array::from_fn(|_| {
                tokio::sync::Mutex::new(HashMap::new())
            })),
        }
    }

    pub(crate) fn with_enqueuer(&self, enqueuer: Arc<dyn CacheKeepaliveEnqueuer>) -> Self {
        Self {
            enqueuer: Some(enqueuer),
            dynamic_view: Arc::clone(&self.dynamic_view),
            next_completion_seq: Arc::clone(&self.next_completion_seq),
            issued_shards: Arc::clone(&self.issued_shards),
            session_shards: Arc::clone(&self.session_shards),
        }
    }

    fn shard_index(session_key: &SessionKey) -> usize {
        let mut hasher = DefaultHasher::new();
        session_key.hash(&mut hasher);
        (hasher.finish() as usize) % SESSION_SHARD_COUNT
    }

    fn session_shard(
        &self,
        session_key: &SessionKey,
    ) -> &tokio::sync::Mutex<HashMap<SessionKey, u64>> {
        &self.session_shards[Self::shard_index(session_key)]
    }

    fn record_issued_seq(&self, session_key: &SessionKey, completion_seq: u64) {
        let mut issued = self.issued_shards[Self::shard_index(session_key)]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self::advance_session_seq(&mut issued, session_key, completion_seq);
    }

    fn latest_issued_seq(&self, session_key: &SessionKey) -> u64 {
        self.issued_shards[Self::shard_index(session_key)]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_key)
            .copied()
            .unwrap_or(0)
    }

    fn claim_completion_seq(
        &self,
        shard: &mut HashMap<SessionKey, u64>,
        session_key: &SessionKey,
        completion_seq: u64,
    ) -> bool {
        if completion_seq < self.latest_issued_seq(session_key) {
            return false;
        }
        Self::advance_session_seq(shard, session_key, completion_seq)
    }

    fn advance_session_seq(
        shard: &mut HashMap<SessionKey, u64>,
        session_key: &SessionKey,
        completion_seq: u64,
    ) -> bool {
        if shard
            .get(session_key)
            .is_some_and(|&last_seq| completion_seq <= last_seq)
        {
            return false;
        }
        if !shard.contains_key(session_key)
            && shard.len() >= MAX_TRACKED_SESSIONS_PER_SHARD
            && let Some(oldest_key) = shard
                .iter()
                .min_by_key(|(_, seq)| **seq)
                .map(|(key, _)| key.clone())
        {
            shard.remove(&oldest_key);
        }
        shard.insert(session_key.clone(), completion_seq);
        true
    }

    /// Detaches classification and the serial keep-alive storage writes onto a
    /// background task so the client response path never awaits them. Loads the
    /// principal's config and rejects disabled principals before cloning the
    /// borrowed inputs for the detached task.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_response_completed(
        &self,
        response_body_json: &serde_json::Value,
        principal: &Principal,
        accounting_key_id: String,
        cache_metadata: &RequestCacheMetadata,
        upstream_id: uuid::Uuid,
        shaped_body: Bytes,
        downstream_headers: &HeaderMap,
        cache_anchor_age: Duration,
        cached_tokens: u64,
    ) {
        if self.enqueuer.is_none() {
            return;
        }
        let view = self.dynamic_view.load();
        let Some(_config) = view
            .principal_view
            .get(&principal.id)
            .and_then(|cached| cached.cache_keepalive())
            .filter(|config| config.enabled)
        else {
            return;
        };
        let Some(session_key) = cache_keepalive_session_key(principal, cache_metadata) else {
            return;
        };
        let completion_seq = self.next_completion_seq.fetch_add(1, Ordering::Relaxed) + 1;
        self.record_issued_seq(&session_key, completion_seq);
        let this = self.clone();
        let response_body_json = response_body_json.clone();
        let principal = principal.clone();
        let cache_metadata = cache_metadata.clone();
        let downstream_headers = downstream_headers.clone();
        tokio::spawn(async move {
            this.persist_completion(
                completion_seq,
                &response_body_json,
                &principal,
                accounting_key_id,
                &cache_metadata,
                upstream_id,
                shaped_body,
                &downstream_headers,
                cache_anchor_age,
                cached_tokens,
            )
            .await;
        });
    }

    #[allow(clippy::too_many_arguments)]
    async fn persist_completion(
        &self,
        completion_seq: u64,
        response_body_json: &serde_json::Value,
        principal: &Principal,
        accounting_key_id: String,
        cache_metadata: &RequestCacheMetadata,
        upstream_id: uuid::Uuid,
        shaped_body: Bytes,
        downstream_headers: &HeaderMap,
        cache_anchor_age: Duration,
        cached_tokens: u64,
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
        let mut shard = self.session_shard(&session_key).lock().await;
        let is_latest = self.claim_completion_seq(&mut shard, &session_key, completion_seq);
        let url =
            Url::parse(KEEPALIVE_SNAPSHOT_URL).expect("cache keep-alive snapshot URL is valid");
        let snapshot = match RequestSnapshot::capture(
            url,
            http::Method::POST,
            keepalive_forward_headers(downstream_headers),
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
                self.record_not_tracked(
                    &session_key,
                    principal,
                    upstream_id,
                    ttl,
                    config,
                    &CacheKeepaliveDisplayReason::SnapshotTooLarge.to_string(),
                )
                .await;
                if is_latest {
                    self.cancel_session(&session_key, principal, CancelReason::SnapshotTooLarge)
                        .await;
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
            super::TurnDecision::AgentInTurn => {
                if cached_tokens < config.min_cache_tokens {
                    let reason = CacheKeepaliveDisplayReason::CachedPrefixTooSmall {
                        cached_tokens,
                        min_cache_tokens: config.min_cache_tokens,
                    }
                    .to_string();
                    self.record_not_tracked(
                        &session_key,
                        principal,
                        upstream_id,
                        ttl,
                        config,
                        &reason,
                    )
                    .await;
                    if is_latest {
                        self.cancel_session(
                            &session_key,
                            principal,
                            CancelReason::CachedPrefixTooSmall,
                        )
                        .await;
                        super::record_cancelled(
                            &principal_name,
                            CancelReason::CachedPrefixTooSmall,
                        );
                    }
                    return;
                }
                if !is_latest {
                    return;
                }
                let params =
                    super::ScheduleParams::from_cache_anchor_age(config, ttl, cache_anchor_age);
                let display_reason =
                    CacheKeepaliveDisplayReason::agent_in_turn(response_body_json, params.delay)
                        .to_string();
                if let Err(error) = enqueuer
                    .enqueue_cache_keepalive(CacheKeepaliveEnqueueRequest {
                        session_key_hash: session_key.to_string(),
                        principal_id: principal.id.clone(),
                        accounting_key_id: Some(accounting_key_id),
                        cache_anchor_age,
                        params,
                        display_reason,
                        config_snapshot: CacheKeepaliveConfigSnapshot {
                            refresh_lead_time_5m_secs: config.refresh_lead_time_5m_secs,
                            refresh_lead_time_1h_secs: config.refresh_lead_time_1h_secs,
                            max_refreshes_per_session: config.max_refreshes_per_session,
                            max_total_duration_secs: config.max_total_duration_secs,
                            snapshot_max_bytes: config.snapshot_max_bytes,
                        },
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
                self.record_not_tracked(
                    &session_key,
                    principal,
                    upstream_id,
                    ttl,
                    config,
                    &CacheKeepaliveDisplayReason::user_turn(response_body_json).to_string(),
                )
                .await;
                if is_latest {
                    self.cancel_session(&session_key, principal, CancelReason::UserTurnDetected)
                        .await;
                }
            }
            super::TurnDecision::Ambiguous => {
                self.record_not_tracked(
                    &session_key,
                    principal,
                    upstream_id,
                    ttl,
                    config,
                    &CacheKeepaliveDisplayReason::AmbiguousTurn.to_string(),
                )
                .await;
                if is_latest {
                    self.cancel_session(&session_key, principal, CancelReason::UserTurnDetected)
                        .await;
                }
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

    async fn record_not_tracked(
        &self,
        session_key: &SessionKey,
        principal: &Principal,
        upstream_id: Uuid,
        ttl: CacheTtl,
        config: &cc_lb_storage_api::CacheKeepaliveConfig,
        reason: &str,
    ) {
        let Some(enqueuer) = self.enqueuer.as_ref() else {
            return;
        };
        if let Err(error) = enqueuer
            .record_cache_keepalive_not_tracked(CacheKeepaliveNotTrackedRequest {
                session_key_hash: session_key.to_string(),
                principal_id: principal.id.clone(),
                upstream_id,
                ttl,
                config_snapshot: CacheKeepaliveConfigSnapshot {
                    refresh_lead_time_5m_secs: config.refresh_lead_time_5m_secs,
                    refresh_lead_time_1h_secs: config.refresh_lead_time_1h_secs,
                    max_refreshes_per_session: config.max_refreshes_per_session,
                    max_total_duration_secs: config.max_total_duration_secs,
                    snapshot_max_bytes: config.snapshot_max_bytes,
                },
                reason: reason.to_owned(),
            })
            .await
        {
            tracing::warn!(
                target: "cache_keepalive",
                principal_id = principal.id.as_str(),
                %error,
                "cache keep-alive not-tracked projection failed"
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
    pub(crate) fn observe(&mut self, event: &ParsedSseEvent<'_>) {
        let Ok(Some(value)) = event.value() else {
            return;
        };
        match event.event_name() {
            Some(b"message_start") => {
                if let Some(message) = value.get("message") {
                    self.message = Some(message.clone());
                }
            }
            Some(b"content_block_start") => {
                if let Some(block) = value.get("content_block") {
                    self.content_blocks.push(block.clone());
                }
            }
            Some(b"message_delta") => {
                if let Some(delta_usage) = value.get("usage").and_then(Value::as_object)
                    && let Some(message) = self.message.as_mut().and_then(Value::as_object_mut)
                {
                    let usage = message
                        .entry("usage".to_owned())
                        .or_insert_with(|| Value::Object(Default::default()));
                    if let Some(existing_usage) = usage.as_object_mut() {
                        for (key, value) in delta_usage {
                            existing_usage.insert(key.clone(), value.clone());
                        }
                    } else {
                        *usage = Value::Object(delta_usage.clone());
                    }
                }
                if let Some(stop_reason) = value
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

const KEEPALIVE_FORWARD_HEADERS: &[&str] = &["anthropic-version", "anthropic-beta"];

fn keepalive_forward_headers(headers: &HeaderMap) -> HeaderMap {
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
        let session_key = if let Some(agent_id) =
            SessionKey::normalize_agent_id(cache_metadata.claude_agent_id.as_deref())
        {
            SessionKey::from_thread_and_agent(principal_id, thread_id, agent_id)
        } else {
            SessionKey::from_thread_id(principal_id, thread_id)
        };
        return Some(session_key);
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
    fn keepalive_forward_headers_preserve_protocol_headers_without_auth_secrets() {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_static("secret-key"));
        headers.insert("authorization", HeaderValue::from_static("Bearer secret"));
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        headers.insert("anthropic-beta", HeaderValue::from_static("prompt-caching"));

        let forwarded = keepalive_forward_headers(&headers);

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
        response.observe(&crate::usage_parser::parse_sse_event(
            b"event: message_start\ndata: {\"message\":{\"id\":\"m\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"cache_read_input_tokens\":2}}}\n\n",
        ));
        response.observe(&crate::usage_parser::parse_sse_event(
            b"event: message_delta\ndata: {\"usage\":{\"cache_creation_input_tokens\":3},\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n",
        ));

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
        assert_eq!(
            value
                .get("usage")
                .and_then(|usage| usage.get("cache_creation_input_tokens"))
                .and_then(Value::as_i64),
            Some(3)
        );
    }
    #[test]
    fn session_key_uses_agent_identity_when_present() {
        let principal = Principal {
            id: "principal-test".to_owned(),
            kind: cc_lb_domain::PrincipalKind::InternalKey,
        };
        let mut metadata = RequestCacheMetadata::default();
        metadata.thread_id = Some("thread-1".to_owned());
        metadata.claude_agent_id = Some("agent-a".to_owned());
        assert_eq!(
            cache_keepalive_session_key(&principal, &metadata)
                .unwrap()
                .to_string(),
            format!(
                "{}/thread_agent:8:thread-1:agent-a",
                cache_keepalive_principal_uuid("principal-test")
            )
        );
    }

    #[test]
    fn session_key_ignores_main_agent_identity() {
        let principal = Principal {
            id: "principal-test".to_owned(),
            kind: cc_lb_domain::PrincipalKind::InternalKey,
        };
        let mut metadata = RequestCacheMetadata::default();
        metadata.thread_id = Some("thread-1".to_owned());
        metadata.claude_agent_id = Some(" main ".to_owned());
        assert_eq!(
            cache_keepalive_session_key(&principal, &metadata)
                .unwrap()
                .to_string(),
            format!(
                "{}/thread:thread-1",
                cache_keepalive_principal_uuid("principal-test")
            )
        );
    }
    #[test]
    fn session_key_without_agent_identity_uses_thread() {
        let principal = Principal {
            id: "principal-test".to_owned(),
            kind: cc_lb_domain::PrincipalKind::InternalKey,
        };
        let mut metadata = RequestCacheMetadata::default();
        metadata.thread_id = Some("thread-1".to_owned());
        metadata.claude_agent_id = None;
        assert_eq!(
            cache_keepalive_session_key(&principal, &metadata)
                .unwrap()
                .to_string(),
            format!(
                "{}/thread:thread-1",
                cache_keepalive_principal_uuid("principal-test")
            )
        );
    }

    #[test]
    fn session_key_falls_back_to_prefix_without_thread() {
        let principal = Principal {
            id: "principal-test".to_owned(),
            kind: cc_lb_domain::PrincipalKind::InternalKey,
        };
        let mut metadata = RequestCacheMetadata::default();
        metadata.cache_prefix_hash = Some("prefix-1".to_owned());
        metadata.claude_agent_id = Some("agent-a".to_owned());
        assert_eq!(
            cache_keepalive_session_key(&principal, &metadata)
                .unwrap()
                .to_string(),
            format!(
                "{}/prefix:prefix-1",
                cache_keepalive_principal_uuid("principal-test")
            )
        );
    }

    #[test]
    fn advance_session_seq_rejects_stale_or_duplicate_completions() {
        let mut shard = HashMap::new();
        let key = SessionKey::from_thread_id(Uuid::from_u128(1), "thread-1");
        assert!(LifecycleKeepalive::advance_session_seq(&mut shard, &key, 2));
        assert!(!LifecycleKeepalive::advance_session_seq(
            &mut shard, &key, 1
        ));
        assert!(!LifecycleKeepalive::advance_session_seq(
            &mut shard, &key, 2
        ));
        assert!(LifecycleKeepalive::advance_session_seq(&mut shard, &key, 3));
    }
}
