use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

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
    pub cycle_id: Uuid,
    pub cache_anchor_age: Duration,
    pub expected_generation: Option<u64>,
    pub params: super::ScheduleParams,
    pub display_reason: String,
    pub config_snapshot: CacheKeepaliveConfigSnapshot,
    pub snapshot: RequestSnapshot,
}

#[derive(Clone, Debug)]
pub struct CacheKeepaliveCancelRequest {
    pub session_key_hash: String,
    pub reason: CancelReason,
    pub expected_generation: Option<u64>,
    pub cache_anchor_age: Duration,
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

    async fn observe_session_generation(
        &self,
        session_key_hash: &str,
    ) -> Result<Option<u64>, CacheKeepaliveEnqueueError> {
        let _ = session_key_hash;
        Ok(None)
    }
}

#[derive(Clone)]
pub(crate) struct LifecycleKeepaliveContext {
    pub(crate) principal: Principal,
    pub(crate) accounting_key_id: String,
    pub(crate) cache_metadata: RequestCacheMetadata,
    pub(crate) upstream_id: Uuid,
    pub(crate) shaped_body: Bytes,
    pub(crate) original_body: Bytes,
    pub(crate) downstream_headers: HeaderMap,
    pub(crate) request_dispatched_at: Instant,
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

    /// Fence queued work before a real request reaches the upstream. Unlike
    /// completion persistence this must finish on the dispatch path.
    pub(crate) async fn on_request_started(
        &self,
        _authenticated: &crate::authn_rail::Authenticated,
        principal: &Principal,
        cache_metadata: &RequestCacheMetadata,
    ) {
        let Some(enqueuer) = self.enqueuer.as_ref() else {
            return;
        };
        if cache_metadata.explicit_auxiliary
            || !is_keepalive_eligible_request_kind(cache_metadata.request_kind.as_deref())
            || (cache_metadata.cache_breakpoints.is_empty()
                && cache_metadata.cache_keepalive_surface.is_none())
        {
            return;
        }
        let view = self.dynamic_view.load();
        if !view
            .principal_view
            .get(&principal.id)
            .and_then(|cached| cached.cache_keepalive())
            .is_some_and(|config| config.enabled)
        {
            return;
        }
        let Some(session_key) = cache_keepalive_session_key(principal, cache_metadata) else {
            return;
        };
        let sequence = self.next_completion_seq.fetch_add(1, Ordering::Relaxed) + 1;
        self.record_issued_seq(&session_key, sequence);
        let mut shard = self.session_shard(&session_key).lock().await;
        // Freshness suppresses stale completions, never a real request's fence:
        // a newer waiter may be dropped before it performs its durable write.
        let _ = self.claim_completion_seq(&mut shard, &session_key, sequence);
        let expected_generation = match enqueuer
            .observe_session_generation(&session_key.to_string())
            .await
        {
            Ok(expected) => expected,
            Err(error) => {
                tracing::warn!(
                    target: "cache_keepalive",
                    principal_id = principal.id.as_str(),
                    %error,
                    "skipping cache keep-alive request fence: failed to observe session generation"
                );
                return;
            }
        };
        self.cancel_session(
            &session_key,
            principal,
            CancelReason::NewRequest,
            expected_generation,
            Duration::ZERO,
        )
        .await;
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
        original_body: Bytes,
        downstream_headers: &HeaderMap,
        cache_anchor_age: Duration,
        cached_tokens: u64,
    ) {
        if self.enqueuer.is_none() {
            return;
        }
        if cache_metadata.explicit_auxiliary
            || !is_keepalive_eligible_request_kind(cache_metadata.request_kind.as_deref())
        {
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
        let compaction_boundary =
            super::classifier::HeuristicClassifier::is_compaction_boundary(response_body_json);
        if cache_metadata.cache_breakpoints.is_empty()
            && !compaction_boundary
            && cache_metadata.cache_keepalive_surface.is_none()
        {
            return;
        }
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
                original_body,
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
        original_body: Bytes,
        downstream_headers: &HeaderMap,
        cache_anchor_age: Duration,
        cached_tokens: u64,
    ) {
        let Some(enqueuer) = self.enqueuer.as_ref() else {
            return;
        };
        if cache_metadata.explicit_auxiliary
            || !is_keepalive_eligible_request_kind(cache_metadata.request_kind.as_deref())
        {
            return;
        }
        let view = self.dynamic_view.load();
        let Some(cached) = view.principal_view.get(&principal.id) else {
            return;
        };
        let Some(config) = cached.cache_keepalive().filter(|config| config.enabled) else {
            return;
        };
        let Some(session_key) = cache_keepalive_session_key(principal, cache_metadata) else {
            return;
        };
        let compaction_boundary = HeuristicClassifier::is_compaction_boundary(response_body_json);
        if compaction_boundary
            && cache_metadata.cache_breakpoints.is_empty()
            && cache_metadata.cache_keepalive_surface.is_none()
        {
            let principal_name: Arc<str> = Arc::from(principal.id.as_str());
            let session_key_hash = session_key.to_string();
            let mut shard = self.session_shard(&session_key).lock().await;
            let is_latest = self.claim_completion_seq(&mut shard, &session_key, completion_seq);
            let expected_generation = match enqueuer
                .observe_session_generation(&session_key_hash)
                .await
            {
                Ok(expected) => expected,
                Err(error) => {
                    tracing::warn!(
                        target: "cache_keepalive",
                        principal_id = principal.id.as_str(),
                        %error,
                        "skipping cache keep-alive compaction fence: failed to observe session generation"
                    );
                    return;
                }
            };
            if is_latest {
                self.cancel_session(
                    &session_key,
                    principal,
                    CancelReason::UserTurnDetected,
                    expected_generation,
                    cache_anchor_age,
                )
                .await;
                super::record_cancelled(&principal_name, CancelReason::UserTurnDetected);
            }
            return;
        }
        let Some(request_json) = cache_metadata.request_json.as_ref() else {
            return;
        };
        let ttl = cache_metadata.cache_keepalive_ttl.unwrap_or_else(|| {
            cache_metadata
                .cache_breakpoints
                .first()
                .map(|breakpoint| CacheTtl::from_ttl_str(breakpoint.ttl.as_deref()))
                .unwrap_or(CacheTtl::Ttl5m)
        });
        let principal_name: Arc<str> = Arc::from(principal.id.as_str());
        let session_key_hash = session_key.to_string();
        let mut shard = self.session_shard(&session_key).lock().await;
        let is_latest = self.claim_completion_seq(&mut shard, &session_key, completion_seq);
        let expected_generation = match enqueuer.observe_session_generation(&session_key_hash).await
        {
            Ok(expected) => expected,
            Err(error) => {
                tracing::warn!(
                    target: "cache_keepalive",
                    principal_id = principal.id.as_str(),
                    %error,
                    "skipping cache keep-alive completion: failed to observe session generation"
                );
                return;
            }
        };
        let renewal_request_json =
            match keepalive_eligibility_request_json(request_json, &original_body, &shaped_body) {
                Ok(value) => value,
                Err(error) => {
                    tracing::warn!(
                        target: "cache_keepalive",
                        principal_id = principal.id.as_str(),
                        %error,
                        "skipping cache keep-alive eligibility: shaped body is not valid JSON"
                    );
                    return;
                }
            };
        if cache_metadata.cache_keepalive_surface.is_none()
            && let Some(reason) = zero_token_renewal_unsupported_reason(&renewal_request_json)
        {
            self.record_not_tracked(
                &session_key,
                principal,
                upstream_id,
                ttl,
                config,
                &reason.to_string(),
            )
            .await;
            if is_latest {
                self.cancel_session(
                    &session_key,
                    principal,
                    CancelReason::ZeroTokenRenewalUnsupported,
                    expected_generation,
                    cache_anchor_age,
                )
                .await;
                super::record_cancelled(&principal_name, CancelReason::ZeroTokenRenewalUnsupported);
            }
            return;
        }
        let snapshot_url = if cache_metadata.cache_keepalive_surface.as_deref() == Some("responses")
        {
            KEEPALIVE_OPENAI_SNAPSHOT_URL
        } else {
            KEEPALIVE_SNAPSHOT_URL
        };
        let url = Url::parse(snapshot_url).expect("cache keep-alive snapshot URL is valid");
        let snapshot_result =
            if cache_metadata.cache_keepalive_surface.as_deref() == Some("responses") {
                RequestSnapshot::capture_openai(
                    url,
                    http::Method::POST,
                    keepalive_forward_headers(downstream_headers),
                    shaped_body,
                    upstream_id,
                    ttl,
                    config.snapshot_max_bytes as usize,
                )
            } else {
                RequestSnapshot::capture(
                    url,
                    http::Method::POST,
                    keepalive_forward_headers(downstream_headers),
                    shaped_body,
                    upstream_id,
                    ttl,
                    config.snapshot_max_bytes as usize,
                )
            };
        let snapshot = match snapshot_result {
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
                    self.cancel_session(
                        &session_key,
                        principal,
                        CancelReason::SnapshotTooLarge,
                        expected_generation,
                        cache_anchor_age,
                    )
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
        let (fingerprint, hash, policy, schema) = cache_prefix_evidence(cache_metadata);
        let snapshot = snapshot.with_prefix_evidence(fingerprint, hash, policy, schema);
        let decision = if cache_metadata.cache_keepalive_surface.is_some() {
            super::TurnDecision::AgentInTurn
        } else {
            HeuristicClassifier::new(&config.classifier).classify(request_json, response_body_json)
        };
        match decision {
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
                            expected_generation,
                            cache_anchor_age,
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
                if ttl == CacheTtl::Ttl12h {
                    self.record_not_tracked(
                        &session_key,
                        principal,
                        upstream_id,
                        ttl,
                        config,
                        "openai 12h retention is recorded without read re-anchoring",
                    )
                    .await;
                    return;
                }
                let params =
                    super::ScheduleParams::from_cache_anchor_age(config, ttl, cache_anchor_age);
                let display_reason =
                    CacheKeepaliveDisplayReason::agent_in_turn(response_body_json, params.delay)
                        .to_string();
                if let Err(error) = enqueuer
                    .enqueue_cache_keepalive(CacheKeepaliveEnqueueRequest {
                        session_key_hash,
                        principal_id: principal.id.clone(),
                        accounting_key_id: Some(accounting_key_id),
                        cycle_id: Uuid::new_v4(),
                        cache_anchor_age,
                        expected_generation,
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
                    self.cancel_session(
                        &session_key,
                        principal,
                        CancelReason::UserTurnDetected,
                        expected_generation,
                        cache_anchor_age,
                    )
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
                    self.cancel_session(
                        &session_key,
                        principal,
                        CancelReason::UserTurnDetected,
                        expected_generation,
                        cache_anchor_age,
                    )
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
        expected_generation: Option<u64>,
        cache_anchor_age: Duration,
    ) {
        if let Some(enqueuer) = self.enqueuer.as_ref()
            && let Err(error) = enqueuer
                .cancel_cache_keepalive(CacheKeepaliveCancelRequest {
                    session_key_hash: session_key.to_string(),
                    reason,
                    expected_generation,
                    cache_anchor_age,
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
    pending_sse_bytes: Vec<u8>,
    sse_observation_disabled: bool,
}

impl StreamingKeepaliveResponse {
    pub(crate) fn observe_emitted(&mut self, bytes: &[u8], max_pending_sse_bytes: usize) {
        if self.sse_observation_disabled {
            return;
        }
        if self.pending_sse_bytes.is_empty() {
            let mut offset = 0;
            while let Some(end) = crate::usage_parser::find_sse_event_end(&bytes[offset..]) {
                let end = offset + end;
                self.observe(&crate::usage_parser::parse_sse_event(&bytes[offset..end]));
                offset = end;
            }
            if offset < bytes.len() {
                let pending = &bytes[offset..];
                if pending.len() > max_pending_sse_bytes {
                    self.sse_observation_disabled = true;
                } else {
                    self.pending_sse_bytes.extend_from_slice(pending);
                }
            }
            return;
        }

        if self.pending_sse_bytes.len().saturating_add(bytes.len()) > max_pending_sse_bytes {
            self.pending_sse_bytes.clear();
            self.sse_observation_disabled = true;
            return;
        }
        self.pending_sse_bytes.extend_from_slice(bytes);
        let mut offset = 0;
        while let Some(relative_end) =
            crate::usage_parser::find_sse_event_end(&self.pending_sse_bytes[offset..])
        {
            let end = offset + relative_end;
            let event_bytes = self.pending_sse_bytes[offset..end].to_vec();
            self.observe(&crate::usage_parser::parse_sse_event(&event_bytes));
            offset = end;
        }
        if offset > 0 {
            self.pending_sse_bytes.drain(..offset);
        }
    }

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

fn keepalive_eligibility_request_json<'a>(
    request_json: &'a Value,
    original_body: &Bytes,
    shaped_body: &Bytes,
) -> Result<Cow<'a, Value>, sonic_rs::Error> {
    if shaped_body == original_body {
        Ok(Cow::Borrowed(request_json))
    } else {
        sonic_rs::from_slice(shaped_body).map(Cow::Owned)
    }
}

/// Fields Anthropic rejects with `max_tokens: 0` cannot be rewritten without
/// changing the live request's cache prefix. Inspect the already-parsed body
/// before capturing a snapshot or classifying the response.
fn zero_token_renewal_unsupported_reason(request: &Value) -> Option<CacheKeepaliveDisplayReason> {
    let feature = if request
        .get("thinking")
        .and_then(|thinking| thinking.get("type"))
        .and_then(Value::as_str)
        == Some("enabled")
    {
        "enabled thinking"
    } else if request
        .get("output_config")
        .and_then(|config| config.get("format"))
        .is_some()
    {
        "structured output"
    } else if matches!(
        request
            .get("tool_choice")
            .and_then(|choice| choice.get("type"))
            .and_then(Value::as_str),
        Some("tool" | "any")
    ) {
        "forced tool_choice"
    } else {
        return None;
    };
    Some(CacheKeepaliveDisplayReason::ZeroTokenRenewalUnsupported(
        feature,
    ))
}

fn is_keepalive_eligible_request_kind(request_kind: Option<&str>) -> bool {
    !matches!(
        request_kind,
        Some(
            "recap"
                | "compaction"
                | "session_title"
                | "auto_thinking"
                | "notification"
                | "side"
                | "look_at"
        )
    )
}
const CACHE_PREFIX_POLICY_VERSION: &str = "prompt-cache-policy-v1";
const CACHE_PREFIX_SCHEMA_VERSION: &str = "request-cache-breakpoint-v1";

fn cache_prefix_evidence(
    metadata: &RequestCacheMetadata,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    if metadata.cache_breakpoints.is_empty() {
        return (None, None, None, None);
    }
    let mut hasher = Sha256::new();
    hasher.update(CACHE_PREFIX_SCHEMA_VERSION.as_bytes());
    for breakpoint in &metadata.cache_breakpoints {
        hasher.update(breakpoint.block_index.to_le_bytes());
        hasher.update(breakpoint.path.as_bytes());
        hasher.update(breakpoint.prefix_hash.as_bytes());
        if let Some(ttl) = breakpoint.ttl.as_deref() {
            hasher.update(ttl.as_bytes());
        }
    }
    let fingerprint = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    (
        Some(fingerprint),
        metadata.cache_prefix_hash.clone(),
        Some(CACHE_PREFIX_POLICY_VERSION.to_owned()),
        Some(CACHE_PREFIX_SCHEMA_VERSION.to_owned()),
    )
}

fn cache_keepalive_session_key(
    principal: &Principal,
    cache_metadata: &RequestCacheMetadata,
) -> Option<SessionKey> {
    let principal_id = uuid::Uuid::parse_str(&principal.id)
        .unwrap_or_else(|_| cache_keepalive_principal_uuid(&principal.id));
    let agent_scope = match cache_metadata.claude_agent_id.as_deref() {
        Some(raw) => SessionKey::normalize_agent_id(Some(raw)).map(ToOwned::to_owned),
        None => (cache_metadata.request_kind.as_deref() == Some("subagent"))
            .then(|| "subagent".to_owned()),
    };
    if let Some(thread_id) = cache_metadata.thread_id.clone() {
        return Some(
            SessionKey::from_thread_id(principal_id, thread_id).with_agent_scope(agent_scope),
        );
    }
    cache_metadata.cache_prefix_hash.clone().map(|prefix_hash| {
        SessionKey::from_cache_prefix_hash(principal_id, prefix_hash).with_agent_scope(agent_scope)
    })
}

fn cache_keepalive_principal_uuid(principal_id: &str) -> Uuid {
    let digest = Sha256::digest(principal_id.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

const KEEPALIVE_SNAPSHOT_URL: &str = "https://api.anthropic.com/v1/messages";
const KEEPALIVE_OPENAI_SNAPSHOT_URL: &str = "https://api.openai.com/v1/responses";

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_control::api_keys::principal_view::PrincipalView;
    use cc_lb_control::dynamic_view::{DynamicViewBuilder, DynamicViewHolder};
    use cc_lb_domain::Upstream;
    use cc_lb_storage_api::principal::{
        Limit, PrincipalKind as StoragePrincipalKind, PrincipalRecord as StoragePrincipalRecord,
    };
    use cc_lb_storage_api::{CacheKeepaliveConfig, ClassifierConfig, MetaStore};
    use cc_lb_upstream::{
        ApiKeyAwareSignerFactory, RetryDecision, ShapedRequest, Signer, SignerError, SignerFactory,
        SigningCapability, UpstreamError,
    };
    use http::HeaderValue;

    #[derive(Debug, PartialEq, Eq)]
    enum OrderedCompletion {
        Observe(String),
        Enqueue(String, Option<u64>),
        NotTracked(String),
        Cancel(String, Option<u64>),
    }

    struct OrderedCompletionEnqueuer {
        events: tokio::sync::mpsc::UnboundedSender<OrderedCompletion>,
    }

    #[async_trait]
    impl CacheKeepaliveEnqueuer for OrderedCompletionEnqueuer {
        async fn observe_session_generation(
            &self,
            key: &str,
        ) -> Result<Option<u64>, CacheKeepaliveEnqueueError> {
            self.events
                .send(OrderedCompletion::Observe(key.to_owned()))
                .expect("observer receiver");
            Ok(Some(41))
        }

        async fn enqueue_cache_keepalive(
            &self,
            request: CacheKeepaliveEnqueueRequest,
        ) -> Result<(), CacheKeepaliveEnqueueError> {
            self.events
                .send(OrderedCompletion::Enqueue(
                    request.session_key_hash,
                    request.expected_generation,
                ))
                .expect("enqueue receiver");
            Ok(())
        }

        async fn cancel_cache_keepalive(
            &self,
            request: CacheKeepaliveCancelRequest,
        ) -> Result<(), CacheKeepaliveEnqueueError> {
            assert_eq!(request.reason, CancelReason::UserTurnDetected);
            self.events
                .send(OrderedCompletion::Cancel(
                    request.session_key_hash,
                    request.expected_generation,
                ))
                .expect("cancel receiver");
            Ok(())
        }

        async fn record_cache_keepalive_not_tracked(
            &self,
            request: CacheKeepaliveNotTrackedRequest,
        ) -> Result<(), CacheKeepaliveEnqueueError> {
            assert_eq!(request.reason, "user turn (stop_reason=end_turn)");
            self.events
                .send(OrderedCompletion::NotTracked(request.session_key_hash))
                .expect("not-tracked receiver");
            Ok(())
        }
    }

    impl LifecycleKeepalive {
        // Called from lifecycle's metadata tests so the auxiliary identity is built
        // by the real header/body constructor, without exposing a production API.
        pub(crate) async fn assert_auxiliary_cannot_supersede_pending_completion(
            dynamic_view: Arc<DynamicViewHolder>,
            principal: Principal,
            legitimate: RequestCacheMetadata,
            auxiliary: RequestCacheMetadata,
            body: Bytes,
            cancellation: bool,
            detached_gate: bool,
        ) {
            let (events, mut received) = tokio::sync::mpsc::unbounded_channel();
            let keepalive = LifecycleKeepalive::new(
                Some(Arc::new(OrderedCompletionEnqueuer { events })),
                dynamic_view,
            );
            let key = cache_keepalive_session_key(&principal, &legitimate)
                .expect("legitimate session key");
            assert_eq!(
                cache_keepalive_session_key(&principal, &auxiliary),
                Some(key.clone())
            );
            let expected_key = key.to_string();
            let headers = HeaderMap::new();
            let agent_turn = serde_json::json!({
                "type": "message",
                "content": [{"type": "tool_use", "id": "toolu_ordered", "name": "Bash", "input": {}}],
                "stop_reason": "tool_use"
            });
            let user_turn = serde_json::json!({
                "type": "message", "content": [{"type": "text", "text": "done"}],
                "stop_reason": "end_turn"
            });
            let (legitimate_response, auxiliary_response) = if cancellation {
                (&user_turn, &agent_turn)
            } else {
                (&agent_turn, &user_turn)
            };
            if detached_gate {
                keepalive
                    .persist_completion(
                        1,
                        auxiliary_response,
                        &principal,
                        "accounting-key".to_owned(),
                        &auxiliary,
                        Uuid::nil(),
                        body.clone(),
                        body.clone(),
                        &headers,
                        Duration::ZERO,
                        12_000,
                    )
                    .await;
                assert!(
                    received.try_recv().is_err(),
                    "an auxiliary detached completion must not observe or mutate a generation"
                );
            }
            let shard = keepalive.session_shard(&key).lock().await;
            keepalive.on_response_completed(
                legitimate_response,
                &principal,
                "accounting-key".to_owned(),
                &legitimate,
                Uuid::nil(),
                body.clone(),
                body.clone(),
                &headers,
                Duration::ZERO,
                12_000,
            );
            if !detached_gate {
                keepalive.on_response_completed(
                    auxiliary_response,
                    &principal,
                    "accounting-key".to_owned(),
                    &auxiliary,
                    Uuid::nil(),
                    body.clone(),
                    body,
                    &headers,
                    Duration::ZERO,
                    12_000,
                );
            }
            drop(shard);
            let operations = tokio::time::timeout(Duration::from_secs(5), async {
                let mut operations = Vec::new();
                loop {
                    let event = received.recv().await.expect("completion event");
                    let terminal = matches!(
                        event,
                        OrderedCompletion::Enqueue(..) | OrderedCompletion::Cancel(..)
                    );
                    operations.push(event);
                    if terminal {
                        break operations;
                    }
                }
            })
            .await
            .expect("legitimate completion must reach its durable operation");
            let expected = if cancellation {
                vec![
                    OrderedCompletion::Observe(expected_key.clone()),
                    OrderedCompletion::NotTracked(expected_key.clone()),
                    OrderedCompletion::Cancel(expected_key, Some(41)),
                ]
            } else {
                vec![
                    OrderedCompletion::Observe(expected_key.clone()),
                    OrderedCompletion::Enqueue(expected_key, Some(41)),
                ]
            };
            assert_eq!(
                operations, expected,
                "auxiliary cannot replace the pending legitimate completion"
            );
        }

        pub(crate) async fn assert_no_breakpoint_cannot_supersede_pending_completion(
            dynamic_view: Arc<DynamicViewHolder>,
            principal: Principal,
            legitimate: RequestCacheMetadata,
            body: Bytes,
            ordinary_response: serde_json::Value,
        ) {
            let (events, mut received) = tokio::sync::mpsc::unbounded_channel();
            let keepalive = LifecycleKeepalive::new(
                Some(Arc::new(OrderedCompletionEnqueuer { events })),
                dynamic_view,
            );
            let key = cache_keepalive_session_key(&principal, &legitimate)
                .expect("legitimate session key");
            let expected_key = key.to_string();
            let mut ordinary = RequestCacheMetadata::default();
            ordinary.thread_id = legitimate.thread_id.clone();
            let headers = HeaderMap::new();
            let shard = keepalive.session_shard(&key).lock().await;
            keepalive.on_response_completed(
                &serde_json::json!({
                    "type": "message",
                    "content": [{"type": "tool_use", "id": "toolu_pending", "name": "Bash", "input": {}}],
                    "stop_reason": "tool_use"
                }),
                &principal,
                "accounting-key".to_owned(),
                &legitimate,
                Uuid::nil(),
                body.clone(),
                body.clone(),
                &headers,
                Duration::ZERO,
                12_000,
            );
            keepalive.on_response_completed(
                &ordinary_response,
                &principal,
                "accounting-key".to_owned(),
                &ordinary,
                Uuid::nil(),
                body.clone(),
                body,
                &headers,
                Duration::ZERO,
                12_000,
            );
            drop(shard);
            let operations = tokio::time::timeout(Duration::from_secs(5), async {
                let mut operations = Vec::new();
                loop {
                    let event = received.recv().await.expect("completion event");
                    let terminal = matches!(event, OrderedCompletion::Enqueue(..));
                    operations.push(event);
                    if terminal {
                        break operations;
                    }
                }
            })
            .await
            .expect("legitimate completion must reach durable enqueue");
            assert_eq!(
                operations,
                vec![
                    OrderedCompletion::Observe(expected_key.clone()),
                    OrderedCompletion::Enqueue(expected_key, Some(41)),
                ]
            );
        }
    }

    #[test]
    fn keepalive_eligibility_reuses_cached_json_for_unchanged_bytes() {
        let cached = serde_json::json!({"tool_choice": {"type": "auto"}});
        // Invalid bytes prove the unchanged path never parses them again.
        let original = Bytes::from_static(b"already parsed at the request boundary");
        for shaped in [original.clone(), Bytes::copy_from_slice(&original)] {
            let value = keepalive_eligibility_request_json(&cached, &original, &shaped)
                .expect("cached JSON");
            assert!(matches!(value, Cow::Borrowed(value) if std::ptr::eq(value, &cached)));
        }
    }

    #[test]
    fn keepalive_eligibility_parses_changed_shape_instead_of_original_fields() {
        let cached = serde_json::json!({"tool_choice": {"type": "auto"}});
        let original = Bytes::from_static(br#"{"tool_choice":{"type":"auto"}}"#);
        let shaped = Bytes::from_static(br#"{"tool_choice":{"type":"any"}}"#);
        let value =
            keepalive_eligibility_request_json(&cached, &original, &shaped).expect("shaped JSON");
        assert!(matches!(&value, Cow::Owned(_)));
        assert_eq!(
            zero_token_renewal_unsupported_reason(&value).map(|reason| reason.to_string()),
            Some("zero-token renewal unsupported: forced tool_choice".to_owned()),
        );
        assert!(
            keepalive_eligibility_request_json(
                &cached,
                &original,
                &Bytes::from_static(b"invalid shaped JSON"),
            )
            .is_err(),
            "invalid changed bytes must not fall back to original eligibility"
        );
    }

    #[test]
    fn keepalive_zero_token_eligibility_has_stable_reasons_and_preserves_controls() {
        for (request, expected) in [
            (
                serde_json::json!({"thinking": {"type": "enabled"}}),
                Some("enabled thinking"),
            ),
            (
                serde_json::json!({"output_config": {"format": null}}),
                Some("structured output"),
            ),
            (
                serde_json::json!({"tool_choice": {"type": "any"}}),
                Some("forced tool_choice"),
            ),
            (
                serde_json::json!({"tool_choice": {"type": "tool", "name": "Bash"}}),
                Some("forced tool_choice"),
            ),
            (
                serde_json::json!({"thinking": {"type": "adaptive"}, "output_config": {"effort": "high"}, "tool_choice": {"type": "auto"}, "stream": true}),
                None,
            ),
            (
                serde_json::json!({"thinking": {"type": "disabled"}, "tool_choice": {"type": "none"}}),
                None,
            ),
            (serde_json::json!({}), None),
        ] {
            assert_eq!(
                zero_token_renewal_unsupported_reason(&request).map(|reason| reason.to_string()),
                expected.map(|feature| format!("zero-token renewal unsupported: {feature}")),
            );
        }
    }

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
    fn emitted_sse_batch_observes_every_transformed_content_block() {
        let mut response = StreamingKeepaliveResponse::default();
        response.observe_emitted(
            b"event: message_start\ndata: {\"message\":{\"content\":[]}}\n\n\
              event: content_block_start\ndata: {\"content_block\":{\"type\":\"tool_use\",\"name\":\"question\"}}\n\n\
              event: content_block_start\ndata: {\"content_block\":{\"type\":\"tool_use\",\"name\":\"ask\"}}\n\n\
              event: message_delta\ndata: {\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n",
            1024,
        );
        response.observe_emitted(b"", 1024);
        let value = response.into_value().expect("emitted message");
        assert_eq!(
            value["content"],
            serde_json::json!([
                {"type": "tool_use", "name": "question"},
                {"type": "tool_use", "name": "ask"}
            ])
        );
        assert_eq!(value["stop_reason"], "tool_use");
    }
    #[test]
    fn emitted_sse_chunks_reassemble_partial_events() {
        let mut response = StreamingKeepaliveResponse::default();
        response.observe_emitted(
            b"event: message_start\ndata: {\"message\":{\"content\":[]}}\n\n\
              event: content_block_start\ndata: {\"content_block\":{\"type\":\"tool_use\",\"name\":\"que",
            1024,
        );
        response.observe_emitted(
            b"stion\"}}\n\n\
              event: message_delta\ndata: {\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n",
            1024,
        );

        let value = response
            .into_value()
            .expect("partial emitted event reassembled");
        assert_eq!(value["content"][0]["name"], "question");
        assert_eq!(value["stop_reason"], "tool_use");
    }

    #[test]
    fn emitted_sse_partial_buffer_disables_observation_at_budget() {
        let mut response = StreamingKeepaliveResponse::default();
        response.observe_emitted(b"data: ", 4);
        response.observe_emitted(b"xxxxx", 4);

        assert!(response.sse_observation_disabled);
        assert!(response.pending_sse_bytes.is_empty());
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
                "{}/thread:thread-1/agent:agent-a",
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
                "{}/prefix:prefix-1/agent:agent-a",
                cache_keepalive_principal_uuid("principal-test")
            )
        );
    }

    #[test]
    fn serving_bundle_prefix_is_threadless_keepalive_fallback_but_thread_wins() {
        let principal = Principal {
            id: "principal-test".to_owned(),
            kind: cc_lb_domain::PrincipalKind::InternalKey,
        };
        let mut metadata = RequestCacheMetadata::default();
        metadata.cache_prefix_hash = Some("serving-custom-prefix".to_owned());
        assert_eq!(
            cache_keepalive_session_key(&principal, &metadata)
                .expect("threadless serving request has a session key")
                .to_string(),
            format!(
                "{}/prefix:serving-custom-prefix",
                cache_keepalive_principal_uuid("principal-test")
            )
        );
        metadata.thread_id = Some("thread-serving".to_owned());
        assert_eq!(
            cache_keepalive_session_key(&principal, &metadata)
                .expect("threaded serving request has a session key")
                .to_string(),
            format!(
                "{}/thread:thread-serving",
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

    #[test]
    fn keepalive_session_keys_isolate_agent_scopes() {
        let principal = Principal {
            id: "principal-test".to_owned(),
            kind: cc_lb_domain::PrincipalKind::InternalKey,
        };
        let mut main = RequestCacheMetadata::default();
        main.thread_id = Some("session-1".to_owned());
        let mut claude_subagent = RequestCacheMetadata::default();
        claude_subagent.thread_id = Some("session-1".to_owned());
        claude_subagent.claude_agent_id = Some("a70f-agent".to_owned());
        let mut generic_subagent = RequestCacheMetadata::default();
        generic_subagent.thread_id = Some("session-1".to_owned());
        generic_subagent.request_kind = Some("subagent".to_owned());
        let main_key = cache_keepalive_session_key(&principal, &main)
            .expect("main session key")
            .to_string();
        let claude_key = cache_keepalive_session_key(&principal, &claude_subagent)
            .expect("Claude subagent session key")
            .to_string();
        let generic_key = cache_keepalive_session_key(&principal, &generic_subagent)
            .expect("generic subagent session key")
            .to_string();

        assert_eq!(
            main_key,
            format!(
                "{}/thread:session-1",
                cache_keepalive_principal_uuid(&principal.id)
            )
        );
        assert_eq!(
            claude_key,
            format!(
                "{}/thread:session-1/agent:a70f-agent",
                cache_keepalive_principal_uuid(&principal.id)
            )
        );
        assert_eq!(
            generic_key,
            format!(
                "{}/thread:session-1/agent:subagent",
                cache_keepalive_principal_uuid(&principal.id)
            )
        );
        assert_ne!(main_key, claude_key);
        assert_ne!(main_key, generic_key);
        assert_ne!(claude_key, generic_key);

        let mut delimiter_main = RequestCacheMetadata::default();
        delimiter_main.thread_id = Some("session-1/agent:a70f-agent".to_owned());
        let delimiter_main_key = cache_keepalive_session_key(&principal, &delimiter_main)
            .expect("delimiter main session key")
            .to_string();
        assert_ne!(delimiter_main_key, claude_key);
    }

    #[test]
    fn auxiliary_request_kinds_are_not_keepalive_eligible() {
        for kind in [
            "recap",
            "compaction",
            "session_title",
            "auto_thinking",
            "notification",
            "side",
            "look_at",
        ] {
            assert!(!is_keepalive_eligible_request_kind(Some(kind)), "{kind}");
        }
        for kind in [
            Some("main"),
            Some("subagent"),
            Some("advisor"),
            Some("unknown"),
            None,
        ] {
            assert!(is_keepalive_eligible_request_kind(kind), "{kind:?}");
        }
    }
    struct TestSignerFactory;
    struct TestSigner;

    impl ApiKeyAwareSignerFactory for TestSignerFactory {
        fn with_router_choice(
            &self,
            _router_chosen_upstream_name: String,
        ) -> Arc<dyn SignerFactory> {
            Arc::new(Self)
        }
    }

    #[async_trait]
    impl SignerFactory for TestSignerFactory {
        async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
            Ok(Arc::new(TestSigner))
        }
    }

    #[async_trait]
    impl Signer for TestSigner {
        async fn sign(
            &self,
            shaped: ShapedRequest,
            capability: &mut SigningCapability,
        ) -> Result<cc_lb_upstream::SignedRequest, SignerError> {
            Ok(cc_lb_upstream::SignedRequest::from_shaped(
                shaped, capability,
            ))
        }

        async fn on_unauthorized(&self, _error: &UpstreamError) -> RetryDecision {
            RetryDecision::Fail
        }
    }
    #[tokio::test]
    async fn superseded_request_still_fences_before_dispatch_boundary() {
        assert_superseded_request_fences(false).await;
    }

    #[tokio::test]
    async fn dropped_newer_request_cannot_remove_older_request_fence() {
        assert_superseded_request_fences(true).await;
    }

    async fn assert_superseded_request_fences(drop_newer: bool) {
        struct RequestFenceEnqueuer {
            ordered: OrderedCompletionEnqueuer,
            active_generation: std::sync::atomic::AtomicBool,
        }

        #[async_trait]
        impl CacheKeepaliveEnqueuer for RequestFenceEnqueuer {
            async fn observe_session_generation(
                &self,
                key: &str,
            ) -> Result<Option<u64>, CacheKeepaliveEnqueueError> {
                self.ordered.observe_session_generation(key).await
            }

            async fn enqueue_cache_keepalive(
                &self,
                request: CacheKeepaliveEnqueueRequest,
            ) -> Result<(), CacheKeepaliveEnqueueError> {
                self.ordered.enqueue_cache_keepalive(request).await
            }

            async fn cancel_cache_keepalive(
                &self,
                request: CacheKeepaliveCancelRequest,
            ) -> Result<(), CacheKeepaliveEnqueueError> {
                assert_eq!(request.reason, CancelReason::NewRequest);
                assert_eq!(request.expected_generation, Some(41));
                self.active_generation.store(false, Ordering::SeqCst);
                self.ordered
                    .events
                    .send(OrderedCompletion::Cancel(
                        request.session_key_hash,
                        request.expected_generation,
                    ))
                    .expect("request fence observer");
                Ok(())
            }

            async fn record_cache_keepalive_not_tracked(
                &self,
                request: CacheKeepaliveNotTrackedRequest,
            ) -> Result<(), CacheKeepaliveEnqueueError> {
                self.ordered
                    .record_cache_keepalive_not_tracked(request)
                    .await
            }
        }

        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
                .await
                .expect("request fence auth storage"),
        );
        storage
            .initialize()
            .await
            .expect("request fence auth schema");
        let principal_record = cc_lb_storage_api::PrincipalStore::create(
            storage.as_ref(),
            cc_lb_storage_api::principal::PrincipalCreate {
                name: "request-fence".to_owned(),
                kind: StoragePrincipalKind::Machine,
                allowed_models: vec!["*".to_owned()],
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
                cache_keepalive: Some(CacheKeepaliveConfig {
                    enabled: true,
                    refresh_lead_time_5m_secs: 30,
                    refresh_lead_time_1h_secs: 300,
                    max_refreshes_per_session: 12,
                    max_total_duration_secs: 14_400,
                    snapshot_max_bytes: 524_288,
                    min_cache_tokens: 10_000,
                    classifier: ClassifierConfig::default(),
                }),
            },
            cc_lb_clock::unix_secs(clock.now()),
        )
        .await
        .expect("request fence principal");
        let generated = cc_lb_control::api_keys::secret::generate_new();
        cc_lb_storage_api::ManagedKeyStore::issue(
            storage.as_ref(),
            &principal_record.id.to_string(),
            &generated.key_id,
            cc_lb_storage_api::IssueParams {
                label: "request-fence".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: Vec::new(),
                secret_salt: generated.secret_salt,
                verify_hash: generated.verify_hash,
                last_4: generated.last_4,
                index_hash: generated.index_hash,
            },
        )
        .await
        .expect("request fence authentication key");
        let authn = cc_lb_control::api_keys::builtin_authn::BuiltinAuthn::new(
            Arc::new(cc_lb_control::api_keys::key_store::KeyStore::new(storage)),
            clock,
        );
        let mut auth_headers = HeaderMap::new();
        auth_headers.insert(
            "x-api-key",
            HeaderValue::from_str(generated.plaintext.expose()).expect("generated key header"),
        );
        let dynamic_view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(TestSignerFactory))
            .principal_view(Arc::new(PrincipalView::from_db(
                std::slice::from_ref(&principal_record),
                HashMap::new(),
            )))
            .build();
        let authenticated = crate::authn_rail::authenticate_first(
            &authn,
            &auth_headers,
            &dynamic_view.principal_view,
        )
        .await
        .expect("request fence authenticates through the real rail");
        let principal = Principal {
            id: principal_record.id.to_string(),
            kind: cc_lb_domain::PrincipalKind::InternalKey,
        };
        let mut metadata = RequestCacheMetadata::default();
        metadata.thread_id = Some("superseded-request".to_owned());
        metadata.cache_breakpoints = vec![cc_lb_request_log::RequestCacheBreakpoint {
            block_index: 0,
            source: cc_lb_request_log::RequestCacheBreakpointSource::System,
            path: "system[0]".to_owned(),
            message_index: None,
            ttl: Some("5m".to_owned()),
            prefix_hash: "request-fence-prefix".to_owned(),
            prefix_token_count: 12_000,
            lookback_prefixes: Vec::new(),
            token_estimate_source: None,
        }];
        let (events, mut received) = tokio::sync::mpsc::unbounded_channel();
        let enqueuer = Arc::new(RequestFenceEnqueuer {
            ordered: OrderedCompletionEnqueuer { events },
            active_generation: std::sync::atomic::AtomicBool::new(true),
        });
        let keepalive = LifecycleKeepalive::new(
            Some(Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>),
            Arc::new(DynamicViewHolder::new(dynamic_view)),
        );
        let key = cache_keepalive_session_key(&principal, &metadata).expect("request session");
        let held_shard = keepalive.session_shard(&key).lock().await;
        let mut first =
            Box::pin(keepalive.on_request_started(&authenticated, &principal, &metadata));
        std::future::poll_fn(|cx| {
            assert!(first.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        let first_sequence = keepalive.latest_issued_seq(&key);
        assert_eq!(first_sequence, 1);
        let mut newer = Some(Box::pin(keepalive.on_request_started(
            &authenticated,
            &principal,
            &metadata,
        )));
        std::future::poll_fn(|cx| {
            assert!(
                newer
                    .as_mut()
                    .expect("newer request")
                    .as_mut()
                    .poll(cx)
                    .is_pending()
            );
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(keepalive.latest_issued_seq(&key), first_sequence + 1);
        assert!(
            received.try_recv().is_err(),
            "the held shard prevents durable writes"
        );
        if drop_newer {
            drop(newer.take());
        }
        drop(held_shard);
        first.await;
        assert!(
            !enqueuer.active_generation.load(Ordering::SeqCst),
            "returning to actual dispatch must fence the queued generation even when a newer request issued a sequence"
        );
        assert_eq!(
            received.try_recv().expect("durable observe"),
            OrderedCompletion::Observe(key.to_string())
        );
        assert_eq!(
            received.try_recv().expect("durable cancellation"),
            OrderedCompletion::Cancel(key.to_string(), Some(41))
        );
        if let Some(newer) = newer {
            newer.await;
        }
    }

    #[tokio::test]
    async fn stale_compaction_completion_cannot_cancel_or_resurrect_newer_generation() {
        struct Journal {
            enqueued: std::sync::Mutex<Vec<CacheKeepaliveEnqueueRequest>>,
            cancelled: std::sync::Mutex<Vec<CacheKeepaliveCancelRequest>>,
            not_tracked: std::sync::Mutex<Vec<CacheKeepaliveNotTrackedRequest>>,
        }
        #[async_trait]
        impl CacheKeepaliveEnqueuer for Journal {
            async fn enqueue_cache_keepalive(
                &self,
                request: CacheKeepaliveEnqueueRequest,
            ) -> Result<(), CacheKeepaliveEnqueueError> {
                self.enqueued.lock().expect("enqueued lock").push(request);
                Ok(())
            }

            async fn cancel_cache_keepalive(
                &self,
                request: CacheKeepaliveCancelRequest,
            ) -> Result<(), CacheKeepaliveEnqueueError> {
                self.cancelled.lock().expect("cancelled lock").push(request);
                Ok(())
            }

            async fn record_cache_keepalive_not_tracked(
                &self,
                request: CacheKeepaliveNotTrackedRequest,
            ) -> Result<(), CacheKeepaliveEnqueueError> {
                self.not_tracked
                    .lock()
                    .expect("not-tracked lock")
                    .push(request);
                Ok(())
            }
        }

        let principal_record = StoragePrincipalRecord {
            id: Uuid::new_v4(),
            name: "stale-test".to_owned(),
            kind: StoragePrincipalKind::Machine,
            allowed_models: vec!["*".to_owned()],
            allowed_upstreams: Vec::new(),
            default_limits: Vec::<Limit>::new(),
            enabled: true,
            deleted_at_unix_secs: None,
            revision: 1,
            created_at_unix_secs: 0,
            updated_at_unix_secs: 0,
            router_terminal_strategy: Default::default(),
            cache_keepalive: Some(CacheKeepaliveConfig {
                enabled: true,
                refresh_lead_time_5m_secs: 30,
                refresh_lead_time_1h_secs: 300,
                max_refreshes_per_session: 12,
                max_total_duration_secs: 14_400,
                snapshot_max_bytes: 524_288,
                min_cache_tokens: 10_000,
                classifier: ClassifierConfig::default(),
            }),
        };
        let dynamic_view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(TestSignerFactory))
            .principal_view(Arc::new(PrincipalView::from_db(
                std::slice::from_ref(&principal_record),
                HashMap::new(),
            )))
            .build();
        let journal = Arc::new(Journal {
            enqueued: std::sync::Mutex::new(Vec::new()),
            cancelled: std::sync::Mutex::new(Vec::new()),
            not_tracked: std::sync::Mutex::new(Vec::new()),
        });
        let keepalive = LifecycleKeepalive::new(
            Some(Arc::clone(&journal) as Arc<dyn CacheKeepaliveEnqueuer>),
            Arc::new(DynamicViewHolder::new(dynamic_view)),
        );
        let principal = Principal {
            id: principal_record.id.to_string(),
            kind: cc_lb_domain::PrincipalKind::InternalKey,
        };
        let request_json = serde_json::json!({
            "model": "claude-test",
            "max_tokens": 32,
            "system": [{"type": "text", "text": "cached", "cache_control": {"type": "ephemeral", "ttl": "5m"}}],
            "messages": [{"role": "user", "content": "hello"}]
        });
        let request_body = Bytes::from(serde_json::to_vec(&request_json).expect("request JSON"));
        let mut metadata = RequestCacheMetadata::default();
        metadata.request_json = Some(request_json);
        metadata.thread_id = Some("stale-order".to_owned());
        metadata.cache_breakpoints = vec![cc_lb_request_log::RequestCacheBreakpoint {
            block_index: 0,
            source: cc_lb_request_log::RequestCacheBreakpointSource::System,
            path: "system[0]".to_owned(),
            message_index: None,
            ttl: Some("5m".to_owned()),
            prefix_hash: "stale-prefix".to_owned(),
            prefix_token_count: 12_000,
            lookback_prefixes: Vec::new(),
            token_estimate_source: None,
        }];
        let response_compaction = serde_json::json!({
            "type": "message",
            "content": [{"type": "compaction", "content": "summary"}],
            "stop_reason": "compaction",
            "usage": {"cache_read_input_tokens": 60_000}
        });
        let response_fresh = serde_json::json!({
            "type": "message",
            "content": [{"type": "tool_use", "name": "Bash", "id": "tool-1", "input": {}}],
            "stop_reason": "tool_use",
            "usage": {"cache_read_input_tokens": 60_000}
        });

        keepalive
            .persist_completion(
                2,
                &response_fresh,
                &principal,
                "key".to_owned(),
                &metadata,
                Uuid::new_v4(),
                request_body.clone(),
                request_body.clone(),
                &HeaderMap::new(),
                Duration::from_secs(1),
                60_000,
            )
            .await;
        keepalive
            .persist_completion(
                1,
                &response_compaction,
                &principal,
                "key".to_owned(),
                &metadata,
                Uuid::new_v4(),
                request_body.clone(),
                request_body.clone(),
                &HeaderMap::new(),
                Duration::from_secs(1),
                60_000,
            )
            .await;
        assert_eq!(
            journal.not_tracked.lock().expect("not-tracked lock").len(),
            1
        );
        assert!(journal.cancelled.lock().expect("cancelled lock").is_empty());
        assert_eq!(journal.enqueued.lock().expect("enqueued lock").len(), 1);

        journal.enqueued.lock().expect("enqueued lock").clear();
        journal.cancelled.lock().expect("cancelled lock").clear();
        journal
            .not_tracked
            .lock()
            .expect("not-tracked lock")
            .clear();
        keepalive
            .persist_completion(
                4,
                &response_compaction,
                &principal,
                "key".to_owned(),
                &metadata,
                Uuid::new_v4(),
                request_body.clone(),
                request_body.clone(),
                &HeaderMap::new(),
                Duration::from_secs(1),
                60_000,
            )
            .await;
        keepalive
            .persist_completion(
                3,
                &response_fresh,
                &principal,
                "key".to_owned(),
                &metadata,
                Uuid::new_v4(),
                request_body.clone(),
                Bytes::new(),
                &HeaderMap::new(),
                Duration::from_secs(1),
                60_000,
            )
            .await;
        assert_eq!(
            journal.not_tracked.lock().expect("not-tracked lock").len(),
            1
        );
        assert_eq!(journal.cancelled.lock().expect("cancelled lock").len(), 1);
        assert!(journal.enqueued.lock().expect("enqueued lock").is_empty());
        for ordinary_response in [
            serde_json::json!({"stop_reason": "end_turn", "content": []}),
            serde_json::json!({"stop_reason": "max_tokens", "content": []}),
            response_fresh.clone(),
        ] {
            LifecycleKeepalive::assert_no_breakpoint_cannot_supersede_pending_completion(
                Arc::clone(&keepalive.dynamic_view),
                principal.clone(),
                metadata.clone(),
                request_body.clone(),
                ordinary_response,
            )
            .await;
        }

        let mut no_breakpoint = RequestCacheMetadata::default();
        no_breakpoint.thread_id = metadata.thread_id.clone();
        let before_enqueued = journal.enqueued.lock().expect("enqueued lock").len();
        let before_cancelled = journal.cancelled.lock().expect("cancelled lock").len();
        keepalive
            .persist_completion(
                1,
                &response_compaction,
                &principal,
                "key".to_owned(),
                &no_breakpoint,
                Uuid::nil(),
                Bytes::new(),
                Bytes::new(),
                &HeaderMap::new(),
                Duration::ZERO,
                0,
            )
            .await;
        assert_eq!(
            journal.enqueued.lock().expect("enqueued lock").len(),
            before_enqueued
        );
        assert_eq!(
            journal.cancelled.lock().expect("cancelled lock").len(),
            before_cancelled,
            "stale no-breakpoint compaction cannot cancel a newer completion"
        );
    }
}
