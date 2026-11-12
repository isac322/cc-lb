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

use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{CacheKeepaliveCompletionTicket, CacheKeepaliveConfigSnapshot, CacheTtl};

use crate::lifecycle::RequestCacheMetadata;
use crate::usage_parser::ParsedSseEvent;

use super::display_reason::CacheKeepaliveDisplayReason;
use super::session_key::SessionId;
use super::{CancelReason, HeuristicClassifier, RequestSnapshot, SessionKey};

#[derive(Clone)]
pub struct CacheKeepaliveEnqueueRequest {
    pub session_key_hash: String,
    pub principal_id: String,
    pub accounting_key_id: Option<String>,
    pub cycle_id: Uuid,
    pub cache_anchor_age: Duration,
    /// Ticket for this identity, acquired at start or ordinary completion.
    pub expected_generation: Option<CacheKeepaliveCompletionTicket>,
    pub params: super::ScheduleParams,
    pub display_reason: String,
    pub config_snapshot: CacheKeepaliveConfigSnapshot,
    pub snapshot: RequestSnapshot,
}

#[derive(Clone, Debug)]
pub struct CacheKeepaliveCancelRequest {
    pub session_key_hash: String,
    pub reason: CancelReason,
    /// Request-start ticket for owned completions; no new completion ticket.
    pub expected_generation: Option<CacheKeepaliveCompletionTicket>,
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
    ) -> Result<Option<CacheKeepaliveCompletionTicket>, CacheKeepaliveEnqueueError> {
        let _ = session_key_hash;
        Ok(None)
    }
}

/// Ownership acquired before dispatch, valid only for its original cache identity.
#[derive(Clone, Debug)]
pub(crate) struct RequestStartOrder {
    session_key: SessionKey,
    sequence: u64,
    expected_generation: Option<CacheKeepaliveCompletionTicket>,
    sibling_generation: Option<CacheKeepaliveCompletionTicket>,
}

enum CompletionDisposition {
    Handled(CancelReason),
    NotRenewable,
}

struct UnconsumedRequestOwner(Option<RequestStartOrder>);

impl Drop for UnconsumedRequestOwner {
    fn drop(&mut self) {
        if let Some(order) = self.0.as_ref() {
            tracing::debug!(
                target: "cache_keepalive",
                session_key_hash = %order.session_key,
                completion_seq = order.sequence,
                "completion owner dropped without a terminal outcome; retaining neutral deadline"
            );
        }
    }
}

pub(crate) struct LifecycleKeepaliveContext {
    principal: Principal,
    pub(crate) request_dispatched_at: Instant,
    request_start_order: UnconsumedRequestOwner,
    renewal: Option<LifecycleKeepaliveRenewal>,
    compaction_candidate: Option<(SessionKey, bool)>,
}

struct LifecycleKeepaliveRenewal {
    accounting_key_id: String,
    cache_metadata: RequestCacheMetadata,
    upstream_id: Uuid,
    shaped_body: Bytes,
    original_body: Bytes,
    downstream_headers: HeaderMap,
}

impl LifecycleKeepaliveContext {
    pub(crate) fn new(
        principal: Principal,
        request_dispatched_at: Instant,
        request_start_order: Option<RequestStartOrder>,
    ) -> Self {
        Self {
            principal,
            request_dispatched_at,
            request_start_order: UnconsumedRequestOwner(request_start_order),
            renewal: None,
            compaction_candidate: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_renewal(
        mut self,
        accounting_key_id: String,
        cache_metadata: RequestCacheMetadata,
        upstream_id: Uuid,
        shaped_body: Bytes,
        original_body: Bytes,
        downstream_headers: HeaderMap,
    ) -> Self {
        self.renewal = Some(LifecycleKeepaliveRenewal {
            accounting_key_id,
            cache_metadata,
            upstream_id,
            shaped_body,
            original_body,
            downstream_headers,
        });
        self
    }

    pub(crate) fn with_compaction_candidate(mut self, metadata: &RequestCacheMetadata) -> Self {
        if !metadata.explicit_auxiliary
            && is_keepalive_eligible_request_kind(metadata.request_kind.as_deref())
        {
            self.compaction_candidate = cache_keepalive_session_key(&self.principal, metadata)
                .map(|key| (key, allows_sibling_backstop(metadata)));
        }
        self
    }

    pub(crate) fn response_observer(&self) -> Option<StreamingKeepaliveResponse> {
        self.observes_response()
            .then(|| StreamingKeepaliveResponse {
                compaction_only: self.renewal.is_none(),
                ..StreamingKeepaliveResponse::default()
            })
    }

    pub(crate) fn observes_response(&self) -> bool {
        self.renewal.is_some() || self.compaction_candidate.is_some()
    }

    pub(crate) fn complete(
        mut self,
        keepalive: &LifecycleKeepalive,
        response: Option<&Value>,
        cache_anchor_age: Duration,
        cached_tokens: u64,
    ) {
        let request_start_order = self.request_start_order.0.take();
        if let (Some(renewal), Some(response)) = (self.renewal, response) {
            keepalive.on_response_completed(
                response,
                &self.principal,
                renewal.accounting_key_id,
                &renewal.cache_metadata,
                renewal.upstream_id,
                renewal.shaped_body,
                renewal.original_body,
                &renewal.downstream_headers,
                cache_anchor_age,
                cached_tokens,
                request_start_order,
            );
        } else if let Some(order) = request_start_order {
            keepalive.on_completed_not_renewable(self.principal, order, cache_anchor_age);
        } else if let Some((session_key, allow_sibling_backstop)) = self.compaction_candidate
            && response.is_some_and(HeuristicClassifier::is_compaction_boundary)
        {
            keepalive.on_compaction_completed(
                self.principal,
                session_key,
                allow_sibling_backstop,
                cache_anchor_age,
            );
        }
    }
}

const SESSION_SHARD_COUNT: usize = 64;
const MAX_TRACKED_SESSIONS_PER_SHARD: usize = 512;

#[cfg(test)]
#[path = "lifecycle_glue/completed_not_renewable_tests.rs"]
mod completed_not_renewable_tests;

#[derive(Clone)]
pub(crate) struct LifecycleKeepalive {
    enqueuer: Option<Arc<dyn CacheKeepaliveEnqueuer>>,
    dynamic_view: Arc<DynamicViewHolder>,
    next_completion_seq: Arc<AtomicU64>,
    issued_shards: Arc<[std::sync::Mutex<HashMap<SessionKey, u64>>; SESSION_SHARD_COUNT]>,
    session_shards: Arc<[tokio::sync::Mutex<HashMap<SessionKey, u64>>; SESSION_SHARD_COUNT]>,
}

fn allows_sibling_backstop(cache_metadata: &RequestCacheMetadata) -> bool {
    cache_metadata.claude_agent_id.is_none()
        && !cache_metadata.explicit_auxiliary
        && cache_metadata.explicit_request_class.is_none()
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
    ) -> Option<RequestStartOrder> {
        let enqueuer = self.enqueuer.as_ref()?;
        if cache_metadata.explicit_auxiliary
            || !is_keepalive_eligible_request_kind(cache_metadata.request_kind.as_deref())
            || (cache_metadata.cache_breakpoints.is_empty()
                && cache_metadata.cache_keepalive_surface.is_none())
        {
            return None;
        }
        let view = self.dynamic_view.load();
        let config = view
            .principal_view
            .get(&principal.id)
            .and_then(|cached| cached.cache_keepalive())
            .filter(|config| config.enabled)?;
        if cache_metadata.cache_keepalive_surface.as_deref() == Some("responses")
            && !config.classifier.openai_responses_enabled
        {
            return None;
        }
        let session_key = cache_keepalive_session_key(principal, cache_metadata)?;
        let sequence = self.next_completion_seq.fetch_add(1, Ordering::Relaxed) + 1;
        self.record_issued_seq(&session_key, sequence);
        let _shard = self.session_shard(&session_key).lock().await;
        // Issuance fences older completions, but only completion claims this
        // ordinal: the request must retain it until its response completes.
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
                return None;
            }
        };
        let sibling_generation = self
            .cancel_session(
                &session_key,
                principal,
                CancelReason::NewRequest,
                expected_generation,
                Duration::ZERO,
                allows_sibling_backstop(cache_metadata),
                None,
            )
            .await;
        drop(_shard);
        Some(RequestStartOrder {
            session_key,
            sequence,
            expected_generation,
            sibling_generation,
        })
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
        request_start_order: Option<RequestStartOrder>,
    ) {
        if self.enqueuer.is_none() {
            return;
        }
        if cache_metadata.explicit_auxiliary
            || !is_keepalive_eligible_request_kind(cache_metadata.request_kind.as_deref())
        {
            if let Some(order) = request_start_order {
                self.on_completed_not_renewable(principal.clone(), order, cache_anchor_age);
            }
            return;
        }
        let view = self.dynamic_view.load();
        let Some(config) = view
            .principal_view
            .get(&principal.id)
            .and_then(|cached| cached.cache_keepalive())
            .filter(|config| config.enabled)
        else {
            if let Some(order) = request_start_order {
                self.on_completed_not_renewable(principal.clone(), order, cache_anchor_age);
            }
            return;
        };
        if cache_metadata.cache_keepalive_surface.as_deref() == Some("responses")
            && !config.classifier.openai_responses_enabled
        {
            if let Some(order) = request_start_order {
                self.on_completed_not_renewable(principal.clone(), order, cache_anchor_age);
            }
            return;
        }
        let compaction_boundary =
            super::classifier::HeuristicClassifier::is_compaction_boundary(response_body_json);
        let session_key = cache_keepalive_session_key(principal, cache_metadata).filter(|_| {
            !cache_metadata.cache_breakpoints.is_empty()
                || compaction_boundary
                || cache_metadata.cache_keepalive_surface.is_some()
        });
        let (request_start_order, changed_identity_order) = match request_start_order {
            Some(order) if Some(&order.session_key) != session_key.as_ref() => (None, Some(order)),
            order => (order, None),
        };
        let completion_seq = session_key.as_ref().map(|session_key| {
            request_start_order.as_ref().map_or_else(
                || {
                    let sequence = self.next_completion_seq.fetch_add(1, Ordering::Relaxed) + 1;
                    self.record_issued_seq(session_key, sequence);
                    sequence
                },
                |order| order.sequence,
            )
        });
        let Some(completion_seq) = completion_seq else {
            if let Some(order) = changed_identity_order {
                let this = self.clone();
                let principal = principal.clone();
                tokio::spawn(async move {
                    this.retire_completed_owners(
                        &principal,
                        &order,
                        cache_anchor_age,
                        CancelReason::SessionIdentityChanged,
                    )
                    .await;
                });
            }
            return;
        };
        let this = self.clone();
        let response_body_json = response_body_json.clone();
        let principal = principal.clone();
        let cache_metadata = cache_metadata.clone();
        let downstream_headers = downstream_headers.clone();
        tokio::spawn(async move {
            if let Some(order) = changed_identity_order {
                this.retire_completed_owners(
                    &principal,
                    &order,
                    cache_anchor_age,
                    CancelReason::SessionIdentityChanged,
                )
                .await;
            }
            let disposition = this
                .persist_completion(
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
                    request_start_order.as_ref(),
                )
                .await;
            let Some(order) = request_start_order.as_ref() else {
                return;
            };
            let CompletionDisposition::Handled(sibling_reason) = disposition else {
                this.retire_completed_owners(
                    &principal,
                    order,
                    cache_anchor_age,
                    CancelReason::CompletedNotRenewable,
                )
                .await;
                return;
            };
            // Primary and sibling ownership have independent freshness fences.
            if this
                .cancel_sibling_session(
                    &order.session_key,
                    &principal,
                    sibling_reason,
                    cache_anchor_age,
                    true,
                    Some(order),
                )
                .await
                .is_some()
                && sibling_reason == CancelReason::SessionIdentityChanged
            {
                super::record_cancelled(&principal.id, sibling_reason);
            }
        });
    }

    pub(crate) fn on_completed_not_renewable(
        &self,
        principal: Principal,
        order: RequestStartOrder,
        cache_anchor_age: Duration,
    ) {
        if self.enqueuer.is_none() {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            this.retire_completed_owners(
                &principal,
                &order,
                cache_anchor_age,
                CancelReason::CompletedNotRenewable,
            )
            .await;
        });
    }

    async fn retire_completed_owners(
        &self,
        principal: &Principal,
        order: &RequestStartOrder,
        cache_anchor_age: Duration,
        reason: CancelReason,
    ) {
        // The completed attempt no longer owns a renewable identity here.
        // Its retained ticket must never fence a replacement identity.
        let _shard = self.session_shard(&order.session_key).lock().await;
        let _ = self
            .cancel_session(
                &order.session_key,
                principal,
                reason,
                order.expected_generation,
                cache_anchor_age,
                true,
                Some(order),
            )
            .await;
        super::record_cancelled(&principal.id, reason);
    }

    fn on_compaction_completed(
        &self,
        principal: Principal,
        session_key: SessionKey,
        allow_sibling_backstop: bool,
        cache_anchor_age: Duration,
    ) {
        if self.enqueuer.is_none()
            || !self
                .dynamic_view
                .load()
                .principal_view
                .get(&principal.id)
                .and_then(|cached| cached.cache_keepalive())
                .is_some_and(|config| config.enabled)
        {
            return;
        }
        let sequence = self.next_completion_seq.fetch_add(1, Ordering::Relaxed) + 1;
        self.record_issued_seq(&session_key, sequence);
        let this = self.clone();
        tokio::spawn(async move {
            if this
                .dynamic_view
                .load()
                .principal_view
                .get(&principal.id)
                .and_then(|cached| cached.cache_keepalive())
                .is_some_and(|config| config.enabled)
            {
                this.persist_compaction_boundary(
                    &principal,
                    &session_key,
                    sequence,
                    cache_anchor_age,
                    allow_sibling_backstop,
                    None,
                )
                .await;
            }
        });
    }

    async fn persist_compaction_boundary(
        &self,
        principal: &Principal,
        session_key: &SessionKey,
        completion_seq: u64,
        cache_anchor_age: Duration,
        allow_sibling_backstop: bool,
        request_start_order: Option<&RequestStartOrder>,
    ) -> CompletionDisposition {
        let Some(enqueuer) = self.enqueuer.as_ref() else {
            return CompletionDisposition::NotRenewable;
        };
        let mut shard = self.session_shard(session_key).lock().await;
        let is_latest = self.claim_completion_seq(&mut shard, session_key, completion_seq);
        if request_start_order.is_some() && !is_latest {
            return CompletionDisposition::Handled(CancelReason::SessionIdentityChanged);
        }
        let expected_generation = if let Some(order) = request_start_order {
            order.expected_generation
        } else {
            match enqueuer
                .observe_session_generation(&session_key.to_string())
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
                    return CompletionDisposition::NotRenewable;
                }
            }
        };
        if is_latest {
            let _ = self
                .cancel_session(
                    session_key,
                    principal,
                    CancelReason::UserTurnDetected,
                    expected_generation,
                    cache_anchor_age,
                    allow_sibling_backstop,
                    request_start_order,
                )
                .await;
            super::record_cancelled(&principal.id, CancelReason::UserTurnDetected);
        }
        CompletionDisposition::Handled(CancelReason::UserTurnDetected)
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
        request_start_order: Option<&RequestStartOrder>,
    ) -> CompletionDisposition {
        let Some(enqueuer) = self.enqueuer.as_ref() else {
            return CompletionDisposition::NotRenewable;
        };
        if cache_metadata.explicit_auxiliary
            || !is_keepalive_eligible_request_kind(cache_metadata.request_kind.as_deref())
        {
            return CompletionDisposition::NotRenewable;
        }
        let view = self.dynamic_view.load();
        let Some(cached) = view.principal_view.get(&principal.id) else {
            return CompletionDisposition::NotRenewable;
        };
        let Some(config) = cached.cache_keepalive().filter(|config| config.enabled) else {
            return CompletionDisposition::NotRenewable;
        };
        let Some(session_key) = cache_keepalive_session_key(principal, cache_metadata) else {
            return CompletionDisposition::NotRenewable;
        };
        // Captured siblings are retired once at the detached completion boundary.
        let allow_sibling_backstop =
            request_start_order.is_none() && allows_sibling_backstop(cache_metadata);
        let compaction_boundary = HeuristicClassifier::is_compaction_boundary(response_body_json);
        if compaction_boundary
            && cache_metadata.cache_breakpoints.is_empty()
            && cache_metadata.cache_keepalive_surface.is_none()
        {
            return self
                .persist_compaction_boundary(
                    principal,
                    &session_key,
                    completion_seq,
                    cache_anchor_age,
                    allow_sibling_backstop,
                    request_start_order,
                )
                .await;
        }
        let Some(request_json) = cache_metadata.request_json.as_ref() else {
            return CompletionDisposition::NotRenewable;
        };
        let ttl = cache_metadata.cache_keepalive_ttl.unwrap_or_else(|| {
            cache_metadata
                .cache_breakpoints
                .iter()
                .map(|breakpoint| CacheTtl::from_ttl_str(breakpoint.ttl.as_deref()))
                .min_by_key(|ttl| ttl.as_secs())
                .unwrap_or(CacheTtl::Ttl5m)
        });
        let principal_name: Arc<str> = Arc::from(principal.id.as_str());
        let session_key_hash = session_key.to_string();
        let mut shard = self.session_shard(&session_key).lock().await;
        let is_latest = self.claim_completion_seq(&mut shard, &session_key, completion_seq);
        if request_start_order.is_some() && !is_latest {
            return CompletionDisposition::Handled(CancelReason::SessionIdentityChanged);
        }
        let expected_generation = if let Some(order) = request_start_order {
            order.expected_generation
        } else {
            match enqueuer.observe_session_generation(&session_key_hash).await {
                Ok(expected) => expected,
                Err(error) => {
                    tracing::warn!(
                        target: "cache_keepalive",
                        principal_id = principal.id.as_str(),
                        %error,
                        "skipping cache keep-alive completion: failed to observe session generation"
                    );
                    return CompletionDisposition::NotRenewable;
                }
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
                    return CompletionDisposition::NotRenewable;
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
                let _ = self
                    .cancel_session(
                        &session_key,
                        principal,
                        CancelReason::ZeroTokenRenewalUnsupported,
                        expected_generation,
                        cache_anchor_age,
                        allow_sibling_backstop,
                        request_start_order,
                    )
                    .await;
                super::record_cancelled(&principal_name, CancelReason::ZeroTokenRenewalUnsupported);
            }
            return CompletionDisposition::Handled(CancelReason::ZeroTokenRenewalUnsupported);
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
                    keepalive_forward_headers(
                        downstream_headers,
                        view.upstreams_snapshot()
                            .iter()
                            .find(|upstream| upstream.id == upstream_id)
                            .map(|upstream| upstream.kind),
                    ),
                    shaped_body,
                    upstream_id,
                    ttl,
                    config.snapshot_max_bytes as usize,
                )
            } else {
                RequestSnapshot::capture(
                    url,
                    http::Method::POST,
                    keepalive_forward_headers(downstream_headers, None),
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
                    let _ = self
                        .cancel_session(
                            &session_key,
                            principal,
                            CancelReason::SnapshotTooLarge,
                            expected_generation,
                            cache_anchor_age,
                            allow_sibling_backstop,
                            request_start_order,
                        )
                        .await;
                    super::record_cancelled(&principal_name, CancelReason::SnapshotTooLarge);
                }
                return CompletionDisposition::Handled(CancelReason::SnapshotTooLarge);
            }
            Err(error) => {
                tracing::warn!(
                    target: "cache_keepalive",
                    principal_id = principal.id.as_str(),
                    %error,
                    "skipping cache keep-alive snapshot capture"
                );
                return CompletionDisposition::NotRenewable;
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
                let min_cache_tokens = if cache_metadata.cache_keepalive_surface.is_some() {
                    super::openai::MIN_PREFIX_TOKENS
                } else {
                    config.min_cache_tokens
                };
                if cached_tokens < min_cache_tokens {
                    let reason = CacheKeepaliveDisplayReason::CachedPrefixTooSmall {
                        cached_tokens,
                        min_cache_tokens,
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
                        let _ = self
                            .cancel_session(
                                &session_key,
                                principal,
                                CancelReason::CachedPrefixTooSmall,
                                expected_generation,
                                cache_anchor_age,
                                allow_sibling_backstop,
                                request_start_order,
                            )
                            .await;
                        super::record_cancelled(
                            &principal_name,
                            CancelReason::CachedPrefixTooSmall,
                        );
                    }
                    return CompletionDisposition::Handled(CancelReason::CachedPrefixTooSmall);
                }
                if !is_latest {
                    return CompletionDisposition::Handled(CancelReason::SessionIdentityChanged);
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
                    let _ = self
                        .cancel_session(
                            &session_key,
                            principal,
                            CancelReason::RetentionRecorded,
                            expected_generation,
                            cache_anchor_age,
                            allow_sibling_backstop,
                            request_start_order,
                        )
                        .await;
                    return CompletionDisposition::Handled(CancelReason::RetentionRecorded);
                }
                if cache_metadata.cache_keepalive_surface.is_some()
                    && !super::openai::should_schedule_refresh(
                        snapshot.keepalive_model(),
                        match ttl {
                            CacheTtl::Ttl30m => super::openai::Lifetime::ThirtyMinutes,
                            CacheTtl::Ttl12h => super::openai::Lifetime::TwelveHours,
                            _ => super::openai::Lifetime::ThirtyMinutes,
                        },
                        config.max_refreshes_per_session,
                    )
                {
                    self.record_not_tracked(
                        &session_key,
                        principal,
                        upstream_id,
                        ttl,
                        config,
                        "refresh expected value is unknown or non-positive",
                    )
                    .await;
                    let _ = self
                        .cancel_session(
                            &session_key,
                            principal,
                            CancelReason::RenewalNotBeneficial,
                            expected_generation,
                            cache_anchor_age,
                            allow_sibling_backstop,
                            request_start_order,
                        )
                        .await;
                    return CompletionDisposition::Handled(CancelReason::RenewalNotBeneficial);
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
                    return CompletionDisposition::NotRenewable;
                }
                CompletionDisposition::Handled(CancelReason::SessionIdentityChanged)
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
                    let _ = self
                        .cancel_session(
                            &session_key,
                            principal,
                            CancelReason::UserTurnDetected,
                            expected_generation,
                            cache_anchor_age,
                            allow_sibling_backstop,
                            request_start_order,
                        )
                        .await;
                }
                CompletionDisposition::Handled(CancelReason::UserTurnDetected)
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
                    let _ = self
                        .cancel_session(
                            &session_key,
                            principal,
                            CancelReason::AmbiguousTurn,
                            expected_generation,
                            cache_anchor_age,
                            allow_sibling_backstop,
                            request_start_order,
                        )
                        .await;
                }
                CompletionDisposition::Handled(CancelReason::AmbiguousTurn)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn cancel_session(
        &self,
        session_key: &SessionKey,
        principal: &Principal,
        reason: CancelReason,
        expected_generation: Option<CacheKeepaliveCompletionTicket>,
        cache_anchor_age: Duration,
        allow_sibling_backstop: bool,
        request_start_order: Option<&RequestStartOrder>,
    ) -> Option<CacheKeepaliveCompletionTicket> {
        let enqueuer = self.enqueuer.as_ref()?;
        if let Err(error) = enqueuer
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
        self.cancel_sibling_session(
            session_key,
            principal,
            reason,
            cache_anchor_age,
            allow_sibling_backstop,
            request_start_order,
        )
        .await
    }

    async fn cancel_sibling_session(
        &self,
        session_key: &SessionKey,
        principal: &Principal,
        reason: CancelReason,
        cache_anchor_age: Duration,
        allow_sibling_backstop: bool,
        request_start_order: Option<&RequestStartOrder>,
    ) -> Option<CacheKeepaliveCompletionTicket> {
        let enqueuer = self.enqueuer.as_ref()?;
        if !allow_sibling_backstop {
            return None;
        }
        let SessionKey {
            principal_id,
            session_id: SessionId::ThreadId(thread_id),
            agent_scope: None,
        } = session_key
        else {
            return None;
        };
        let sibling_key =
            SessionKey::from_thread_and_agent(*principal_id, thread_id.clone(), "subagent");
        let sibling_generation = if let Some(order) = request_start_order {
            // A captured absence is not permission to acquire a new owner.
            order.sibling_generation
        } else {
            match enqueuer
                .observe_session_generation(&sibling_key.to_string())
                .await
            {
                Ok(generation) => generation,
                Err(error) => {
                    tracing::warn!(
                        target: "cache_keepalive",
                        principal_id = principal.id.as_str(),
                        reason = reason.as_str(),
                        %error,
                        "durable subagent cache keep-alive generation lookup failed"
                    );
                    None
                }
            }
        };
        let sibling_generation = sibling_generation?;
        if let Err(error) = enqueuer
            .cancel_cache_keepalive(CacheKeepaliveCancelRequest {
                session_key_hash: sibling_key.to_string(),
                reason,
                expected_generation: Some(sibling_generation),
                cache_anchor_age,
            })
            .await
        {
            tracing::warn!(
                target: "cache_keepalive",
                principal_id = principal.id.as_str(),
                reason = reason.as_str(),
                %error,
                "durable subagent cache keep-alive cancellation failed"
            );
        }
        Some(sibling_generation)
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
    openai_response: Option<Value>,
    content_blocks: Vec<Value>,
    stop_reason: Option<String>,
    pending_sse_bytes: Vec<u8>,
    sse_observation_disabled: bool,
    compaction_only: bool,
    compaction_observed: bool,
}

impl StreamingKeepaliveResponse {
    pub(crate) fn control_only() -> Self {
        Self {
            compaction_only: true,
            ..Self::default()
        }
    }

    pub(crate) fn observe_emitted(
        &mut self,
        bytes: &[u8],
        max_pending_sse_bytes: usize,
        mut observe_event: impl FnMut(&ParsedSseEvent<'_>),
    ) {
        if self.sse_observation_disabled {
            return;
        }
        let mut bytes = bytes;
        if !self.pending_sse_bytes.is_empty() {
            // The longest event delimiter is CRLF CRLF. Check its possible
            // boundary overlap before scanning the incoming chunk directly.
            let suffix_len = self.pending_sse_bytes.len().min(3);
            let prefix_len = bytes.len().min(3);
            let mut boundary = [0_u8; 6];
            boundary[..suffix_len].copy_from_slice(
                &self.pending_sse_bytes[self.pending_sse_bytes.len() - suffix_len..],
            );
            boundary[suffix_len..suffix_len + prefix_len].copy_from_slice(&bytes[..prefix_len]);
            let completing_len =
                crate::usage_parser::find_sse_event_end(&boundary[..suffix_len + prefix_len])
                    .filter(|end| *end > suffix_len)
                    .map(|end| end - suffix_len)
                    .or_else(|| crate::usage_parser::find_sse_event_end(bytes));
            let Some(completing_len) = completing_len else {
                if self.pending_sse_bytes.len().saturating_add(bytes.len()) > max_pending_sse_bytes
                {
                    self.pending_sse_bytes.clear();
                    self.sse_observation_disabled = true;
                } else {
                    self.pending_sse_bytes.extend_from_slice(bytes);
                }
                return;
            };

            // Join only the split event, not the rest of a potentially large
            // batch. Complete events are observed before budgeting the tail.
            let mut event_bytes = std::mem::take(&mut self.pending_sse_bytes);
            event_bytes.extend_from_slice(&bytes[..completing_len]);
            {
                let event = crate::usage_parser::parse_sse_event(&event_bytes);
                observe_event(&event);
                self.observe(&event);
            }
            if event_bytes.capacity() <= max_pending_sse_bytes {
                event_bytes.clear();
                self.pending_sse_bytes = event_bytes;
            }
            bytes = &bytes[completing_len..];
        }

        let mut offset = 0;
        while let Some(end) = crate::usage_parser::find_sse_event_end(&bytes[offset..]) {
            let end = offset + end;
            let event = crate::usage_parser::parse_sse_event(&bytes[offset..end]);
            observe_event(&event);
            self.observe(&event);
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
    }

    pub(crate) fn observe(&mut self, event: &ParsedSseEvent<'_>) {
        let Ok(Some(value)) = event.value() else {
            return;
        };
        if self.compaction_only {
            self.compaction_observed |= HeuristicClassifier::is_compaction_boundary(value)
                || value
                    .get("message")
                    .is_some_and(HeuristicClassifier::is_compaction_boundary)
                || value
                    .get("content_block")
                    .and_then(|block| block.get("type"))
                    .and_then(Value::as_str)
                    == Some("compaction")
                || value
                    .get("delta")
                    .and_then(|delta| delta.get("stop_reason"))
                    .and_then(Value::as_str)
                    == Some("compaction");
            return;
        }
        if matches!(
            value.get("type").and_then(Value::as_str),
            Some("response.completed" | "response.incomplete")
        ) {
            self.openai_response = value.get("response").cloned();
            return;
        }
        match event.event_name() {
            Some(b"response.completed" | b"response.incomplete") => {
                self.openai_response = value.get("response").cloned();
            }
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
        if self.compaction_only {
            return self
                .compaction_observed
                .then(|| serde_json::json!({"stop_reason": "compaction"}));
        }
        if self.openai_response.is_some() {
            return self.openai_response;
        }
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
const OPENAI_API_KEY_KEEPALIVE_FORWARD_HEADERS: &[&str] = &[
    "anthropic-version",
    "anthropic-beta",
    "openai-organization",
    "openai-project",
    "openai-processing-region",
];

fn keepalive_forward_headers(
    headers: &HeaderMap,
    responses_upstream_kind: Option<UpstreamKind>,
) -> HeaderMap {
    let names = if responses_upstream_kind == Some(UpstreamKind::OpenaiApiKey) {
        OPENAI_API_KEY_KEEPALIVE_FORWARD_HEADERS
    } else {
        KEEPALIVE_FORWARD_HEADERS
    };
    let mut out = HeaderMap::new();
    for name in names {
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
        let Some(boundary) = metadata.openai_selected_boundary.as_deref() else {
            return (None, None, None, None);
        };
        return (
            Some(openai_prefix_fingerprint(metadata, boundary)),
            metadata.cache_prefix_hash.clone(),
            Some(CACHE_PREFIX_POLICY_VERSION.to_owned()),
            Some(CACHE_PREFIX_SCHEMA_VERSION.to_owned()),
        );
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

fn openai_prefix_fingerprint(metadata: &RequestCacheMetadata, boundary: &str) -> String {
    let ttl = match metadata.cache_keepalive_ttl {
        Some(cc_lb_storage_api::CacheTtl::Ttl30m) => "30m",
        Some(cc_lb_storage_api::CacheTtl::Ttl12h) => "12h",
        Some(cc_lb_storage_api::CacheTtl::Ttl5m) => "5m",
        Some(cc_lb_storage_api::CacheTtl::Ttl1h) => "1h",
        None => "unknown",
    };
    let identity = metadata
        .openai_replay_identity
        .as_deref()
        .unwrap_or("unknown");
    let provenance = format!(
        "policy=openai-responses-prompt-cache-v1;schema={CACHE_PREFIX_SCHEMA_VERSION};selected={};ttl={ttl};identity={identity}",
        truncate_evidence_component(boundary, 128),
    );
    let digest = Sha256::digest(provenance.as_bytes());
    let digest = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{provenance};sha256={digest}")
}

fn truncate_evidence_component(value: &str, max_bytes: usize) -> &str {
    value.get(..max_bytes).unwrap_or(value)
}

pub(crate) fn follow_up_session_key_hash(
    principal: &Principal,
    cache_metadata: &RequestCacheMetadata,
) -> Option<String> {
    if cache_metadata.explicit_auxiliary
        || !is_keepalive_eligible_request_kind(cache_metadata.request_kind.as_deref())
        || (cache_metadata.thread_id.is_none()
            && !(cache_metadata.cache_keepalive_surface.as_deref() == Some("responses")
                && cache_metadata.openai_replay_identity.is_some()))
    {
        return None;
    }
    cache_keepalive_session_key(principal, cache_metadata).map(|key| key.to_string())
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
            SessionKey::from_thread_id(principal_id, thread_id)
                .with_agent_scope(agent_scope.clone()),
        );
    }
    if cache_metadata.cache_keepalive_surface.as_deref() == Some("responses")
        && let Some(identity) = cache_metadata.openai_replay_identity.clone()
    {
        return Some(
            SessionKey::from_cache_prefix_hash(principal_id, format!("openai-replay:{identity}"))
                .with_agent_scope(agent_scope.clone()),
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

    #[test]
    fn compaction_only_stream_observer_retains_no_renewal_snapshot() {
        for terminal in [
            b"event: message_delta\ndata: {\"delta\":{\"stop_reason\":\"compaction\"}}\n\n".as_slice(),
            b"event: content_block_start\ndata: {\"content_block\":{\"type\":\"compaction\",\"content\":\"summary\"}}\n\n".as_slice(),
        ] {
            for emitted in [false, true] {
                let mut response = StreamingKeepaliveResponse {
                    compaction_only: true,
                    ..StreamingKeepaliveResponse::default()
                };
                response.observe(&crate::usage_parser::parse_sse_event(
                    b"event: message_start\ndata: {\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"large response\"}],\"usage\":{\"input_tokens\":12}}}\n\n",
                ));
                if emitted {
                    let split = terminal.len() / 2;
                    response.observe_emitted(&terminal[..split], 1024, |_| {});
                    response.observe_emitted(&terminal[split..], 1024, |_| {});
                } else {
                    response.observe(&crate::usage_parser::parse_sse_event(terminal));
                }
                assert!(response.message.is_none());
                assert!(response.openai_response.is_none());
                assert!(response.content_blocks.is_empty());
                assert!(response.stop_reason.is_none());
                assert_eq!(response.into_value(), Some(serde_json::json!({"stop_reason": "compaction"})));
            }
        }
    }

    #[test]
    fn compaction_only_stream_observer_ignores_normal_tool_completion() {
        let mut response = StreamingKeepaliveResponse {
            compaction_only: true,
            ..StreamingKeepaliveResponse::default()
        };
        response.observe_emitted(
            b"event: message_start\ndata: {\"message\":{\"content\":[]}}\n\n\
              event: content_block_start\ndata: {\"content_block\":{\"type\":\"tool_use\",\"name\":\"Bash\"}}\n\n\
              event: message_delta\ndata: {\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n",
            1024,
            |_| {},
        );
        assert!(response.into_value().is_none());
    }

    fn completion_ticket(sequence: u64) -> CacheKeepaliveCompletionTicket {
        CacheKeepaliveCompletionTicket {
            epoch: Uuid::from_u128(1),
            sequence,
            valid_until_unix_secs: u64::MAX,
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    enum OrderedCompletion {
        Observe(String),
        Enqueue(String, Option<CacheKeepaliveCompletionTicket>),
        NotTracked(String),
        Cancel(String, Option<CacheKeepaliveCompletionTicket>),
    }

    struct OrderedCompletionEnqueuer {
        events: tokio::sync::mpsc::UnboundedSender<OrderedCompletion>,
    }

    #[async_trait]
    impl CacheKeepaliveEnqueuer for OrderedCompletionEnqueuer {
        async fn observe_session_generation(
            &self,
            key: &str,
        ) -> Result<Option<CacheKeepaliveCompletionTicket>, CacheKeepaliveEnqueueError> {
            self.events
                .send(OrderedCompletion::Observe(key.to_owned()))
                .expect("observer receiver");
            Ok(Some(completion_ticket(41)))
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
            assert!(matches!(
                request.reason,
                CancelReason::UserTurnDetected | CancelReason::SessionIdentityChanged
            ));
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
    #[derive(Default)]
    struct BackstopState {
        generations: std::sync::Mutex<HashMap<String, Option<CacheKeepaliveCompletionTicket>>>,
        cancellations: std::sync::Mutex<Vec<(String, Option<CacheKeepaliveCompletionTicket>)>>,
        fail_primary: bool,
    }

    struct BackstopEnqueuer {
        state: Arc<BackstopState>,
    }

    #[async_trait]
    impl CacheKeepaliveEnqueuer for BackstopEnqueuer {
        async fn enqueue_cache_keepalive(
            &self,
            _request: CacheKeepaliveEnqueueRequest,
        ) -> Result<(), CacheKeepaliveEnqueueError> {
            Ok(())
        }

        async fn cancel_cache_keepalive(
            &self,
            request: CacheKeepaliveCancelRequest,
        ) -> Result<(), CacheKeepaliveEnqueueError> {
            let is_primary = !request.session_key_hash.ends_with("/agent:subagent");
            self.state
                .cancellations
                .lock()
                .expect("cancellation state")
                .push((
                    request.session_key_hash.clone(),
                    request.expected_generation,
                ));
            if self.state.fail_primary && is_primary {
                Err(CacheKeepaliveEnqueueError(
                    "primary cancellation failed".to_owned(),
                ))
            } else {
                Ok(())
            }
        }

        async fn record_cache_keepalive_not_tracked(
            &self,
            _request: CacheKeepaliveNotTrackedRequest,
        ) -> Result<(), CacheKeepaliveEnqueueError> {
            Ok(())
        }

        async fn observe_session_generation(
            &self,
            session_key_hash: &str,
        ) -> Result<Option<CacheKeepaliveCompletionTicket>, CacheKeepaliveEnqueueError> {
            Ok(self
                .state
                .generations
                .lock()
                .expect("generation state")
                .get(session_key_hash)
                .copied()
                .flatten())
        }
    }

    fn backstop_keepalive(state: Arc<BackstopState>) -> LifecycleKeepalive {
        let view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(TestSignerFactory))
            .principal_view(Arc::new(PrincipalView::from_db(&[], HashMap::new())))
            .build();
        LifecycleKeepalive::new(
            Some(Arc::new(BackstopEnqueuer { state })),
            Arc::new(DynamicViewHolder::new(view)),
        )
    }

    #[tokio::test]
    async fn unscoped_cancel_backstops_durable_subagent_with_own_generation() {
        let state = Arc::new(BackstopState {
            fail_primary: true,
            ..BackstopState::default()
        });
        let principal_id = cache_keepalive_principal_uuid("principal-test");
        let primary = SessionKey::from_thread_id(principal_id, "thread-1");
        let sibling = SessionKey::from_thread_and_agent(principal_id, "thread-1", "subagent");
        state
            .generations
            .lock()
            .expect("generation state")
            .insert(sibling.to_string(), Some(completion_ticket(9)));
        let keepalive = backstop_keepalive(Arc::clone(&state));
        let _ = keepalive
            .cancel_session(
                &primary,
                &Principal {
                    id: "principal-test".to_owned(),
                    kind: cc_lb_domain::PrincipalKind::InternalKey,
                },
                CancelReason::UserTurnDetected,
                Some(completion_ticket(3)),
                Duration::ZERO,
                true,
                None,
            )
            .await;
        assert_eq!(
            *state.cancellations.lock().expect("cancellation state"),
            vec![
                (primary.to_string(), Some(completion_ticket(3))),
                (sibling.to_string(), Some(completion_ticket(9)))
            ]
        );
    }

    #[tokio::test]
    async fn owned_completion_reuses_sibling_start_ticket_after_newer_sibling_start() {
        let state = Arc::new(BackstopState::default());
        let principal = Principal {
            id: "principal-test".to_owned(),
            kind: cc_lb_domain::PrincipalKind::InternalKey,
        };
        let principal_id = cache_keepalive_principal_uuid(&principal.id);
        let primary = SessionKey::from_thread_id(principal_id, "owned-sibling");
        let sibling = SessionKey::from_thread_and_agent(principal_id, "owned-sibling", "subagent");
        state
            .generations
            .lock()
            .expect("generations")
            .insert(sibling.to_string(), Some(completion_ticket(9)));
        let keepalive = backstop_keepalive(Arc::clone(&state));
        let sibling_generation = keepalive
            .cancel_session(
                &primary,
                &principal,
                CancelReason::NewRequest,
                Some(completion_ticket(3)),
                Duration::ZERO,
                true,
                None,
            )
            .await;
        assert_eq!(sibling_generation, Some(completion_ticket(9)));
        state
            .generations
            .lock()
            .expect("generations")
            .insert(sibling.to_string(), Some(completion_ticket(10)));
        let _ = keepalive
            .cancel_session(
                &primary,
                &principal,
                CancelReason::UserTurnDetected,
                Some(completion_ticket(3)),
                Duration::ZERO,
                true,
                Some(&RequestStartOrder {
                    session_key: primary.clone(),
                    sequence: 1,
                    expected_generation: Some(completion_ticket(3)),
                    sibling_generation,
                }),
            )
            .await;
        assert_eq!(
            *state.cancellations.lock().expect("cancellations"),
            vec![
                (primary.to_string(), Some(completion_ticket(3))),
                (sibling.to_string(), Some(completion_ticket(9))),
                (primary.to_string(), Some(completion_ticket(3))),
                (sibling.to_string(), Some(completion_ticket(9))),
            ],
            "completion cannot acquire sibling ticket 10 after dispatch"
        );
        state.cancellations.lock().expect("cancellations").clear();
        let _ = keepalive
            .cancel_session(
                &primary,
                &principal,
                CancelReason::UserTurnDetected,
                Some(completion_ticket(3)),
                Duration::ZERO,
                true,
                Some(&RequestStartOrder {
                    session_key: primary.clone(),
                    sequence: 1,
                    expected_generation: Some(completion_ticket(3)),
                    sibling_generation: None,
                }),
            )
            .await;
        assert_eq!(
            *state.cancellations.lock().expect("cancellations"),
            vec![(primary.to_string(), Some(completion_ticket(3)))],
            "an uncaptured sibling must not acquire ticket 10 at completion"
        );
    }

    #[tokio::test]
    async fn cancel_backstop_skips_inactive_and_scoped_sessions_and_is_restart_safe() {
        let state = Arc::new(BackstopState::default());
        let principal_id = cache_keepalive_principal_uuid("principal-test");
        let primary = SessionKey::from_thread_id(principal_id, "thread-2");
        let sibling = SessionKey::from_thread_and_agent(principal_id, "thread-2", "subagent");
        let keepalive = backstop_keepalive(Arc::clone(&state));
        let _ = keepalive
            .cancel_session(
                &primary,
                &Principal {
                    id: "principal-test".to_owned(),
                    kind: cc_lb_domain::PrincipalKind::InternalKey,
                },
                CancelReason::NewRequest,
                None,
                Duration::ZERO,
                true,
                None,
            )
            .await;
        assert_eq!(
            *state.cancellations.lock().expect("cancellation state"),
            vec![(primary.to_string(), None)]
        );

        state
            .generations
            .lock()
            .expect("generation state")
            .insert(sibling.to_string(), Some(completion_ticket(12)));
        let scoped = SessionKey::from_thread_and_agent(principal_id, "thread-2", "advisor");
        let _ = backstop_keepalive(Arc::clone(&state))
            .cancel_session(
                &scoped,
                &Principal {
                    id: "principal-test".to_owned(),
                    kind: cc_lb_domain::PrincipalKind::InternalKey,
                },
                CancelReason::NewRequest,
                Some(completion_ticket(4)),
                Duration::ZERO,
                true,
                None,
            )
            .await;
        assert_eq!(
            *state.cancellations.lock().expect("cancellation state"),
            vec![
                (primary.to_string(), None),
                (scoped.to_string(), Some(completion_ticket(4))),
            ]
        );

        let restarted = backstop_keepalive(Arc::clone(&state));
        let _ = restarted
            .cancel_session(
                &primary,
                &Principal {
                    id: "principal-test".to_owned(),
                    kind: cc_lb_domain::PrincipalKind::InternalKey,
                },
                CancelReason::NewRequest,
                Some(completion_ticket(5)),
                Duration::ZERO,
                true,
                None,
            )
            .await;
        assert_eq!(
            *state.cancellations.lock().expect("cancellation state"),
            vec![
                (primary.to_string(), None),
                (scoped.to_string(), Some(completion_ticket(4))),
                (primary.to_string(), Some(completion_ticket(5))),
                (sibling.to_string(), Some(completion_ticket(12))),
            ]
        );
    }
    #[tokio::test]
    async fn claude_main_agent_id_does_not_cancel_subagent_sibling() {
        let state = Arc::new(BackstopState::default());
        let principal_id = cache_keepalive_principal_uuid("principal-test");
        let primary = SessionKey::from_thread_id(principal_id, "thread-claude-main");
        let sibling =
            SessionKey::from_thread_and_agent(principal_id, "thread-claude-main", "subagent");
        state
            .generations
            .lock()
            .expect("generation state")
            .insert(sibling.to_string(), Some(completion_ticket(7)));
        let keepalive = backstop_keepalive(Arc::clone(&state));
        let mut metadata = RequestCacheMetadata::default();
        metadata.claude_agent_id = Some("main".to_owned());
        let _ = keepalive
            .cancel_session(
                &primary,
                &Principal {
                    id: "principal-test".to_owned(),
                    kind: cc_lb_domain::PrincipalKind::InternalKey,
                },
                CancelReason::NewRequest,
                Some(completion_ticket(4)),
                Duration::ZERO,
                allows_sibling_backstop(&metadata),
                None,
            )
            .await;
        assert_eq!(
            *state.cancellations.lock().expect("cancellation state"),
            vec![(primary.to_string(), Some(completion_ticket(4)))]
        );
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
                        None,
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
                None,
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
                    None,
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
                    OrderedCompletion::Cancel(expected_key.clone(), Some(completion_ticket(41))),
                ]
            } else {
                vec![
                    OrderedCompletion::Observe(expected_key.clone()),
                    OrderedCompletion::Enqueue(expected_key, Some(completion_ticket(41))),
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
            keepalive.on_response_completed(&serde_json::json!({
                "type": "message",
                "content": [{"type": "tool_use", "id": "toolu_pending", "name": "Bash", "input": {}}],
                "stop_reason": "tool_use"
            }), &principal, "accounting-key".to_owned(), &legitimate, Uuid::nil(), body.clone(), body.clone(), &headers, Duration::ZERO, 12_000, None);
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
                None,
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
                    OrderedCompletion::Enqueue(expected_key, Some(completion_ticket(41))),
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

        let forwarded = keepalive_forward_headers(&headers, None);

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
    fn keepalive_scope_headers_are_api_key_responses_only() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "openai-organization",
            HeaderValue::from_static("org-client"),
        );
        headers.insert("openai-project", HeaderValue::from_static("proj-client"));
        headers.insert("openai-processing-region", HeaderValue::from_static("us"));
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer client-secret"),
        );
        headers.insert("x-api-key", HeaderValue::from_static("client-key"));
        headers.insert(
            "x-goog-api-key",
            HeaderValue::from_static("client-google-key"),
        );
        headers.insert("api-key", HeaderValue::from_static("client-api-key"));
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));

        for kind in [
            None,
            Some(UpstreamKind::OpenaiApiKey),
            Some(UpstreamKind::OpenaiOauth),
            Some(UpstreamKind::AnthropicApiKey),
            Some(UpstreamKind::AnthropicOauth),
            Some(UpstreamKind::GeminiApiKey),
            Some(UpstreamKind::GeminiOauth),
        ] {
            let forwarded = keepalive_forward_headers(&headers, kind);
            for name in [
                "openai-organization",
                "openai-project",
                "openai-processing-region",
            ] {
                assert_eq!(
                    forwarded.get(name),
                    (kind == Some(UpstreamKind::OpenaiApiKey)).then(|| headers.get(name).unwrap()),
                    "scope selector {name} for {kind:?}",
                );
            }
            for name in ["authorization", "x-api-key", "x-goog-api-key", "api-key"] {
                assert!(
                    !forwarded.contains_key(name),
                    "credential {name} for {kind:?}"
                );
            }
            assert_eq!(forwarded["anthropic-version"], headers["anthropic-version"]);
        }
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
    fn streaming_openai_response_completed_becomes_response_body_for_keepalive() {
        let mut response = StreamingKeepaliveResponse::default();
        response.observe(&crate::usage_parser::parse_sse_event(
            b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-5.6\",\"usage\":{\"input_tokens\":1200,\"output_tokens\":16,\"input_tokens_details\":{\"cached_tokens\":1100}}}}\n\n",
        ));
        let value = response.into_value().expect("response.completed captured");
        assert_eq!(value["model"], "gpt-5.6");
        assert_eq!(
            value["usage"]["input_tokens_details"]["cached_tokens"],
            1100
        );
    }

    #[test]
    fn streaming_openai_data_only_terminals_become_response_bodies() {
        for event in [
            br#"data: {"type":"response.completed","response":{"model":"gpt-5.6","usage":{"input_tokens":10}}}"# as &[u8],
            br#"data: {"type":"response.incomplete","response":{"model":"gpt-5.6","usage":{"input_tokens":11}}}"#,
        ] {
            let mut response = StreamingKeepaliveResponse::default();
            response.observe(&crate::usage_parser::parse_sse_event(event));
            assert_eq!(response.into_value().unwrap()["model"], "gpt-5.6");
        }
    }
    #[test]
    fn streaming_named_incomplete_without_type_becomes_response_body() {
        let mut response = StreamingKeepaliveResponse::default();
        response.observe(&crate::usage_parser::parse_sse_event(
            b"event: response.incomplete\ndata: {\"response\":{\"model\":\"gpt-5.6\",\"usage\":{\"input_tokens\":12}}}\n\n",
        ));
        assert_eq!(response.into_value().unwrap()["model"], "gpt-5.6");
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
            |_| {},
        );
        response.observe_emitted(b"", 1024, |_| {});
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
            |_| {},
        );
        response.observe_emitted(
            b"stion\"}}\n\n\
              event: message_delta\ndata: {\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n",
            1024,
            |_| {},
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
        response.observe_emitted(b"data: ", 4, |_| {});
        response.observe_emitted(b"xxxxx", 4, |_| {});

        assert!(response.sse_observation_disabled);
        assert!(response.pending_sse_bytes.is_empty());
    }

    #[test]
    fn follow_up_session_keys_preserve_thread_and_agent_scopes() {
        let principal = Principal {
            id: "principal-test".to_owned(),
            kind: cc_lb_domain::PrincipalKind::InternalKey,
        };
        let principal_id = cache_keepalive_principal_uuid(&principal.id);
        for (request_kind, agent_id, suffix) in [
            ("main", None, ""),
            ("main", Some("agent-a"), "/agent:agent-a"),
            ("subagent", Some("main"), ""),
            ("subagent", None, "/agent:subagent"),
        ] {
            let mut metadata = RequestCacheMetadata::default();
            metadata.thread_id = Some("thread-1".to_owned());
            metadata.request_kind = Some(request_kind.to_owned());
            metadata.claude_agent_id = agent_id.map(str::to_owned);
            let expected = Some(format!("{principal_id}/thread:thread-1{suffix}"));
            assert_eq!(follow_up_session_key_hash(&principal, &metadata), expected);
            assert_eq!(
                follow_up_session_key_hash(&principal, &metadata),
                cache_keepalive_session_key(&principal, &metadata).map(|key| key.to_string()),
            );
        }
    }

    #[test]
    fn follow_up_session_keys_exclude_auxiliary_ineligible_and_prefix_only_requests() {
        let principal = Principal {
            id: "principal-test".to_owned(),
            kind: cc_lb_domain::PrincipalKind::InternalKey,
        };
        let mut metadata = RequestCacheMetadata::default();
        metadata.thread_id = Some("thread-1".to_owned());
        metadata.cache_prefix_hash = Some("prefix-1".to_owned());
        metadata.explicit_auxiliary = true;
        assert_eq!(follow_up_session_key_hash(&principal, &metadata), None);
        metadata.explicit_auxiliary = false;
        metadata.request_kind = Some("compaction".to_owned());
        assert_eq!(follow_up_session_key_hash(&principal, &metadata), None);
        metadata.request_kind = Some("main".to_owned());
        metadata.thread_id = None;
        assert!(cache_keepalive_session_key(&principal, &metadata).is_some());
        assert_eq!(follow_up_session_key_hash(&principal, &metadata), None);
        metadata.openai_replay_identity = Some("prompt-cache-key:stable".to_owned());
        assert_eq!(follow_up_session_key_hash(&principal, &metadata), None);
        metadata.cache_keepalive_surface = Some("responses".to_owned());
        let key = follow_up_session_key_hash(&principal, &metadata)
            .expect("OpenAI replay identity is stable across turns");
        assert!(key.contains("openai-replay:prompt-cache-key:stable"));
        assert_eq!(
            Some(key),
            cache_keepalive_session_key(&principal, &metadata).map(|key| key.to_string()),
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
    fn openai_responses_without_headers_use_replay_identity() {
        let principal = Principal {
            id: "principal-test".to_owned(),
            kind: cc_lb_domain::PrincipalKind::InternalKey,
        };
        let mut metadata = RequestCacheMetadata::default();
        metadata.cache_keepalive_surface = Some("responses".to_owned());
        metadata.openai_replay_identity = Some("prompt-cache-key:stable".to_owned());
        let key = cache_keepalive_session_key(&principal, &metadata)
            .expect("OpenAI replay identity should create a session key")
            .to_string();
        assert!(key.contains("openai-replay:prompt-cache-key:stable"));
        metadata.openai_replay_identity = Some("conversation:conv-1".to_owned());
        assert_ne!(
            key,
            cache_keepalive_session_key(&principal, &metadata)
                .unwrap()
                .to_string()
        );
    }

    #[test]
    fn openai_replay_identity_does_not_override_stable_thread_identity() {
        let principal = Principal {
            id: "principal-test".to_owned(),
            kind: cc_lb_domain::PrincipalKind::InternalKey,
        };
        let mut first = RequestCacheMetadata::default();
        first.cache_keepalive_surface = Some("responses".to_owned());
        first.thread_id = Some("session-1".to_owned());
        first.openai_replay_identity = Some("input-sha256:first".to_owned());
        let mut next = first.clone();
        next.openai_replay_identity = Some("input-sha256:next".to_owned());
        assert_eq!(
            cache_keepalive_session_key(&principal, &first)
                .unwrap()
                .to_string(),
            cache_keepalive_session_key(&principal, &next)
                .unwrap()
                .to_string()
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

    #[tokio::test]
    async fn changed_identity_gets_fresh_local_order_and_yields_to_newer_completion() {
        for newer_completion in [false, true] {
            let record = StoragePrincipalRecord {
                id: Uuid::from_u128(99),
                name: "identity-order".to_owned(),
                kind: StoragePrincipalKind::Machine,
                allowed_models: vec!["*".to_owned()],
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
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
            let view = DynamicViewBuilder::new(0)
                .signer_factory(Arc::new(TestSignerFactory))
                .principal_view(Arc::new(PrincipalView::from_db(&[record], HashMap::new())))
                .build();
            let (events, mut received) = tokio::sync::mpsc::unbounded_channel();
            let keepalive = LifecycleKeepalive::new(
                Some(Arc::new(OrderedCompletionEnqueuer { events })),
                Arc::new(DynamicViewHolder::new(view)),
            );
            let principal = Principal {
                id: "identity-order".to_owned(),
                kind: cc_lb_domain::PrincipalKind::InternalKey,
            };
            let k1 = SessionKey::from_cache_prefix_hash(
                cache_keepalive_principal_uuid(&principal.id),
                "official",
            );
            let k2 = SessionKey::from_cache_prefix_hash(
                cache_keepalive_principal_uuid(&principal.id),
                "custom",
            );
            let request_json = serde_json::json!({
                "model":"claude-test","max_tokens":32,
                "system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral"}}],
                "messages":[{"role":"user","content":"hello"}]
            });
            let body = Bytes::from(serde_json::to_vec(&request_json).unwrap());
            let mut metadata = RequestCacheMetadata::default();
            metadata.cache_prefix_hash = Some("custom".to_owned());
            metadata.request_json = Some(request_json);
            metadata.cache_breakpoints = vec![cc_lb_request_log::RequestCacheBreakpoint {
                block_index: 0,
                source: cc_lb_request_log::RequestCacheBreakpointSource::System,
                path: "system[0]".to_owned(),
                message_index: None,
                ttl: Some("5m".to_owned()),
                prefix_hash: "custom".to_owned(),
                prefix_token_count: 12_000,
                lookback_prefixes: Vec::new(),
                token_estimate_source: None,
            }];
            // K1's start was ordinal 1; K2 already completed ordinal 2.
            keepalive.next_completion_seq.store(2, Ordering::Relaxed);
            keepalive.record_issued_seq(&k1, 1);
            keepalive.record_issued_seq(&k2, 2);
            let mut shard = keepalive.session_shard(&k2).lock().await;
            assert!(keepalive.claim_completion_seq(&mut shard, &k2, 2));
            keepalive.on_response_completed(
                &serde_json::json!({
                    "type":"message","content":[{"type":"tool_use","id":"t","name":"Bash","input":{}}],
                    "stop_reason":"tool_use"
                }),
                &principal, "key".to_owned(), &metadata, Uuid::nil(),
                body.clone(), body, &HeaderMap::new(), Duration::ZERO, 12_000,
                Some(RequestStartOrder {
                    session_key: k1.clone(), sequence: 1,
                    expected_generation: Some(completion_ticket(7)), sibling_generation: None,
                }),
            );
            assert_eq!(
                keepalive.latest_issued_seq(&k2),
                3,
                "K1's old ordinal cannot be reused for K2's fresh completion"
            );
            if newer_completion {
                keepalive.record_issued_seq(&k2, 4);
                assert!(keepalive.claim_completion_seq(&mut shard, &k2, 4));
            }
            drop(shard);
            let cancel = tokio::time::timeout(Duration::from_secs(5), received.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                cancel,
                OrderedCompletion::Cancel(k1.to_string(), Some(completion_ticket(7)))
            );
            let observe = tokio::time::timeout(Duration::from_secs(5), received.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(observe, OrderedCompletion::Observe(k2.to_string()));
            // Acquiring the shard after Observe waits until persistence has
            // either enqueued or rejected the older K2 completion.
            let _settled = keepalive.session_shard(&k2).lock().await;
            if newer_completion {
                assert!(received.try_recv().is_err(), "newer K2 owner must survive");
            } else {
                assert_eq!(
                    received.try_recv().unwrap(),
                    OrderedCompletion::Enqueue(k2.to_string(), Some(completion_ticket(41)))
                );
            }
        }
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
        assert_superseded_request_fences(false, false).await;
    }

    #[tokio::test]
    async fn dropped_newer_request_cannot_remove_older_request_fence() {
        assert_superseded_request_fences(true, false).await;
    }

    #[tokio::test]
    async fn openai_headerless_turns_cancel_previous_generation() {
        assert_superseded_request_fences(false, true).await;
    }

    async fn assert_superseded_request_fences(drop_newer: bool, openai: bool) {
        struct RequestFenceEnqueuer {
            ordered: OrderedCompletionEnqueuer,
            active_generation: std::sync::atomic::AtomicBool,
        }

        #[async_trait]
        impl CacheKeepaliveEnqueuer for RequestFenceEnqueuer {
            async fn observe_session_generation(
                &self,
                key: &str,
            ) -> Result<Option<CacheKeepaliveCompletionTicket>, CacheKeepaliveEnqueueError>
            {
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
                assert_eq!(request.expected_generation, Some(completion_ticket(41)));
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
        let classifier = ClassifierConfig {
            openai_responses_enabled: openai,
            ..ClassifierConfig::default()
        };
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
                    classifier,
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
        if openai {
            metadata.cache_keepalive_surface = Some("responses".to_owned());
            metadata.openai_replay_identity = Some("input-sha256:first".to_owned());
        } else {
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
        }
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
        let mut next_metadata = metadata.clone();
        if openai {
            next_metadata.openai_replay_identity = Some("input-sha256:next".to_owned());
        }
        let mut newer = Some(Box::pin(keepalive.on_request_started(
            &authenticated,
            &principal,
            &next_metadata,
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
        let first_order = first.await;
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
            OrderedCompletion::Cancel(key.to_string(), Some(completion_ticket(41)))
        );
        if let Some(newer) = newer {
            let _ = newer.await;
        }
        keepalive.on_response_completed(
            &serde_json::json!({"stop_reason": "end_turn", "content": []}),
            &principal,
            "request-owner-key".to_owned(),
            &metadata,
            Uuid::nil(),
            Bytes::new(),
            Bytes::new(),
            &HeaderMap::new(),
            Duration::ZERO,
            12_000,
            first_order,
        );
        assert_eq!(
            keepalive.latest_issued_seq(&key),
            first_sequence + 1,
            "A's response must not advance local issuance past the newer B start"
        );
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
            "system": [{"type": "text", "text": "cached", "cache_control": {"type": "ephemeral", "ttl": "1h"}}],
            "messages": [{"role": "user", "content": [{"type": "text", "text": "hello", "cache_control": {"type": "ephemeral", "ttl": "5m"}}]}]
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
            ttl: Some("1h".to_owned()),
            prefix_hash: "stale-prefix".to_owned(),
            prefix_token_count: 12_000,
            lookback_prefixes: Vec::new(),
            token_estimate_source: None,
        }];
        let mut message_breakpoint = metadata.cache_breakpoints[0].clone();
        message_breakpoint.source = cc_lb_request_log::RequestCacheBreakpointSource::Message;
        message_breakpoint.path = "messages[0].content[0]".to_owned();
        message_breakpoint.message_index = Some(0);
        message_breakpoint.ttl = Some("5m".to_owned());
        metadata.cache_breakpoints.push(message_breakpoint);
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
                None,
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
                None,
            )
            .await;
        assert_eq!(
            journal.not_tracked.lock().expect("not-tracked lock").len(),
            1
        );
        assert!(journal.cancelled.lock().expect("cancelled lock").is_empty());
        assert_eq!(journal.enqueued.lock().expect("enqueued lock").len(), 1);
        {
            let enqueued = journal.enqueued.lock().expect("enqueued lock");
            assert_eq!(enqueued[0].snapshot.ttl, CacheTtl::Ttl5m);
            assert_eq!(enqueued[0].params.delay, Duration::from_secs(269));
            assert_eq!(
                enqueued[0].snapshot.body, request_body,
                "mixed TTL scheduling must not rewrite client bytes"
            );
        }

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
                None,
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
                None,
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
                None,
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
