use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_clock::{Clock, ClockHandle, unix_secs};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_control::{AuditPayload, MetadataHookHandle, MetadataHookRequest};
use cc_lb_oauth_protocol::{
    ExistingTokenParts, TokenEndpointResponse, is_terminal_refresh_failure,
    parse_token_endpoint_response, refresh_requires_reconnect, refresh_token_form_body,
    refresh_token_json_body, refreshed_token_parts,
};
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::worker::{
    AdaptiveJob, AdaptiveTaskKeyState, SchedulerBackend, SchedulerPushTask,
};
use cc_lb_signer_anthropic_oauth::{LazyRefreshError, LazyRefreshHandle};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    AuditActorFields, AuditEntry, AuditStore, MetaStore, OAuthRefreshFence, StorageError,
    StorageResult, UpstreamRecord, UpstreamStatusUpdate,
};
use http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderValue, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::dynamic_view_builder::Stores;

const LAZY_REFRESH_CONTENTION_WAIT_SECS: u64 = 60;
const LAZY_REFRESH_POLL_INTERVAL_SECS: u64 = 1;
pub(crate) const OAUTH_REFRESH_LEASE_TTL_SECS: u64 = 90;
const OAUTH_REFRESH_LEASE_RENEW_INTERVAL_SECS: u64 = 25;
const OAUTH_REFRESH_LEASE_SAFETY_MARGIN_SECS: u64 = 1;
const OAUTH_REFRESH_LEASE_CAS_ATTEMPTS: usize = 4;

pub(crate) fn oauth_refresh_lease_meta_key(upstream_id: Uuid) -> String {
    format!("oauth_refresh_lease:v1:{upstream_id}")
}

pub(crate) async fn read_oauth_refresh_lease_expires_at(
    meta: &dyn MetaStore,
    upstream_id: Uuid,
) -> StorageResult<Option<u64>> {
    let key = oauth_refresh_lease_meta_key(upstream_id);
    let raw = meta.get_meta_value(&key).await?;
    Ok(raw
        .as_deref()
        .and_then(parse_oauth_refresh_lease)
        .and_then(|parsed| match parsed {
            ParsedOAuthRefreshLease::Held {
                expires_at_unix_secs,
                ..
            } => Some(expires_at_unix_secs),
            ParsedOAuthRefreshLease::Idle { .. } => None,
        }))
}

pub(crate) fn oauth_refresh_lease_retry_delay(
    now_unix_secs: u64,
    expires_at_unix_secs: Option<u64>,
) -> Duration {
    const DEFAULT_LEASE_RETRY_DELAY_SECS: u64 = 30;
    const MAX_LEASE_RETRY_DELAY_SECS: u64 = 600;
    let secs = match expires_at_unix_secs {
        Some(expires_at) => expires_at
            .saturating_sub(now_unix_secs)
            .saturating_add(1)
            .min(MAX_LEASE_RETRY_DELAY_SECS),
        None => DEFAULT_LEASE_RETRY_DELAY_SECS,
    };
    Duration::from_secs(secs)
}

enum ParsedOAuthRefreshLease {
    Idle {
        generation: u64,
    },
    Held {
        generation: u64,
        holder: String,
        expires_at_unix_secs: u64,
    },
}

fn parse_oauth_refresh_lease(raw: &str) -> Option<ParsedOAuthRefreshLease> {
    let mut parts = raw.split(':');
    match parts.next()? {
        "idle" => {
            let generation = parts.next()?.parse().ok()?;
            if parts.next().is_some() {
                return None;
            }
            Some(ParsedOAuthRefreshLease::Idle { generation })
        }
        "held" => {
            let generation = parts.next()?.parse().ok()?;
            let holder = parts.next()?.to_owned();
            if holder.is_empty() {
                return None;
            }
            let expires_at_unix_secs = parts.next()?.parse().ok()?;
            if parts.next().is_some() {
                return None;
            }
            Some(ParsedOAuthRefreshLease::Held {
                generation,
                holder,
                expires_at_unix_secs,
            })
        }
        _ => None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OAuthRefreshLease {
    pub(crate) upstream_id: Uuid,
    pub(crate) token: String,
}

fn oauth_refresh_lease_token(generation: u64, holder: &str, now_unix_secs: u64) -> String {
    let expires_at = now_unix_secs.saturating_add(OAUTH_REFRESH_LEASE_TTL_SECS);
    format!("held:{generation}:{holder}:{expires_at}")
}

fn normalized_lease_holder(holder: &str) -> String {
    if holder.is_empty() || holder.contains(':') {
        Uuid::new_v4().to_string()
    } else {
        holder.to_owned()
    }
}

async fn reconcile_exact_oauth_refresh_lease(
    meta: &dyn MetaStore,
    upstream_id: Uuid,
    attempted_token: &str,
    final_generation: u64,
) -> StorageResult<()> {
    let key = oauth_refresh_lease_meta_key(upstream_id);
    let idle = format!("idle:{final_generation}");
    // Issue the exact-token conditional write directly (no unlocked pre-read)
    // so cleanup serializes on the backend's per-key lock behind any in-flight
    // COMMIT from the original claim transaction.
    let _ = meta
        .compare_and_put_meta_value(&key, Some(attempted_token), &idle)
        .await?;
    Ok(())
}

/// Acquire a short-lived shared CAS lease in `MetaStore` for `upstream_id` at
/// `generation`. Shared across scheduled refreshes (expiry-key and watchdog
/// bootstrap jobs) and in-process lazy recovery fallbacks.
#[cfg(test)]
pub(crate) async fn try_acquire_oauth_refresh_lease(
    meta: &dyn MetaStore,
    upstream_id: Uuid,
    generation: u64,
    now_unix_secs: u64,
) -> StorageResult<Option<OAuthRefreshLease>> {
    let holder = Uuid::new_v4().to_string();
    try_acquire_oauth_refresh_lease_with_holder(
        meta,
        upstream_id,
        generation,
        &holder,
        now_unix_secs,
    )
    .await
}

pub(crate) async fn try_acquire_oauth_refresh_lease_with_holder(
    meta: &dyn MetaStore,
    upstream_id: Uuid,
    generation: u64,
    holder: &str,
    now_unix_secs: u64,
) -> StorageResult<Option<OAuthRefreshLease>> {
    let key = oauth_refresh_lease_meta_key(upstream_id);
    let holder = normalized_lease_holder(holder);
    let token = oauth_refresh_lease_token(generation, &holder, now_unix_secs);
    for _ in 0..OAUTH_REFRESH_LEASE_CAS_ATTEMPTS {
        let existing = meta.get_meta_value(&key).await?;
        match existing.as_deref().and_then(parse_oauth_refresh_lease) {
            Some(ParsedOAuthRefreshLease::Idle {
                generation: idle_gen,
            }) if idle_gen > generation => {
                return Ok(None);
            }
            Some(ParsedOAuthRefreshLease::Held {
                generation: held_gen,
                expires_at_unix_secs,
                ..
            }) if held_gen > generation || now_unix_secs < expires_at_unix_secs => {
                return Ok(None);
            }
            _ => {}
        }
        match meta
            .compare_and_put_meta_value(&key, existing.as_deref(), &token)
            .await
        {
            Ok(true) => {
                return Ok(Some(OAuthRefreshLease { upstream_id, token }));
            }
            Ok(false) => continue,
            Err(error) => {
                if let Err(reconcile_error) =
                    reconcile_exact_oauth_refresh_lease(meta, upstream_id, &token, generation).await
                {
                    tracing::warn!(
                        error = %reconcile_error,
                        upstream_id = %upstream_id,
                        generation,
                        "failed to reconcile ambiguous oauth refresh lease commit",
                    );
                }
                return Err(error);
            }
        }
    }
    Ok(None)
}

pub(crate) async fn renew_oauth_refresh_lease(
    meta: &dyn MetaStore,
    upstream_id: Uuid,
    current_token: &str,
    now_unix_secs: u64,
) -> StorageResult<Option<String>> {
    let Some(ParsedOAuthRefreshLease::Held {
        generation,
        holder,
        expires_at_unix_secs: current_expires_at,
    }) = parse_oauth_refresh_lease(current_token)
    else {
        return Ok(None);
    };
    let key = oauth_refresh_lease_meta_key(upstream_id);
    let expires_at = now_unix_secs.saturating_add(OAUTH_REFRESH_LEASE_TTL_SECS);
    let next_token = format!("held:{generation}:{holder}:{expires_at}");
    match meta
        .compare_and_put_meta_value(&key, Some(current_token), &next_token)
        .await
    {
        Ok(true) => return Ok(Some(next_token)),
        Ok(false) => {}
        Err(error) => {
            if next_token != current_token
                && matches!(
                    meta.compare_and_put_meta_value(&key, Some(&next_token), &next_token)
                        .await,
                    Ok(true)
                )
            {
                return Ok(Some(next_token));
            }
            return Err(error);
        }
    }

    for _ in 0..OAUTH_REFRESH_LEASE_CAS_ATTEMPTS {
        let Some(existing) = meta.get_meta_value(&key).await? else {
            return Ok(None);
        };
        match parse_oauth_refresh_lease(&existing) {
            Some(ParsedOAuthRefreshLease::Held {
                generation: stored_gen,
                holder: stored_holder,
                expires_at_unix_secs: stored_expires_at,
            }) if stored_gen == generation
                && stored_holder == holder
                && stored_expires_at >= current_expires_at
                && now_unix_secs < stored_expires_at =>
            {
                let reconciled_expires_at = expires_at.max(stored_expires_at);
                let reconciled_token =
                    format!("held:{generation}:{holder}:{reconciled_expires_at}");
                match meta
                    .compare_and_put_meta_value(&key, Some(&existing), &reconciled_token)
                    .await
                {
                    Ok(true) => return Ok(Some(reconciled_token)),
                    Ok(false) => continue,
                    Err(error) => {
                        if reconciled_token != existing
                            && matches!(
                                meta.compare_and_put_meta_value(
                                    &key,
                                    Some(&reconciled_token),
                                    &reconciled_token,
                                )
                                .await,
                                Ok(true)
                            )
                        {
                            return Ok(Some(reconciled_token));
                        }
                        return Err(error);
                    }
                }
            }
            _ => return Ok(None),
        }
    }
    Ok(None)
}

fn oauth_refresh_lease_fence_prefix(token: &str) -> String {
    match parse_oauth_refresh_lease(token) {
        Some(ParsedOAuthRefreshLease::Held {
            generation, holder, ..
        }) => format!("held:{generation}:{holder}:"),
        _ => token.to_owned(),
    }
}

pub(crate) async fn release_oauth_refresh_lease(
    meta: &dyn MetaStore,
    upstream_id: Uuid,
    lease_token: &str,
    final_generation: u64,
) -> StorageResult<()> {
    let prefix = oauth_refresh_lease_fence_prefix(lease_token);
    if prefix.is_empty() {
        return Ok(());
    }
    let key = oauth_refresh_lease_meta_key(upstream_id);
    let idle = format!("idle:{final_generation}");
    // Try releasing the exact token directly before falling back to prefix
    // discovery so un-renewed releases serialize on the backend's per-key
    // lock behind any in-flight COMMIT.
    if meta
        .compare_and_put_meta_value(&key, Some(lease_token), &idle)
        .await?
    {
        return Ok(());
    }
    for _ in 0..OAUTH_REFRESH_LEASE_CAS_ATTEMPTS {
        let Some(existing) = meta.get_meta_value(&key).await? else {
            return Ok(());
        };
        if !existing.starts_with(&prefix) {
            return Ok(());
        }
        if meta
            .compare_and_put_meta_value(&key, Some(&existing), &idle)
            .await?
        {
            return Ok(());
        }
    }
    Ok(())
}

#[async_trait]
trait OAuthLeaseController: Send + Sync + 'static {
    async fn renew(
        &self,
        upstream_id: Uuid,
        current_token: &str,
        now_unix_secs: u64,
    ) -> StorageResult<Option<String>>;

    async fn release(
        &self,
        upstream_id: Uuid,
        lease_token: &str,
        final_generation: u64,
    ) -> StorageResult<()>;

    fn meta_key(&self, upstream_id: Uuid) -> Option<String>;
}

struct MetaStoreLeaseController(Arc<dyn MetaStore>);

#[async_trait]
impl OAuthLeaseController for MetaStoreLeaseController {
    async fn renew(
        &self,
        upstream_id: Uuid,
        current_token: &str,
        now_unix_secs: u64,
    ) -> StorageResult<Option<String>> {
        renew_oauth_refresh_lease(self.0.as_ref(), upstream_id, current_token, now_unix_secs).await
    }

    async fn release(
        &self,
        upstream_id: Uuid,
        lease_token: &str,
        final_generation: u64,
    ) -> StorageResult<()> {
        release_oauth_refresh_lease(self.0.as_ref(), upstream_id, lease_token, final_generation)
            .await
    }

    fn meta_key(&self, upstream_id: Uuid) -> Option<String> {
        Some(oauth_refresh_lease_meta_key(upstream_id))
    }
}

struct ClaimGuardLeaseController(Arc<dyn LazyRefreshClaimGuard>);

#[async_trait]
impl OAuthLeaseController for ClaimGuardLeaseController {
    async fn renew(
        &self,
        upstream_id: Uuid,
        current_token: &str,
        now_unix_secs: u64,
    ) -> StorageResult<Option<String>> {
        let lease = LazyRefreshFallbackLease::from_token(current_token.to_owned());
        Ok(self
            .0
            .renew_fallback_claim(upstream_id, &lease, now_unix_secs)
            .await?
            .map(|next| next.0))
    }

    async fn release(
        &self,
        upstream_id: Uuid,
        lease_token: &str,
        final_generation: u64,
    ) -> StorageResult<()> {
        let lease = LazyRefreshFallbackLease::from_token(lease_token.to_owned());
        self.0
            .finish_fallback_claim(upstream_id, &lease, final_generation)
            .await
    }

    fn meta_key(&self, upstream_id: Uuid) -> Option<String> {
        self.0.fallback_lease_meta_key(upstream_id)
    }
}

enum LeaseHeartbeatState {
    Active(String),
    Lost { cleanup_token: String },
    Released,
}

/// Keeps an acquired OAuth refresh credential lease renewed via periodic
/// heartbeat while an exchange is in flight, cancels the linked request token
/// on ownership loss, and emits an atomic [`OAuthRefreshFence`] for database
/// persistence.
pub(crate) struct ActiveOAuthRefreshLease {
    upstream_id: Uuid,
    expected_generation: u64,
    controller: Arc<dyn OAuthLeaseController>,
    state: Arc<tokio::sync::Mutex<LeaseHeartbeatState>>,
    lease_cancel: CancellationToken,
    stop_heartbeat: CancellationToken,
    heartbeat: Option<tokio::task::JoinHandle<()>>,
    released: bool,
    provider_exchange_started: bool,
}

async fn take_release_token(state: &tokio::sync::Mutex<LeaseHeartbeatState>) -> Option<String> {
    let mut guard = state.lock().await;
    match std::mem::replace(&mut *guard, LeaseHeartbeatState::Released) {
        LeaseHeartbeatState::Active(token)
        | LeaseHeartbeatState::Lost {
            cleanup_token: token,
        } => Some(token),
        LeaseHeartbeatState::Released => {
            *guard = LeaseHeartbeatState::Released;
            None
        }
    }
}

impl Drop for ActiveOAuthRefreshLease {
    fn drop(&mut self) {
        self.stop_heartbeat.cancel();
        self.lease_cancel.cancel();
        if self.released || self.provider_exchange_started {
            return;
        }
        if self.heartbeat.is_some() && tokio::runtime::Handle::try_current().is_ok() {
            drop(self.spawn_release(self.expected_generation));
        }
    }
}

async fn mark_lease_lost(
    state: &tokio::sync::Mutex<LeaseHeartbeatState>,
    lease_cancel: &CancellationToken,
    pending_token: Option<String>,
) {
    let mut guard = state.lock().await;
    match std::mem::replace(&mut *guard, LeaseHeartbeatState::Released) {
        LeaseHeartbeatState::Active(current_token) => {
            *guard = LeaseHeartbeatState::Lost {
                cleanup_token: pending_token.unwrap_or(current_token),
            };
            lease_cancel.cancel();
        }
        LeaseHeartbeatState::Lost { cleanup_token } => {
            *guard = LeaseHeartbeatState::Lost {
                cleanup_token: pending_token.unwrap_or(cleanup_token),
            };
            lease_cancel.cancel();
        }
        LeaseHeartbeatState::Released => {
            *guard = LeaseHeartbeatState::Released;
        }
    }
}

impl ActiveOAuthRefreshLease {
    pub(crate) fn start(
        meta: Arc<dyn MetaStore>,
        clock: ClockHandle,
        parent_cancel: CancellationToken,
        lease: OAuthRefreshLease,
        expected_generation: u64,
    ) -> Self {
        let safety_margin = Duration::from_secs(OAUTH_REFRESH_LEASE_SAFETY_MARGIN_SECS);
        let effective_ttl =
            Duration::from_secs(OAUTH_REFRESH_LEASE_TTL_SECS).saturating_sub(safety_margin);
        Self::start_with_controller_and_timings(
            Arc::new(MetaStoreLeaseController(meta)),
            clock,
            parent_cancel,
            lease.upstream_id,
            lease.token,
            expected_generation,
            Duration::from_secs(OAUTH_REFRESH_LEASE_RENEW_INTERVAL_SECS),
            effective_ttl,
            safety_margin,
        )
    }

    fn start_for_claim_guard(
        claim_guard: Arc<dyn LazyRefreshClaimGuard>,
        clock: ClockHandle,
        parent_cancel: CancellationToken,
        upstream_id: Uuid,
        lease: LazyRefreshFallbackLease,
        expected_generation: u64,
    ) -> Self {
        let safety_margin = Duration::from_secs(OAUTH_REFRESH_LEASE_SAFETY_MARGIN_SECS);
        let effective_ttl =
            Duration::from_secs(OAUTH_REFRESH_LEASE_TTL_SECS).saturating_sub(safety_margin);
        Self::start_with_controller_and_timings(
            Arc::new(ClaimGuardLeaseController(claim_guard)),
            clock,
            parent_cancel,
            upstream_id,
            lease.0,
            expected_generation,
            Duration::from_secs(OAUTH_REFRESH_LEASE_RENEW_INTERVAL_SECS),
            effective_ttl,
            safety_margin,
        )
    }

    #[cfg(test)]
    pub(crate) fn start_with_interval(
        meta: Arc<dyn MetaStore>,
        clock: ClockHandle,
        parent_cancel: CancellationToken,
        lease: OAuthRefreshLease,
        expected_generation: u64,
        renew_interval: Duration,
    ) -> Self {
        let safety_margin = Duration::from_secs(OAUTH_REFRESH_LEASE_SAFETY_MARGIN_SECS);
        let effective_ttl =
            Duration::from_secs(OAUTH_REFRESH_LEASE_TTL_SECS).saturating_sub(safety_margin);
        Self::start_with_controller_and_timings(
            Arc::new(MetaStoreLeaseController(meta)),
            clock,
            parent_cancel,
            lease.upstream_id,
            lease.token,
            expected_generation,
            renew_interval,
            effective_ttl,
            safety_margin,
        )
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    fn start_with_controller_for_tests(
        controller: Arc<dyn OAuthLeaseController>,
        clock: ClockHandle,
        parent_cancel: CancellationToken,
        upstream_id: Uuid,
        initial_token: String,
        expected_generation: u64,
        renew_interval: Duration,
        effective_ttl: Duration,
        safety_margin: Duration,
    ) -> Self {
        Self::start_with_controller_and_timings(
            controller,
            clock,
            parent_cancel,
            upstream_id,
            initial_token,
            expected_generation,
            renew_interval,
            effective_ttl,
            safety_margin,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_with_controller_and_timings(
        controller: Arc<dyn OAuthLeaseController>,
        clock: ClockHandle,
        parent_cancel: CancellationToken,
        upstream_id: Uuid,
        initial_token: String,
        expected_generation: u64,
        renew_interval: Duration,
        effective_ttl: Duration,
        safety_margin: Duration,
    ) -> Self {
        let start_instant = tokio::time::Instant::now();
        let start_unix = unix_secs(clock.now());
        let (mut confirmed_expires_at, initial_deadline) =
            match parse_oauth_refresh_lease(&initial_token) {
                Some(ParsedOAuthRefreshLease::Held {
                    expires_at_unix_secs,
                    ..
                }) => {
                    let remaining =
                        Duration::from_secs(expires_at_unix_secs.saturating_sub(start_unix))
                            .saturating_sub(safety_margin);
                    (
                        expires_at_unix_secs,
                        start_instant + remaining.min(effective_ttl),
                    )
                }
                _ => (u64::MAX, start_instant + effective_ttl),
            };
        let state = Arc::new(tokio::sync::Mutex::new(LeaseHeartbeatState::Active(
            initial_token,
        )));
        let lease_cancel = parent_cancel.child_token();
        let stop_heartbeat = CancellationToken::new();

        let controller_bg = Arc::clone(&controller);
        let state_bg = Arc::clone(&state);
        let lease_cancel_bg = lease_cancel.clone();
        let stop_bg = stop_heartbeat.clone();
        let parent_bg = parent_cancel;
        let heartbeat = tokio::spawn(async move {
            let expiry_sleep = tokio::time::sleep_until(initial_deadline);
            tokio::pin!(expiry_sleep);
            loop {
                tokio::select! {
                    biased;
                    _ = stop_bg.cancelled() => break,
                    _ = parent_bg.cancelled() => break,
                    () = &mut expiry_sleep => {
                        mark_lease_lost(&state_bg, &lease_cancel_bg, None).await;
                        break;
                    }
                    () = tokio::time::sleep(renew_interval) => {}
                }
                let current_token = {
                    let guard = state_bg.lock().await;
                    match &*guard {
                        LeaseHeartbeatState::Active(token) => token.clone(),
                        LeaseHeartbeatState::Lost { .. } | LeaseHeartbeatState::Released => break,
                    }
                };
                let renew_started = tokio::time::Instant::now();
                let renew_unix = unix_secs(clock.now());
                if renew_unix >= confirmed_expires_at {
                    mark_lease_lost(&state_bg, &lease_cancel_bg, None).await;
                    break;
                }
                let pending_token = match parse_oauth_refresh_lease(&current_token) {
                    Some(ParsedOAuthRefreshLease::Held {
                        generation, holder, ..
                    }) => Some(oauth_refresh_lease_token(generation, &holder, renew_unix)),
                    _ => None,
                };
                let renew_result = tokio::select! {
                    biased;
                    _ = stop_bg.cancelled() => {
                        if let Some(token) = pending_token {
                            let mut guard = state_bg.lock().await;
                            if matches!(&*guard, LeaseHeartbeatState::Active(tok) if tok == &current_token)
                            {
                                *guard = LeaseHeartbeatState::Active(token);
                            }
                        }
                        break;
                    }
                    _ = parent_bg.cancelled() => {
                        if let Some(token) = pending_token {
                            let mut guard = state_bg.lock().await;
                            if matches!(&*guard, LeaseHeartbeatState::Active(tok) if tok == &current_token)
                            {
                                *guard = LeaseHeartbeatState::Active(token);
                            }
                        }
                        break;
                    }
                    () = &mut expiry_sleep => {
                        mark_lease_lost(&state_bg, &lease_cancel_bg, pending_token).await;
                        break;
                    }
                    result = controller_bg.renew(upstream_id, &current_token, renew_unix) => result,
                };
                match renew_result {
                    Ok(Some(next_token)) => {
                        {
                            let mut guard = state_bg.lock().await;
                            if matches!(&*guard, LeaseHeartbeatState::Active(tok) if tok == &current_token)
                            {
                                *guard = LeaseHeartbeatState::Active(next_token.clone());
                            }
                        }
                        let post_now_unix = unix_secs(clock.now());
                        let post_now_instant = tokio::time::Instant::now();
                        let next_expires_at = match parse_oauth_refresh_lease(&next_token) {
                            Some(ParsedOAuthRefreshLease::Held {
                                expires_at_unix_secs,
                                ..
                            }) => expires_at_unix_secs,
                            _ => u64::MAX,
                        };
                        if post_now_unix >= next_expires_at {
                            mark_lease_lost(&state_bg, &lease_cancel_bg, None).await;
                            break;
                        }
                        let monotonic_deadline = renew_started + effective_ttl;
                        let new_deadline = if next_expires_at == u64::MAX {
                            monotonic_deadline
                        } else {
                            let wall_remaining =
                                Duration::from_secs(next_expires_at.saturating_sub(post_now_unix))
                                    .saturating_sub(safety_margin);
                            let wall_deadline = post_now_instant + wall_remaining;
                            monotonic_deadline.min(wall_deadline)
                        };
                        if new_deadline <= post_now_instant {
                            mark_lease_lost(&state_bg, &lease_cancel_bg, None).await;
                            break;
                        }
                        confirmed_expires_at = next_expires_at;
                        expiry_sleep.as_mut().reset(new_deadline);
                    }
                    Ok(None) => {
                        mark_lease_lost(&state_bg, &lease_cancel_bg, None).await;
                        break;
                    }
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            upstream_id = %upstream_id,
                            "transient storage error renewing oauth refresh lease",
                        );
                        if unix_secs(clock.now()) >= confirmed_expires_at {
                            mark_lease_lost(&state_bg, &lease_cancel_bg, None).await;
                            break;
                        }
                    }
                }
            }
        });

        Self {
            upstream_id,
            expected_generation,
            controller,
            state,
            lease_cancel,
            stop_heartbeat,
            heartbeat: Some(heartbeat),
            released: false,
            provider_exchange_started: false,
        }
    }

    pub(crate) fn cancel_token(&self) -> CancellationToken {
        self.lease_cancel.clone()
    }

    pub(crate) fn mark_provider_exchange_started(&mut self) {
        self.provider_exchange_started = true;
    }

    pub(crate) fn provider_exchange_started(&self) -> bool {
        self.provider_exchange_started
    }

    pub(crate) async fn is_lost(&self) -> bool {
        matches!(&*self.state.lock().await, LeaseHeartbeatState::Lost { .. })
    }

    pub(crate) async fn fence(&self) -> Result<Option<OAuthRefreshFence>, ()> {
        let guard = self.state.lock().await;
        match &*guard {
            LeaseHeartbeatState::Active(token) => Ok(self
                .controller
                .meta_key(self.upstream_id)
                .map(|lease_meta_key| OAuthRefreshFence {
                    expected_generation: self.expected_generation,
                    lease_meta_key,
                    expected_lease_prefix: oauth_refresh_lease_fence_prefix(token),
                })),
            LeaseHeartbeatState::Lost { .. } | LeaseHeartbeatState::Released => Err(()),
        }
    }

    async fn retire_heartbeat(&mut self) -> Option<u64> {
        self.released = true;
        self.stop_heartbeat.cancel();
        if let Some(handle) = self.heartbeat.take() {
            let _ = handle.await;
        }
        let mut guard = self.state.lock().await;
        let expires_at = match &*guard {
            LeaseHeartbeatState::Active(token)
            | LeaseHeartbeatState::Lost {
                cleanup_token: token,
            } => match parse_oauth_refresh_lease(token) {
                Some(ParsedOAuthRefreshLease::Held {
                    expires_at_unix_secs,
                    ..
                }) => Some(expires_at_unix_secs),
                _ => None,
            },
            LeaseHeartbeatState::Released => None,
        };
        *guard = LeaseHeartbeatState::Released;
        expires_at
    }

    pub(crate) async fn mark_committed_and_released(&mut self) {
        let _ = self.retire_heartbeat().await;
    }

    /// Stops heartbeat renewals and disarms Drop cleanup without clearing the
    /// persisted `held:...` lease row so exclusive ownership holds until the
    /// existing TTL expires when a post-send failure leaves rotation uncertain.
    /// Returns the lease expiry unix timestamp when a `held:...` token was held.
    pub(crate) async fn retire_without_release(mut self) -> Option<u64> {
        self.retire_heartbeat().await
    }

    fn spawn_release(
        &mut self,
        final_generation: u64,
    ) -> tokio::task::JoinHandle<StorageResult<()>> {
        self.released = true;
        self.stop_heartbeat.cancel();
        let heartbeat = self.heartbeat.take();
        let upstream_id = self.upstream_id;
        let controller = Arc::clone(&self.controller);
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            if let Some(handle) = heartbeat {
                let _ = handle.await;
            }
            if let Some(token) = take_release_token(&state).await
                && let Err(error) = controller
                    .release(upstream_id, &token, final_generation)
                    .await
            {
                tracing::warn!(
                    error = %error,
                    upstream_id = %upstream_id,
                    "failed to release oauth refresh lease",
                );
                return Err(error);
            }
            Ok(())
        })
    }

    pub(crate) async fn release(mut self, final_generation: u64) -> StorageResult<()> {
        let release_task = self.spawn_release(final_generation);
        release_task.await.map_err(|_| StorageError::Fatal {
            message: "oauth refresh lease release task aborted".to_owned(),
        })?
    }
}

const OPENAI_OAUTH_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
const OPENAI_CODEX_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
use cc_lb_oauth_protocol::GEMINI_OAUTH_TOKEN_URL;
const GEMINI_OAUTH_CLIENT_ID: &str =
    "681255809395-oo8ft2oprdrnp9e3aqf6av3hmdib135j.apps.googleusercontent.com";
const GEMINI_OAUTH_CLIENT_SECRET: &str = "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";

type HyperTokenClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>;

#[derive(Clone)]
struct TokenHttpClient {
    client: HyperTokenClient,
    timeout: Duration,
}

impl TokenHttpClient {
    fn new(timeout: Duration) -> Self {
        let connector = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();
        let client = Client::builder(TokioExecutor::new()).build(connector);
        Self { client, timeout }
    }
}

type InFlightRefreshOutcome = Result<(), String>;
type InFlightRefreshMap = Arc<
    std::sync::Mutex<
        std::collections::HashMap<
            Uuid,
            tokio::sync::watch::Receiver<Option<InFlightRefreshOutcome>>,
        >,
    >,
>;

struct InFlightRefreshGuard {
    in_flight: InFlightRefreshMap,
    upstream_id: Uuid,
}

impl Drop for InFlightRefreshGuard {
    fn drop(&mut self) {
        self.in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.upstream_id);
    }
}

#[derive(Clone)]
pub struct LazyRefresher {
    stores: Arc<Stores>,
    aead: Arc<AeadService>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    google_config: Arc<cc_lb_config::Config>,
    replica_id: Uuid,
    http: TokenHttpClient,
    metadata_hook: Option<MetadataHookHandle>,
    cancel: CancellationToken,
    claim_guard: Arc<dyn LazyRefreshClaimGuard>,
    contention: LazyRefreshContentionConfig,
    apalis_handle: SchedulerBackend,
    clock: ClockHandle,
    in_flight: InFlightRefreshMap,
}

#[derive(Clone, Copy)]
pub struct LazyRefreshContentionConfig {
    wait_timeout: Duration,
    poll_interval: Duration,
}

impl LazyRefreshContentionConfig {
    const fn production() -> Self {
        Self {
            wait_timeout: Duration::from_secs(LAZY_REFRESH_CONTENTION_WAIT_SECS),
            poll_interval: Duration::from_secs(LAZY_REFRESH_POLL_INTERVAL_SECS),
        }
    }

    #[cfg(test)]
    const fn for_tests(wait_timeout: Duration, poll_interval: Duration) -> Self {
        Self {
            wait_timeout,
            poll_interval,
        }
    }
}

/// Exclusive lease token granting one caller permission to execute an
/// in-process OAuth refresh fallback after a scheduler-backed wait.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LazyRefreshFallbackLease(String);

impl LazyRefreshFallbackLease {
    pub fn from_token(token: String) -> Self {
        Self(token)
    }

    pub fn token(&self) -> &str {
        &self.0
    }
}

#[async_trait]
pub trait LazyRefreshClaimGuard: Send + Sync {
    async fn begin_refresh(
        &self,
        upstream_id: Uuid,
        holder: &str,
        expires_at_unix_secs: u64,
        now_unix_secs: u64,
    ) -> StorageResult<LazyRefreshClaim>;

    async fn begin_refresh_for_generation(
        &self,
        upstream_id: Uuid,
        holder: &str,
        expires_at_unix_secs: u64,
        _expected_generation: u64,
        now_unix_secs: u64,
    ) -> StorageResult<LazyRefreshClaim> {
        self.begin_refresh(upstream_id, holder, expires_at_unix_secs, now_unix_secs)
            .await
    }

    async fn complete_and_bump_generation(
        &self,
        upstream_id: Uuid,
        holder: &str,
        generation: u64,
    ) -> StorageResult<bool>;

    async fn release_if_holder(&self, upstream_id: Uuid, holder: &str) -> StorageResult<bool>;

    async fn task_state(&self, _idempotency_key: &str) -> StorageResult<LazyRefreshTaskState> {
        Ok(LazyRefreshTaskState::Active)
    }

    /// Return `Some(lease)` only when this claim guard acquires exclusive
    /// credential ownership across all scheduled and lazy refresh paths for
    /// `upstream_id` at `generation`. Defaults to fail-closed (`None`).
    async fn try_claim_fallback(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _idempotency_key: &str,
        _generation: u64,
        _now_unix_secs: u64,
    ) -> StorageResult<Option<LazyRefreshFallbackLease>> {
        Ok(None)
    }

    async fn renew_fallback_claim(
        &self,
        _upstream_id: Uuid,
        lease: &LazyRefreshFallbackLease,
        _now_unix_secs: u64,
    ) -> StorageResult<Option<LazyRefreshFallbackLease>> {
        Ok(Some(lease.clone()))
    }

    fn fallback_lease_meta_key(&self, _upstream_id: Uuid) -> Option<String> {
        None
    }

    async fn finish_fallback_claim(
        &self,
        _upstream_id: Uuid,
        _lease: &LazyRefreshFallbackLease,
        _final_generation: u64,
    ) -> StorageResult<()> {
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LazyRefreshClaim {
    Acquired,
    Enqueued { idempotency_key: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LazyRefreshTaskState {
    Active,
    Done,
    TerminalFailure { reason: String },
}

enum WaitGenerationOutcome {
    GenerationAdvanced,
    JobDoneWithoutAdvance,
    JobTerminalFailure { reason: String },
    TimedOut { reason: String },
    Cancelled,
}

#[derive(Clone)]
pub struct ApalisLazyRefreshClaimGuard {
    scheduler_backend: SchedulerBackend,
    meta: Arc<dyn MetaStore>,
}

impl std::fmt::Debug for ApalisLazyRefreshClaimGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApalisLazyRefreshClaimGuard")
            .field("scheduler_backend", &self.scheduler_backend)
            .finish_non_exhaustive()
    }
}

impl ApalisLazyRefreshClaimGuard {
    pub fn new(scheduler_backend: SchedulerBackend, meta: Arc<dyn MetaStore>) -> Self {
        Self {
            scheduler_backend,
            meta,
        }
    }

    async fn begin_refresh_task(
        &self,
        job: OAuthRefreshJob,
        idempotency_key: String,
        expires_at_unix_secs: u64,
        now_unix_secs: u64,
    ) -> StorageResult<LazyRefreshClaim> {
        let _ = expires_at_unix_secs;
        let task = SchedulerPushTask {
            args: AdaptiveJob::OAuthRefresh(job),
            idempotency_key: Some(idempotency_key.clone()),
            run_at_unix_secs: Some(now_unix_secs),
            max_attempts: None,
        };
        match self
            .scheduler_backend
            .push_or_expedite_adaptive_task(task)
            .await
        {
            Ok(()) | Err(cc_lb_scheduler::error::SchedulerError::Conflict(_)) => {
                Ok(LazyRefreshClaim::Enqueued { idempotency_key })
            }
            Err(error) => Err(scheduler_claim_error(error)),
        }
    }
}

#[async_trait]
impl LazyRefreshClaimGuard for ApalisLazyRefreshClaimGuard {
    async fn begin_refresh(
        &self,
        upstream_id: Uuid,
        _holder: &str,
        expires_at_unix_secs: u64,
        now_unix_secs: u64,
    ) -> StorageResult<LazyRefreshClaim> {
        let job = OAuthRefreshJob::new(upstream_id);
        let idempotency_key = job.idempotency_key(expires_at_unix_secs);
        self.begin_refresh_task(job, idempotency_key, expires_at_unix_secs, now_unix_secs)
            .await
    }

    async fn begin_refresh_for_generation(
        &self,
        upstream_id: Uuid,
        _holder: &str,
        expires_at_unix_secs: u64,
        expected_generation: u64,
        now_unix_secs: u64,
    ) -> StorageResult<LazyRefreshClaim> {
        let job =
            OAuthRefreshJob::for_credential(upstream_id, expected_generation, expires_at_unix_secs);
        let canonical_key =
            job.generation_idempotency_key(expected_generation, expires_at_unix_secs);
        self.begin_refresh_task(
            job,
            canonical_key.clone(),
            expires_at_unix_secs,
            now_unix_secs,
        )
        .await?;
        if matches!(
            self.scheduler_backend
                .adaptive_task_key_state(&canonical_key)
                .await
                .map_err(scheduler_claim_error)?,
            AdaptiveTaskKeyState::Active
        ) {
            return Ok(LazyRefreshClaim::Enqueued {
                idempotency_key: canonical_key,
            });
        }
        if let Some(active_key) = self
            .scheduler_backend
            .find_active_adaptive_key_in_family(&canonical_key)
            .await
            .map_err(scheduler_claim_error)?
            && active_key != canonical_key
        {
            let recovery_job = OAuthRefreshJob::for_credential(
                upstream_id,
                expected_generation,
                expires_at_unix_secs,
            );
            return self
                .begin_refresh_task(
                    recovery_job,
                    active_key,
                    expires_at_unix_secs,
                    now_unix_secs,
                )
                .await;
        }
        Ok(LazyRefreshClaim::Enqueued {
            idempotency_key: canonical_key,
        })
    }

    async fn complete_and_bump_generation(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _generation: u64,
    ) -> StorageResult<bool> {
        Ok(false)
    }

    async fn release_if_holder(&self, _upstream_id: Uuid, _holder: &str) -> StorageResult<bool> {
        Ok(false)
    }

    async fn task_state(&self, idempotency_key: &str) -> StorageResult<LazyRefreshTaskState> {
        apalis_lazy_refresh_task_state(&self.scheduler_backend, idempotency_key).await
    }

    async fn try_claim_fallback(
        &self,
        upstream_id: Uuid,
        holder: &str,
        _idempotency_key: &str,
        generation: u64,
        now_unix_secs: u64,
    ) -> StorageResult<Option<LazyRefreshFallbackLease>> {
        Ok(try_acquire_oauth_refresh_lease_with_holder(
            self.meta.as_ref(),
            upstream_id,
            generation,
            holder,
            now_unix_secs,
        )
        .await?
        .map(|lease| LazyRefreshFallbackLease::from_token(lease.token)))
    }

    async fn renew_fallback_claim(
        &self,
        upstream_id: Uuid,
        lease: &LazyRefreshFallbackLease,
        now_unix_secs: u64,
    ) -> StorageResult<Option<LazyRefreshFallbackLease>> {
        Ok(renew_oauth_refresh_lease(
            self.meta.as_ref(),
            upstream_id,
            lease.token(),
            now_unix_secs,
        )
        .await?
        .map(LazyRefreshFallbackLease::from_token))
    }

    fn fallback_lease_meta_key(&self, upstream_id: Uuid) -> Option<String> {
        Some(oauth_refresh_lease_meta_key(upstream_id))
    }

    async fn finish_fallback_claim(
        &self,
        upstream_id: Uuid,
        lease: &LazyRefreshFallbackLease,
        final_generation: u64,
    ) -> StorageResult<()> {
        release_oauth_refresh_lease(
            self.meta.as_ref(),
            upstream_id,
            lease.token(),
            final_generation,
        )
        .await
    }
}

async fn apalis_lazy_refresh_task_state(
    scheduler_backend: &SchedulerBackend,
    idempotency_key: &str,
) -> StorageResult<LazyRefreshTaskState> {
    match scheduler_backend
        .adaptive_task_key_family_state(idempotency_key)
        .await
        .map_err(scheduler_claim_error)?
    {
        AdaptiveTaskKeyState::Active | AdaptiveTaskKeyState::Missing => {
            Ok(LazyRefreshTaskState::Active)
        }
        AdaptiveTaskKeyState::Done => Ok(LazyRefreshTaskState::Done),
        AdaptiveTaskKeyState::TerminalFailure => Ok(LazyRefreshTaskState::TerminalFailure {
            reason: "oauth refresh job failed permanently".to_owned(),
        }),
    }
}

enum ClaimHandoff<T> {
    Pending,
    Delivered(T),
    Taken,
    WaiterAbandoned,
}

fn take_delivered_handoff<T>(handoff: &std::sync::Mutex<ClaimHandoff<T>>) -> Option<T> {
    let mut guard = handoff
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match std::mem::replace(&mut *guard, ClaimHandoff::Taken) {
        ClaimHandoff::Delivered(value) => Some(value),
        other => {
            *guard = other;
            None
        }
    }
}

fn abandon_or_take_delivered_handoff<T>(handoff: &std::sync::Mutex<ClaimHandoff<T>>) -> Option<T> {
    let mut guard = handoff
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match std::mem::replace(&mut *guard, ClaimHandoff::WaiterAbandoned) {
        ClaimHandoff::Pending | ClaimHandoff::WaiterAbandoned => None,
        ClaimHandoff::Delivered(value) => Some(value),
        ClaimHandoff::Taken => {
            *guard = ClaimHandoff::Taken;
            None
        }
    }
}

fn handoff_try_deliver<T>(handoff: &std::sync::Mutex<ClaimHandoff<T>>, value: T) -> Option<T> {
    let mut state = handoff
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match &*state {
        ClaimHandoff::WaiterAbandoned => Some(value),
        _ => {
            *state = ClaimHandoff::Delivered(value);
            None
        }
    }
}

#[derive(Clone)]
struct ScheduledLeaseTarget {
    meta: Arc<dyn MetaStore>,
    upstreams: Arc<dyn cc_lb_storage_api::UpstreamStore>,
    upstream_id: Uuid,
    generation: u64,
}

async fn release_abandoned_scheduled_lease(target: ScheduledLeaseTarget, lease_token: String) {
    let final_generation = target
        .upstreams
        .read_oauth_token_generation(target.upstream_id)
        .await
        .ok()
        .flatten()
        .unwrap_or(target.generation);
    if let Err(error) = release_oauth_refresh_lease(
        target.meta.as_ref(),
        target.upstream_id,
        &lease_token,
        final_generation,
    )
    .await
    {
        tracing::warn!(
            error = %error,
            upstream_id = %target.upstream_id,
            generation = target.generation,
            "failed to release abandoned scheduled oauth refresh lease",
        );
    }
}

/// Ensures any scheduled lease committed by `try_acquire_oauth_refresh_lease_with_holder`
/// is released immediately if a worker timeout or shutdown drops the waiter before delivery.
struct ScheduledLeaseWaiterGuard {
    handoff: Arc<std::sync::Mutex<ClaimHandoff<OAuthRefreshLease>>>,
    target: ScheduledLeaseTarget,
}

impl ScheduledLeaseWaiterGuard {
    fn take_delivered(&self) -> Option<OAuthRefreshLease> {
        take_delivered_handoff(&self.handoff)
    }
}

impl Drop for ScheduledLeaseWaiterGuard {
    fn drop(&mut self) {
        if let Some(abandoned_lease) = abandon_or_take_delivered_handoff(&self.handoff) {
            let target = self.target.clone();
            tokio::spawn(release_abandoned_scheduled_lease(
                target,
                abandoned_lease.token,
            ));
        }
    }
}

pub(crate) async fn try_acquire_oauth_refresh_lease_cancellation_safe(
    meta: Arc<dyn MetaStore>,
    upstreams: Arc<dyn cc_lb_storage_api::UpstreamStore>,
    upstream_id: Uuid,
    generation: u64,
    now_unix_secs: u64,
) -> StorageResult<Option<OAuthRefreshLease>> {
    let target = ScheduledLeaseTarget {
        meta,
        upstreams,
        upstream_id,
        generation,
    };
    let handoff = Arc::new(std::sync::Mutex::new(ClaimHandoff::Pending));
    let guard = ScheduledLeaseWaiterGuard {
        handoff: Arc::clone(&handoff),
        target: target.clone(),
    };

    let (tx, rx) = tokio::sync::oneshot::channel::<StorageResult<Option<()>>>();
    let attempt_holder = Uuid::new_v4().to_string();
    let proposed_token = oauth_refresh_lease_token(generation, &attempt_holder, now_unix_secs);
    let task_handoff = Arc::clone(&handoff);
    tokio::spawn(async move {
        let result = try_acquire_oauth_refresh_lease_with_holder(
            target.meta.as_ref(),
            target.upstream_id,
            target.generation,
            &attempt_holder,
            now_unix_secs,
        )
        .await;
        match result {
            Ok(Some(lease)) => {
                if let Some(abandoned_lease) = handoff_try_deliver(&task_handoff, lease) {
                    release_abandoned_scheduled_lease(target, abandoned_lease.token).await;
                    return;
                }
                let _ = tx.send(Ok(Some(())));
            }
            Ok(None) => {
                let _ = tx.send(Ok(None));
            }
            Err(error) => {
                release_abandoned_scheduled_lease(target, proposed_token).await;
                let _ = tx.send(Err(error));
            }
        }
    });

    let signal = rx.await.map_err(|_| StorageError::Fatal {
        message: "scheduled oauth refresh lease claim task aborted".to_owned(),
    })?;
    match signal? {
        Some(()) => Ok(guard.take_delivered()),
        None => Ok(None),
    }
}

#[derive(Clone)]
struct FallbackClaimTarget {
    claim_guard: Arc<dyn LazyRefreshClaimGuard>,
    stores: Arc<Stores>,
    upstream_id: Uuid,
    idempotency_key: String,
    generation: u64,
}

async fn release_abandoned_fallback_lease(
    target: FallbackClaimTarget,
    lease: LazyRefreshFallbackLease,
) {
    let final_generation = target
        .stores
        .upstreams
        .read_oauth_token_generation(target.upstream_id)
        .await
        .ok()
        .flatten()
        .unwrap_or(target.generation);
    if let Err(error) = target
        .claim_guard
        .finish_fallback_claim(target.upstream_id, &lease, final_generation)
        .await
    {
        tracing::warn!(
            error = %error,
            upstream_id = %target.upstream_id,
            idempotency_key = %target.idempotency_key,
            "failed to release abandoned oauth refresh fallback lease",
        );
    }
}

/// Ensures any lease committed by `try_claim_fallback` is deterministically
/// released even if the caller's future is cancelled or dropped after the
/// database write commits and before the waiter receives the lease token.
struct FallbackClaimWaiterGuard {
    handoff: Arc<std::sync::Mutex<ClaimHandoff<LazyRefreshFallbackLease>>>,
    target: FallbackClaimTarget,
}

impl FallbackClaimWaiterGuard {
    fn take_delivered(&self) -> Option<LazyRefreshFallbackLease> {
        take_delivered_handoff(&self.handoff)
    }
}

impl Drop for FallbackClaimWaiterGuard {
    fn drop(&mut self) {
        if let Some(lease) = abandon_or_take_delivered_handoff(&self.handoff) {
            let target = self.target.clone();
            tokio::spawn(release_abandoned_fallback_lease(target, lease));
        }
    }
}

async fn try_claim_fallback_cancellation_safe(
    target: FallbackClaimTarget,
    now_unix_secs: u64,
) -> Result<Option<LazyRefreshFallbackLease>, LazyRefreshError> {
    let handoff = Arc::new(std::sync::Mutex::new(ClaimHandoff::Pending));
    let guard = FallbackClaimWaiterGuard {
        handoff: Arc::clone(&handoff),
        target: target.clone(),
    };

    // Pass only a completion signal through the oneshot so cancelling before
    // `rx.await` polls never drops the lease inside an unread channel buffer.
    let (tx, rx) = tokio::sync::oneshot::channel::<StorageResult<Option<()>>>();
    let attempt_holder = Uuid::new_v4().to_string();
    let proposed_lease = LazyRefreshFallbackLease::from_token(oauth_refresh_lease_token(
        target.generation,
        &attempt_holder,
        now_unix_secs,
    ));
    let task_handoff = Arc::clone(&handoff);
    tokio::spawn(async move {
        let result = target
            .claim_guard
            .try_claim_fallback(
                target.upstream_id,
                &attempt_holder,
                &target.idempotency_key,
                target.generation,
                now_unix_secs,
            )
            .await;
        match result {
            Ok(Some(lease)) => {
                if let Some(abandoned_lease) = handoff_try_deliver(&task_handoff, lease) {
                    release_abandoned_fallback_lease(target, abandoned_lease).await;
                    return;
                }
                let _ = tx.send(Ok(Some(())));
            }
            Ok(None) => {
                let _ = tx.send(Ok(None));
            }
            Err(error) => {
                release_abandoned_fallback_lease(target, proposed_lease).await;
                let _ = tx.send(Err(error));
            }
        }
    });

    let signal = rx.await.map_err(|_| LazyRefreshError::Failed {
        reason: "oauth refresh fallback claim task aborted".to_owned(),
    })?;
    match signal.map_err(lazy_error)? {
        Some(()) => Ok(guard.take_delivered()),
        None => Ok(None),
    }
}

impl LazyRefresher {
    pub fn new(params: LazyRefresherParams) -> Self {
        let claim_guard =
            lazy_refresh_claim_guard_from_scheduler(&params.apalis_handle, params.meta);
        Self::new_with_claim_guard(
            params.deps,
            params.replica_id,
            params.metadata_hook,
            params.cancel,
            claim_guard,
            params.apalis_handle,
        )
    }

    pub fn new_with_claim_guard(
        deps: LazyRefresherDeps,
        replica_id: Uuid,
        metadata_hook: Option<MetadataHookHandle>,
        cancel: CancellationToken,
        claim_guard: Arc<dyn LazyRefreshClaimGuard>,
        apalis_handle: SchedulerBackend,
    ) -> Self {
        Self::new_with_claim_guard_and_config(
            deps,
            replica_id,
            metadata_hook,
            cancel,
            claim_guard,
            LazyRefreshContentionConfig::production(),
            apalis_handle,
        )
    }

    #[cfg(test)]
    fn new_with_claim_guard_for_tests(
        deps: LazyRefresherDeps,
        replica_id: Uuid,
        metadata_hook: Option<MetadataHookHandle>,
        cancel: CancellationToken,
        claim_guard: Arc<dyn LazyRefreshClaimGuard>,
        contention: LazyRefreshContentionConfig,
        apalis_handle: SchedulerBackend,
    ) -> Self {
        Self::new_with_claim_guard_and_config(
            deps,
            replica_id,
            metadata_hook,
            cancel,
            claim_guard,
            contention,
            apalis_handle,
        )
    }

    fn new_with_claim_guard_and_config(
        deps: LazyRefresherDeps,
        replica_id: Uuid,
        metadata_hook: Option<MetadataHookHandle>,
        cancel: CancellationToken,
        claim_guard: Arc<dyn LazyRefreshClaimGuard>,
        contention: LazyRefreshContentionConfig,
        apalis_handle: SchedulerBackend,
    ) -> Self {
        let http = TokenHttpClient::new(Duration::from_secs(30));
        Self {
            stores: deps.stores,
            aead: deps.aead,
            oauth_cfg: deps.oauth_cfg,
            google_config: deps.google_config,
            replica_id,
            http,
            metadata_hook,
            cancel,
            claim_guard,
            contention,
            apalis_handle,
            clock: deps.clock,
            in_flight: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    async fn wait_for_token_generation_until(
        &self,
        upstream_id: Uuid,
        starting_generation: u64,
        idempotency_key: Option<&str>,
        deadline: tokio::time::Instant,
    ) -> Result<WaitGenerationOutcome, LazyRefreshError> {
        loop {
            let Some(current_generation) = self
                .stores
                .upstreams
                .read_oauth_token_generation(upstream_id)
                .await
                .map_err(lazy_error)?
            else {
                return Err(LazyRefreshError::Failed {
                    reason: "oauth upstream not found".to_owned(),
                });
            };
            if current_generation > starting_generation {
                return Ok(WaitGenerationOutcome::GenerationAdvanced);
            }

            if let Some(idempotency_key) = idempotency_key {
                let task_state = self
                    .claim_guard
                    .task_state(idempotency_key)
                    .await
                    .map_err(lazy_error)?;
                if !matches!(task_state, LazyRefreshTaskState::Active) {
                    // The worker may have committed complete_refresh AND marked the task
                    // terminal between our generation read above and this task_state read.
                    // Re-read the generation so we observe its post-commit state before
                    // concluding that the job completed without advancing the generation.
                    let Some(post_terminal_generation) = self
                        .stores
                        .upstreams
                        .read_oauth_token_generation(upstream_id)
                        .await
                        .map_err(lazy_error)?
                    else {
                        return Err(LazyRefreshError::Failed {
                            reason: "oauth upstream not found".to_owned(),
                        });
                    };
                    if post_terminal_generation > starting_generation {
                        return Ok(WaitGenerationOutcome::GenerationAdvanced);
                    }
                    return Ok(match task_state {
                        LazyRefreshTaskState::Done => WaitGenerationOutcome::JobDoneWithoutAdvance,
                        LazyRefreshTaskState::TerminalFailure { reason } => {
                            WaitGenerationOutcome::JobTerminalFailure { reason }
                        }
                        LazyRefreshTaskState::Active => unreachable!(),
                    });
                }
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                ::metrics::counter!(cc_lb_scheduler::scheduler_metrics::LAZY_REFRESH_TIMEOUT_TOTAL)
                    .increment(1);
                return Ok(WaitGenerationOutcome::TimedOut {
                    reason: "oauth refresh timed out waiting for another holder".to_owned(),
                });
            }
            let sleep_for = self.contention.poll_interval.min(deadline - now);
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    return Ok(WaitGenerationOutcome::Cancelled);
                }
                _ = tokio::time::sleep(sleep_for) => {}
            }
        }
    }
}

pub(crate) fn lazy_refresh_claim_guard_from_scheduler(
    scheduler_backend: &SchedulerBackend,
    meta: Arc<dyn MetaStore>,
) -> Arc<dyn LazyRefreshClaimGuard> {
    Arc::new(ApalisLazyRefreshClaimGuard::new(
        scheduler_backend.clone(),
        meta,
    ))
}

#[derive(Clone)]
pub struct LazyRefresherDeps {
    pub stores: Arc<Stores>,
    pub aead: Arc<AeadService>,
    pub oauth_cfg: Arc<AnthropicOAuthConfig>,
    pub google_config: Arc<cc_lb_config::Config>,
    pub clock: ClockHandle,
}

pub struct LazyRefresherParams {
    pub deps: LazyRefresherDeps,
    pub replica_id: Uuid,
    pub metadata_hook: Option<MetadataHookHandle>,
    pub cancel: CancellationToken,
    pub apalis_handle: SchedulerBackend,
    pub meta: Arc<dyn MetaStore>,
}

impl LazyRefresher {
    async fn run_in_process_refresh(
        self,
        upstream_id: Uuid,
        starting_generation: u64,
        active_lease: &mut ActiveOAuthRefreshLease,
    ) -> Result<u64, RefreshError> {
        let upstream = self
            .stores
            .upstreams
            .get_by_id(upstream_id)
            .await?
            .ok_or(RefreshError::NotFound)?;
        if upstream.oauth_token_generation > starting_generation {
            return Ok(upstream.oauth_token_generation);
        }
        if upstream.oauth_token_generation != starting_generation {
            return Err(RefreshError::LeaseLost);
        }
        let is_anthropic_oauth = upstream.kind == UpstreamKind::AnthropicOauth;
        let refreshed = refresh_flow(
            &self.stores,
            self.stores.audit.as_deref(),
            &self.aead,
            &self.oauth_cfg,
            &self.google_config,
            self.replica_id,
            &self.http,
            &self.cancel,
            upstream,
            &*self.clock,
            Some(active_lease),
            Some(starting_generation),
        )
        .await?;
        if let Some(expires_at_unix_secs) = refreshed.expires_at_unix_secs {
            let job = OAuthRefreshJob::for_credential(
                upstream_id,
                refreshed.generation,
                expires_at_unix_secs,
            );
            let idempotency_key =
                job.generation_idempotency_key(refreshed.generation, expires_at_unix_secs);
            let task = SchedulerPushTask {
                args: AdaptiveJob::OAuthRefresh(job),
                idempotency_key: Some(idempotency_key),
                run_at_unix_secs: Some(OAuthRefreshJob::run_at_for_expires_at(
                    expires_at_unix_secs,
                )),
                max_attempts: None,
            };
            match self.apalis_handle.push_adaptive_task(task).await {
                Ok(()) | Err(cc_lb_scheduler::error::SchedulerError::Conflict(_)) => {}
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        upstream_id = %upstream_id,
                        expires_at_unix_secs,
                        "failed to schedule successor oauth refresh job after in-process fallback",
                    );
                }
            }
        }
        if refreshed.expires_at_unix_secs.is_some()
            && is_anthropic_oauth
            && let Some(metadata_hook) = &self.metadata_hook
        {
            metadata_hook
                .enqueue(MetadataHookRequest {
                    upstream_id,
                    credential_generation: refreshed.generation,
                    traceparent: None,
                })
                .await
                .map_err(|error| RefreshError::MetadataHook(error.to_string()))?;
        }
        Ok(refreshed.generation)
    }

    async fn execute_refresh_one(self, upstream_id: Uuid) -> Result<(), LazyRefreshError> {
        let upstream = self
            .stores
            .upstreams
            .get_by_id(upstream_id)
            .await
            .map_err(lazy_error)?
            .ok_or_else(|| LazyRefreshError::Failed {
                reason: "oauth upstream not found".to_owned(),
            })?;
        let bundle = oauth_token_bundle(&upstream, &self.aead)?;
        if bundle.never_refresh {
            // Access-only imports and Anthropic long-lived grants must stop
            // before acquiring a refresh claim or contacting a token endpoint.
            return Err(LazyRefreshError::Failed {
                reason: RefreshError::NotRefreshable.to_string(),
            });
        }
        check_refresh_allowed(&self.stores, &upstream, &bundle, &*self.clock)
            .await
            .map_err(lazy_refresh_error)?;
        let holder = lazy_refresh_holder(self.replica_id);
        let expires_at_unix_secs = bundle.expires_at_unix_secs;
        let starting_generation = upstream.oauth_token_generation;
        let claim = self
            .claim_guard
            .begin_refresh_for_generation(
                upstream_id,
                &holder,
                expires_at_unix_secs,
                starting_generation,
                unix_secs(self.clock.now()),
            )
            .await
            .map_err(lazy_error)?;
        let wait_deadline = tokio::time::Instant::now() + self.contention.wait_timeout;
        match claim {
            LazyRefreshClaim::Enqueued { idempotency_key } => {
                match self
                    .wait_for_token_generation_until(
                        upstream_id,
                        starting_generation,
                        Some(&idempotency_key),
                        wait_deadline,
                    )
                    .await?
                {
                    WaitGenerationOutcome::GenerationAdvanced => Ok(()),
                    WaitGenerationOutcome::Cancelled => Err(LazyRefreshError::Failed {
                        reason: "oauth refresh cancelled".to_owned(),
                    }),
                    WaitGenerationOutcome::JobTerminalFailure { reason } => {
                        self.run_exclusive_in_process_fallback(
                            upstream_id,
                            &idempotency_key,
                            starting_generation,
                            &reason,
                            wait_deadline,
                        )
                        .await
                    }
                    WaitGenerationOutcome::JobDoneWithoutAdvance => {
                        self.run_exclusive_in_process_fallback(
                            upstream_id,
                            &idempotency_key,
                            starting_generation,
                            "oauth refresh job completed without advancing generation",
                            wait_deadline,
                        )
                        .await
                    }
                    WaitGenerationOutcome::TimedOut { reason } => {
                        self.run_exclusive_in_process_fallback(
                            upstream_id,
                            &idempotency_key,
                            starting_generation,
                            &reason,
                            wait_deadline,
                        )
                        .await
                    }
                }
            }
            LazyRefreshClaim::Acquired => self.run_acquired_refresh(upstream, holder).await,
        }
    }

    async fn run_exclusive_in_process_fallback(
        self,
        upstream_id: Uuid,
        idempotency_key: &str,
        starting_generation: u64,
        contended_reason: &str,
        wait_deadline: tokio::time::Instant,
    ) -> Result<(), LazyRefreshError> {
        enum PollFallback {
            Claimed(LazyRefreshFallbackLease),
            GenerationAdvanced,
            Contended,
        }

        let mut attempt = 0usize;
        let lease = loop {
            if attempt > 0 && tokio::time::Instant::now() >= wait_deadline {
                tracing::warn!(
                    upstream_id = %upstream_id,
                    idempotency_key = %idempotency_key,
                    "skipping in-process oauth refresh fallback because exclusive credential ownership could not be established",
                );
                return Err(LazyRefreshError::Failed {
                    reason: contended_reason.to_owned(),
                });
            }

            let poll_once = async {
                let upstream = self
                    .stores
                    .upstreams
                    .get_by_id(upstream_id)
                    .await
                    .map_err(lazy_error)?
                    .ok_or_else(|| LazyRefreshError::Failed {
                        reason: "oauth upstream not found".to_owned(),
                    })?;
                if upstream.oauth_token_generation > starting_generation {
                    return Ok(PollFallback::GenerationAdvanced);
                }
                let bundle = oauth_token_bundle(&upstream, &self.aead)?;
                if bundle.never_refresh {
                    return Err(LazyRefreshError::Failed {
                        reason: RefreshError::NotRefreshable.to_string(),
                    });
                }
                check_refresh_allowed(&self.stores, &upstream, &bundle, &*self.clock)
                    .await
                    .map_err(lazy_refresh_error)?;

                if let Some(lease) = try_claim_fallback_cancellation_safe(
                    FallbackClaimTarget {
                        claim_guard: Arc::clone(&self.claim_guard),
                        stores: Arc::clone(&self.stores),
                        upstream_id,
                        idempotency_key: idempotency_key.to_owned(),
                        generation: starting_generation,
                    },
                    unix_secs(self.clock.now()),
                )
                .await?
                {
                    return Ok(PollFallback::Claimed(lease));
                }
                if let Some(current_generation) = self
                    .stores
                    .upstreams
                    .read_oauth_token_generation(upstream_id)
                    .await
                    .map_err(lazy_error)?
                    && current_generation > starting_generation
                {
                    return Ok(PollFallback::GenerationAdvanced);
                }
                Ok(PollFallback::Contended)
            };

            let outcome = if attempt == 0 {
                poll_once.await?
            } else {
                tokio::select! {
                    biased;
                    _ = self.cancel.cancelled() => {
                        return Err(LazyRefreshError::Failed {
                            reason: "oauth refresh cancelled".to_owned(),
                        });
                    }
                    res = tokio::time::timeout_at(wait_deadline, poll_once) => {
                        match res {
                            Ok(outcome) => outcome?,
                            Err(_) => {
                                tracing::warn!(
                                    upstream_id = %upstream_id,
                                    idempotency_key = %idempotency_key,
                                    "skipping in-process oauth refresh fallback because wait budget expired during contended storage poll",
                                );
                                return Err(LazyRefreshError::Failed {
                                    reason: contended_reason.to_owned(),
                                });
                            }
                        }
                    }
                }
            };

            match outcome {
                PollFallback::GenerationAdvanced => return Ok(()),
                PollFallback::Claimed(lease) => break lease,
                PollFallback::Contended => {
                    attempt += 1;
                    let now = tokio::time::Instant::now();
                    if now >= wait_deadline {
                        tracing::warn!(
                            upstream_id = %upstream_id,
                            idempotency_key = %idempotency_key,
                            "skipping in-process oauth refresh fallback because exclusive credential ownership could not be established",
                        );
                        return Err(LazyRefreshError::Failed {
                            reason: contended_reason.to_owned(),
                        });
                    }
                    let sleep_for = self.contention.poll_interval.min(wait_deadline - now);
                    tokio::select! {
                        _ = self.cancel.cancelled() => {
                            return Err(LazyRefreshError::Failed {
                                reason: "oauth refresh cancelled".to_owned(),
                            });
                        }
                        _ = tokio::time::sleep(sleep_for) => {}
                    }
                }
            }
        };

        let mut active_lease = ActiveOAuthRefreshLease::start_for_claim_guard(
            Arc::clone(&self.claim_guard),
            self.clock.clone(),
            self.cancel.clone(),
            upstream_id,
            lease,
            starting_generation,
        );

        match self
            .stores
            .upstreams
            .read_oauth_token_generation(upstream_id)
            .await
        {
            Ok(Some(current_generation)) if current_generation > starting_generation => {
                if let Err(error) = active_lease.release(current_generation).await {
                    tracing::warn!(
                        error = %error,
                        upstream_id = %upstream_id,
                        idempotency_key = %idempotency_key,
                        "failed to release oauth refresh fallback lease",
                    );
                }
                return Ok(());
            }
            Ok(_) => {}
            Err(read_error) => {
                if let Err(error) = active_lease.release(starting_generation).await {
                    tracing::warn!(
                        error = %error,
                        upstream_id = %upstream_id,
                        idempotency_key = %idempotency_key,
                        "failed to release oauth refresh fallback lease after post-claim generation read error",
                    );
                }
                return Err(lazy_error(read_error));
            }
        }

        tracing::warn!(
            upstream_id = %upstream_id,
            idempotency_key = %idempotency_key,
            reason = %contended_reason,
            "acquired exclusive fallback ownership after scheduler-backed wait; running in-process refresh",
        );
        ::metrics::counter!("cclb_lazy_refresh_inproc_fallback_total").increment(1);
        let outcome = self
            .clone()
            .run_in_process_refresh(upstream_id, starting_generation, &mut active_lease)
            .await;
        let final_generation = match &outcome {
            Ok(generation) => *generation,
            Err(_) => self
                .stores
                .upstreams
                .read_oauth_token_generation(upstream_id)
                .await
                .ok()
                .flatten()
                .unwrap_or(starting_generation),
        };
        if active_lease.provider_exchange_started()
            && final_generation == starting_generation
            && refresh_outcome_is_ambiguous_after_send(&outcome)
        {
            active_lease.retire_without_release().await;
        } else if let Err(error) = active_lease.release(final_generation).await {
            tracing::warn!(
                error = %error,
                upstream_id = %upstream_id,
                idempotency_key = %idempotency_key,
                "failed to release oauth refresh fallback lease",
            );
        }
        match outcome {
            Ok(_) => Ok(()),
            Err(RefreshError::LeaseLost) => {
                if final_generation > starting_generation {
                    Ok(())
                } else {
                    Err(LazyRefreshError::Failed {
                        reason: RefreshError::LeaseLost.to_string(),
                    })
                }
            }
            Err(error) => Err(lazy_refresh_error(error)),
        }
    }

    async fn run_acquired_refresh(
        self,
        upstream: UpstreamRecord,
        holder: String,
    ) -> Result<(), LazyRefreshError> {
        let upstream_id = upstream.id;
        let is_anthropic_oauth = upstream.kind == UpstreamKind::AnthropicOauth;
        let result = refresh_flow(
            &self.stores,
            self.stores.audit.as_deref(),
            &self.aead,
            &self.oauth_cfg,
            &self.google_config,
            self.replica_id,
            &self.http,
            &self.cancel,
            upstream,
            &*self.clock,
            None,
            None,
        )
        .await;
        match result {
            Ok(refreshed) => {
                let generation = refreshed.generation;
                let completed = self
                    .claim_guard
                    .complete_and_bump_generation(upstream_id, &holder, generation)
                    .await
                    .map_err(lazy_error)?;
                if !completed {
                    return Err(LazyRefreshError::Failed {
                        reason: "oauth refresh claim was not held at completion".to_owned(),
                    });
                }
                if is_anthropic_oauth {
                    if let Some(metadata_hook) = &self.metadata_hook {
                        metadata_hook
                            .enqueue(MetadataHookRequest {
                                upstream_id,
                                credential_generation: generation,
                                traceparent: None,
                            })
                            .await
                            .map_err(lazy_metadata_hook_error)?;
                    } else {
                        self.apalis_handle
                            .push_job(AdaptiveJob::MetadataRefresh(MetadataRefreshJob::new(
                                upstream_id,
                                generation,
                            )))
                            .await
                            .map_err(lazy_scheduler_error)?;
                    }
                }
                Ok(())
            }
            Err(error) => {
                if let Err(release_error) = self
                    .claim_guard
                    .release_if_holder(upstream_id, &holder)
                    .await
                {
                    tracing::warn!(error = %release_error, upstream_id = %upstream_id, "oauth refresh claim release failed");
                }
                Err(LazyRefreshError::Failed {
                    reason: error.to_string(),
                })
            }
        }
    }
}

#[async_trait]
impl LazyRefreshHandle for LazyRefresher {
    async fn refresh_one(&self, upstream_id: Uuid) -> Result<(), LazyRefreshError> {
        // Share one detached per-upstream refresh task across concurrent or
        // subsequent callers so cancelling a caller (for example a timed-out
        // WebSocket handshake) releases nothing prematurely and never races a
        // second credential exchange before the first task persists its result.
        let mut rx = {
            let mut in_flight = self
                .in_flight
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(existing) = in_flight.get(&upstream_id) {
                existing.clone()
            } else {
                let (tx, rx) = tokio::sync::watch::channel(None);
                in_flight.insert(upstream_id, rx.clone());
                let this = self.clone();
                let in_flight_map = Arc::clone(&self.in_flight);
                tokio::spawn(async move {
                    let guard = InFlightRefreshGuard {
                        in_flight: in_flight_map,
                        upstream_id,
                    };
                    let outcome = this
                        .execute_refresh_one(upstream_id)
                        .await
                        .map_err(|error| match error {
                            LazyRefreshError::Failed { reason } => reason,
                        });
                    drop(guard);
                    let _ = tx.send(Some(outcome));
                });
                rx
            }
        };
        let outcome = rx
            .wait_for(Option::is_some)
            .await
            .map_err(|_| LazyRefreshError::Failed {
                reason: "oauth refresh task failed".to_owned(),
            })?;
        match outcome.as_ref().expect("watch waited for Some(outcome)") {
            Ok(()) => Ok(()),
            Err(reason) => Err(LazyRefreshError::Failed {
                reason: reason.clone(),
            }),
        }
    }

    async fn enqueue_only(&self, upstream_id: Uuid) -> Result<(), LazyRefreshError> {
        let upstream = self
            .stores
            .upstreams
            .get_by_id(upstream_id)
            .await
            .map_err(lazy_error)?
            .ok_or_else(|| LazyRefreshError::Failed {
                reason: "oauth upstream not found".to_owned(),
            })?;
        let holder = lazy_refresh_holder(self.replica_id);
        let bundle = oauth_token_bundle(&upstream, &self.aead)?;
        if bundle.never_refresh {
            // Long-lived credentials are never refreshed; there is nothing to
            // enqueue.
            return Ok(());
        }
        check_refresh_allowed(&self.stores, &upstream, &bundle, &*self.clock)
            .await
            .map_err(lazy_refresh_error)?;
        let expires_at_unix_secs = bundle.expires_at_unix_secs;
        let claim = self
            .claim_guard
            .begin_refresh_for_generation(
                upstream_id,
                &holder,
                expires_at_unix_secs,
                upstream.oauth_token_generation,
                unix_secs(self.clock.now()),
            )
            .await
            .map_err(lazy_error)?;
        if let LazyRefreshClaim::Acquired = claim
            && let Err(release_error) = self
                .claim_guard
                .release_if_holder(upstream_id, &holder)
                .await
        {
            tracing::warn!(
                error = %release_error,
                upstream_id = %upstream_id,
                "soft refresh claim release failed",
            );
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RefreshError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("oauth upstream not found")]
    NotFound,
    #[error("missing oauth credentials")]
    MissingCredentials,
    #[error("oauth decrypt failed")]
    Decrypt,
    #[error("oauth encrypt failed")]
    Encrypt,
    #[error("oauth token endpoint returned {0}")]
    Status(StatusCode),
    #[error("oauth token endpoint returned {0}; reauthorization required")]
    ReconnectRequired(StatusCode),
    #[error("oauth refresh token expired; reauthorization required")]
    RefreshTokenExpired,
    #[error("oauth credential requires reauthorization")]
    ExistingReconnectRequired,
    #[error("oauth token endpoint request failed: {0}")]
    Http(String),
    #[error("oauth token response parse failed: {0}")]
    Parse(cc_lb_oauth_protocol::TokenEndpointParseError),
    #[error("oauth refresh cancelled")]
    Cancelled,
    #[error("oauth refresh ownership lost before commit")]
    LeaseLost,
    #[error("metadata hook failed: {0}")]
    MetadataHook(String),
    #[error(
        "oauth_client_not_configured: configure oauth.antigravity.client_id and oauth.antigravity.client_secret before refreshing Antigravity credentials"
    )]
    AntigravityClientNotConfigured,
    /// The credential's policy prohibits token refresh. This protects both
    /// access-only imports and Anthropic long-lived grants from rotation.
    #[error("long-lived oauth credential must not be refreshed; reauthorization required")]
    NotRefreshable,
}

pub(crate) fn oauth_refresh_http_status_proves_no_rotation(status: StatusCode) -> bool {
    status.is_client_error() && status != StatusCode::REQUEST_TIMEOUT
}

fn refresh_outcome_is_ambiguous_after_send(outcome: &Result<u64, RefreshError>) -> bool {
    match outcome {
        Err(
            RefreshError::Cancelled
            | RefreshError::Http(_)
            | RefreshError::Parse(_)
            | RefreshError::Encrypt
            | RefreshError::Storage(_)
            | RefreshError::LeaseLost
            | RefreshError::MetadataHook(_),
        ) => true,
        Err(RefreshError::Status(status) | RefreshError::ReconnectRequired(status)) => {
            !oauth_refresh_http_status_proves_no_rotation(*status)
        }
        _ => false,
    }
}

async fn check_refresh_allowed(
    stores: &Stores,
    upstream: &UpstreamRecord,
    bundle: &OAuthTokenBundle,
    clock: &dyn Clock,
) -> Result<(), RefreshError> {
    if bundle.never_refresh {
        return Err(RefreshError::NotRefreshable);
    }
    if refresh_requires_reconnect(upstream.last_apply_error.as_deref()) {
        return Err(RefreshError::ExistingReconnectRequired);
    }
    if bundle
        .refresh_token_expires_at_unix_secs
        .is_some_and(|expires_at| expires_at <= unix_secs(clock.now()))
    {
        record_refresh_failure(stores, upstream, "refresh_token_expired".to_owned()).await?;
        return Err(RefreshError::RefreshTokenExpired);
    }
    Ok(())
}

async fn record_refresh_failure(
    stores: &Stores,
    upstream: &UpstreamRecord,
    reason: String,
) -> StorageResult<()> {
    stores
        .upstreams
        .set_status(
            upstream.id,
            UpstreamStatusUpdate {
                last_apply_error: Some(Some(reason)),
                expected_oauth_token_generation: Some(upstream.oauth_token_generation),
                ..Default::default()
            },
        )
        .await
}

struct RefreshedGeneration {
    generation: u64,
    expires_at_unix_secs: Option<u64>,
}

#[allow(clippy::too_many_arguments)]
async fn refresh_flow(
    stores: &Stores,
    audit: Option<&dyn AuditStore>,
    aead: &AeadService,
    oauth_cfg: &AnthropicOAuthConfig,
    google_config: &cc_lb_config::Config,
    replica_id: Uuid,
    http: &TokenHttpClient,
    cancel: &CancellationToken,
    upstream: UpstreamRecord,
    clock: &dyn Clock,
    mut active_lease: Option<&mut ActiveOAuthRefreshLease>,
    expected_generation: Option<u64>,
) -> Result<RefreshedGeneration, RefreshError> {
    let current = stores
        .upstreams
        .get_by_id(upstream.id)
        .await?
        .ok_or(RefreshError::MissingCredentials)?;
    let prior_generation = expected_generation.unwrap_or(upstream.oauth_token_generation);
    if current.oauth_token_generation > prior_generation {
        return Ok(RefreshedGeneration {
            generation: current.oauth_token_generation,
            expires_at_unix_secs: None,
        });
    }
    if current.oauth_token_generation != prior_generation {
        return Err(RefreshError::LeaseLost);
    }
    let upstream = current;
    let previous = upstream
        .oauth_credentials
        .as_ref()
        .ok_or(RefreshError::MissingCredentials)?
        .decrypt(aead, upstream.id.as_bytes())
        .map_err(|_| RefreshError::Decrypt)?;
    check_refresh_allowed(stores, &upstream, &previous, clock).await?;
    let effective_cancel = active_lease
        .as_ref()
        .map_or_else(|| cancel.clone(), |lease| lease.cancel_token());
    if let Some(lease) = active_lease.as_deref_mut() {
        lease.mark_provider_exchange_started();
    }
    let token = request_refresh(
        http,
        oauth_cfg,
        google_config,
        upstream.kind,
        previous.oauth_profile,
        &effective_cancel,
        &previous.refresh_token,
    )
    .await;
    match token {
        Ok(response) => {
            let account_id = response.account_id.clone().or(previous.account_id.clone());
            let refreshed = refreshed_token_parts(
                ExistingTokenParts {
                    refresh_token: previous.refresh_token,
                    refresh_token_expires_at_unix_secs: previous.refresh_token_expires_at_unix_secs,
                    scopes: previous.scopes,
                },
                response,
                unix_secs(clock.now()),
            );
            let bundle = OAuthTokenBundle {
                access_token: refreshed.access_token,
                refresh_token: refreshed.refresh_token,
                expires_at_unix_secs: refreshed.expires_at_unix_secs,
                refresh_token_expires_at_unix_secs: refreshed.refresh_token_expires_at_unix_secs,
                scopes: refreshed.scopes,
                never_refresh: previous.never_refresh,
                account_id,
                project_id: previous.project_id,
                oauth_profile: previous.oauth_profile,
            };
            let fingerprint = access_token_fingerprint(&bundle.access_token);
            let expires_at = bundle.expires_at_unix_secs;
            let encrypted = EncryptedOAuthTokens::encrypt(aead, &bundle, upstream.id.as_bytes())
                .map_err(|_| RefreshError::Encrypt)?;
            let updated = if let Some(lease) = active_lease.as_deref_mut() {
                let fence = lease.fence().await.map_err(|()| RefreshError::LeaseLost)?;
                match fence {
                    Some(fence) => {
                        let updated = match stores
                            .upstreams
                            .complete_refresh_fenced(upstream.id, replica_id, encrypted, &fence)
                            .await
                        {
                            Ok(updated) => updated,
                            Err(StorageError::Conflict { .. }) => {
                                return Err(RefreshError::LeaseLost);
                            }
                            Err(error) => return Err(error.into()),
                        };
                        lease.mark_committed_and_released().await;
                        updated
                    }
                    None => {
                        stores
                            .upstreams
                            .complete_refresh(upstream.id, replica_id, encrypted)
                            .await?
                    }
                }
            } else {
                stores
                    .upstreams
                    .complete_refresh(upstream.id, replica_id, encrypted)
                    .await?
            };
            increment_metric(&upstream.name, "success");
            emit_audit(
                audit,
                AuditPayload::UpstreamOauthRefreshSuccess {
                    upstream_id: upstream.id.to_string(),
                    upstream_name: upstream.name.clone(),
                    expires_at_unix_secs: expires_at,
                    access_token_fingerprint: fingerprint,
                    replica_id,
                },
                &upstream,
                200,
                clock,
            )
            .await;
            Ok(RefreshedGeneration {
                generation: updated.oauth_token_generation,
                expires_at_unix_secs: Some(expires_at),
            })
        }
        Err(error) => {
            if let Some(lease) = active_lease.as_deref()
                && lease.is_lost().await
            {
                return Err(RefreshError::LeaseLost);
            }
            increment_metric(&upstream.name, "failure");
            let reason = reason_for(&error);
            let status_result = record_refresh_failure(stores, &upstream, reason.clone()).await;
            emit_audit(
                audit,
                AuditPayload::UpstreamOauthRefreshFailure {
                    upstream_id: upstream.id.to_string(),
                    upstream_name: upstream.name.clone(),
                    reason,
                    replica_id,
                },
                &upstream,
                500,
                clock,
            )
            .await;
            status_result?;
            Err(error)
        }
    }
}

async fn request_refresh(
    http: &TokenHttpClient,
    oauth_cfg: &AnthropicOAuthConfig,
    google_config: &cc_lb_config::Config,
    oauth_kind: UpstreamKind,
    oauth_profile: cc_lb_oauth_protocol::GeminiOAuthProfile,
    cancel: &CancellationToken,
    refresh_token: &str,
) -> Result<TokenEndpointResponse, RefreshError> {
    let antigravity = google_config.oauth.antigravity.clone().unwrap_or_default();
    let (token_url, content_type, body) = match oauth_kind {
        UpstreamKind::AnthropicOauth => (
            oauth_cfg.token_url.as_str(),
            "application/x-www-form-urlencoded",
            refresh_token_form_body(oauth_cfg.client_id.as_str(), refresh_token),
        ),
        UpstreamKind::OpenaiOauth => (
            OPENAI_OAUTH_TOKEN_URL,
            "application/json",
            refresh_token_json_body(OPENAI_CODEX_CLIENT_ID, refresh_token),
        ),
        UpstreamKind::GeminiOauth => {
            let (client_id, client_secret, token_url) = match oauth_profile {
                cc_lb_oauth_protocol::GeminiOAuthProfile::Antigravity => {
                    if antigravity.missing_client_credentials().is_some() {
                        return Err(RefreshError::AntigravityClientNotConfigured);
                    }
                    (
                        antigravity.client_id.as_str(),
                        antigravity.client_secret.as_str(),
                        antigravity.token_url.as_str(),
                    )
                }
                cc_lb_oauth_protocol::GeminiOAuthProfile::GeminiCli => (
                    GEMINI_OAUTH_CLIENT_ID,
                    GEMINI_OAUTH_CLIENT_SECRET,
                    GEMINI_OAUTH_TOKEN_URL,
                ),
            };
            (
                token_url,
                "application/x-www-form-urlencoded",
                cc_lb_oauth_protocol::gemini_refresh_token_form_body(
                    client_id,
                    client_secret,
                    refresh_token,
                ),
            )
        }
        _ => return Err(RefreshError::NotRefreshable),
    };
    let content_length = HeaderValue::from_str(&body.len().to_string())
        .map_err(|error| RefreshError::Http(error.to_string()))?;
    let request = Request::post(token_url)
        .header(CONTENT_TYPE, content_type)
        .header(CONTENT_LENGTH, content_length)
        .body(Full::new(Bytes::from(body)))
        .map_err(|error| RefreshError::Http(error.to_string()))?;

    let response = tokio::select! {
        _ = cancel.cancelled() => return Err(RefreshError::Cancelled),
        response = tokio::time::timeout(http.timeout, http.client.request(request)) => {
            response
                .map_err(|_| RefreshError::Http("request timed out".to_owned()))?
                .map_err(|error| RefreshError::Http(error.to_string()))?
        }
    };
    let status = response.status();
    if !status.is_success() && status != StatusCode::BAD_REQUEST {
        return Err(if is_terminal_refresh_failure(status.as_u16(), &[]) {
            RefreshError::ReconnectRequired(status)
        } else {
            RefreshError::Status(status)
        });
    }
    let bytes = tokio::select! {
        _ = cancel.cancelled() => return Err(RefreshError::Cancelled),
        bytes = tokio::time::timeout(http.timeout, response.into_body().collect()) => {
            bytes
                .map_err(|_| RefreshError::Http("response body timed out".to_owned()))?
                .map_err(|error| RefreshError::Http(error.to_string()))?
                .to_bytes()
        }
    };
    if !status.is_success() {
        return Err(if is_terminal_refresh_failure(status.as_u16(), &bytes) {
            RefreshError::ReconnectRequired(status)
        } else {
            RefreshError::Status(status)
        });
    }
    parse_token_endpoint_response(&bytes).map_err(RefreshError::Parse)
}

fn access_token_fingerprint(access_token: &str) -> String {
    let digest = Sha256::digest(access_token.as_bytes());
    to_hex(&digest[..4])
}

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn increment_metric(upstream: &str, outcome: &'static str) {
    metrics::counter!(
        "cclb_oauth_refresh_total",
        "upstream" => upstream.to_owned(),
        "outcome" => outcome
    )
    .increment(1);
}

async fn emit_audit(
    audit: Option<&dyn AuditStore>,
    payload: AuditPayload,
    upstream: &UpstreamRecord,
    status: u16,
    clock: &dyn Clock,
) {
    let Some(audit) = audit else {
        return;
    };
    let now = unix_secs(clock.now());
    let AuditActorFields {
        actor,
        authority,
        subject,
        kind,
        email,
    } = AuditActorFields::system("oauth_refresh");
    let entry = AuditEntry {
        ts: now,
        request_id: format!("oauth-refresh-{}-{now}", upstream.id),
        principal_id: String::new(),
        route: "oauth_refresh".to_owned(),
        upstream: upstream.id.to_string(),
        model: None,
        status,
        input_tokens: None,
        output_tokens: None,
        duration_ms: 0,
        agent_label: None,
        api_key_id: None,
        cost_usd_micros: None,
        limit_violation: None,
        admin_action: Some(payload.to_string()),
        actor: Some(actor),
        actor_authority: Some(authority),
        actor_subject: Some(subject),
        actor_kind: Some(kind),
        actor_email: email,
        payload: None,
    };
    if let Err(error) = audit.append_audit(&entry).await {
        tracing::warn!(error = %error, upstream_id = %upstream.id, "oauth refresh audit append failed");
    }
}

fn reason_for(error: &RefreshError) -> String {
    match error {
        RefreshError::ReconnectRequired(status) => format!("status_{}", status.as_u16()),
        RefreshError::Status(StatusCode::BAD_REQUEST) => "status_400_retryable".to_owned(),
        RefreshError::Status(status) => format!("status_{}", status.as_u16()),
        RefreshError::RefreshTokenExpired => "refresh_token_expired".to_owned(),
        RefreshError::ExistingReconnectRequired => "reauthorization_required".to_owned(),
        RefreshError::Http(_) => "network".to_owned(),
        RefreshError::Parse(_) => "parse".to_owned(),
        RefreshError::Cancelled => "cancelled".to_owned(),
        RefreshError::LeaseLost => "lease_lost".to_owned(),
        RefreshError::MetadataHook(_) => "metadata_hook".to_owned(),
        RefreshError::NotFound => "not_found".to_owned(),
        RefreshError::MissingCredentials => "missing_credentials".to_owned(),
        RefreshError::Decrypt => "decrypt".to_owned(),
        RefreshError::Encrypt => "encrypt".to_owned(),
        RefreshError::Storage(_) => "storage".to_owned(),
        RefreshError::AntigravityClientNotConfigured => "oauth_client_not_configured".to_owned(),
        // Unreachable in practice: `reason_for` is only called on errors from
        // `request_refresh`, and a long-lived credential is stopped by the
        // `refresh_one` guard long before that. The arm exists because the
        // variant does, and a catch-all would silently swallow future ones.
        RefreshError::NotRefreshable => "not_refreshable".to_owned(),
    }
}

fn lazy_refresh_error(error: RefreshError) -> LazyRefreshError {
    LazyRefreshError::Failed {
        reason: error.to_string(),
    }
}

fn lazy_error(error: StorageError) -> LazyRefreshError {
    LazyRefreshError::Failed {
        reason: error.to_string(),
    }
}

fn lazy_scheduler_error(error: cc_lb_scheduler::error::SchedulerError) -> LazyRefreshError {
    LazyRefreshError::Failed {
        reason: error.to_string(),
    }
}

fn lazy_metadata_hook_error(error: cc_lb_control::MetadataHookEnqueueError) -> LazyRefreshError {
    LazyRefreshError::Failed {
        reason: error.to_string(),
    }
}

fn oauth_token_bundle(
    upstream: &UpstreamRecord,
    aead: &AeadService,
) -> Result<OAuthTokenBundle, LazyRefreshError> {
    let credentials =
        upstream
            .oauth_credentials
            .as_ref()
            .ok_or_else(|| LazyRefreshError::Failed {
                reason: "missing oauth credentials".to_owned(),
            })?;
    credentials
        .decrypt(aead, upstream.id.as_bytes())
        .map_err(|_| LazyRefreshError::Failed {
            reason: "oauth decrypt failed".to_owned(),
        })
}

fn lazy_refresh_holder(replica_id: Uuid) -> String {
    format!("cc-lb-server:lazy-refresher:{replica_id}")
}

#[cfg(any(feature = "storage-sqlite", feature = "storage-postgres"))]
fn scheduler_claim_error(error: cc_lb_scheduler::error::SchedulerError) -> StorageError {
    StorageError::Unavailable {
        message: error.to_string(),
    }
}

#[cfg(all(test, feature = "storage-sqlite"))]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use axum::extract::State;
    use axum::routing::post;
    use axum::{Json, Router};
    use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
    use cc_lb_clock::{Clock, ClockHandle, TestClock};
    use cc_lb_config::AnthropicOAuthConfig;
    use cc_lb_signer_anthropic_oauth::LazyRefreshHandle;
    use cc_lb_storage_api::upstream::UpstreamKind;
    use cc_lb_storage_api::{MetaStore, StorageResult, UpstreamCreate, UpstreamStore};
    use tokio::net::TcpListener;
    use tokio::sync::{Notify, watch};
    use tokio_util::sync::CancellationToken;
    use url::Url;
    use uuid::Uuid;

    use super::{
        LazyRefreshClaim, LazyRefreshClaimGuard, LazyRefreshContentionConfig, LazyRefreshTaskState,
        LazyRefresher, LazyRefresherDeps, SchedulerBackend,
    };

    #[tokio::test]
    async fn lazy_refresher_single_flight_under_concurrency() {
        let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
        fixture.hold_refresh_response();
        let upstream_id = fixture.create_oauth_upstream().await;
        let starting_generation = fixture
            .storage
            .read_oauth_token_generation(upstream_id)
            .await
            .expect("starting generation read")
            .expect("starting generation exists");
        let claims = Arc::new(TestOAuthRefreshClaims::single_winner());
        let config = LazyRefreshContentionConfig::for_tests(
            Duration::from_secs(30),
            Duration::from_millis(5),
        );
        let first = fixture.lazy_refresher(claims.clone(), config);
        let second = fixture.lazy_refresher(claims.clone(), config);

        let first = tokio::spawn(async move { first.refresh_one(upstream_id).await });
        fixture.wait_for_refresh_start().await;
        let second = tokio::spawn(async move { second.refresh_one(upstream_id).await });
        claims.wait_for_contention().await;
        fixture.release_refresh_response();

        first
            .await
            .expect("first lazy refresh task joins")
            .expect("first lazy refresh succeeds");
        second
            .await
            .expect("second lazy refresh task joins")
            .expect("second lazy refresh joins");
        assert_eq!(fixture.refresh_call_count(), 1);
        assert_eq!(claims.completed_generation(), Some(starting_generation + 1),);
    }

    #[tokio::test]
    async fn lazy_refresher_falls_back_to_inproc_when_scheduler_wait_times_out() {
        let fixture = LazyRefreshFixture::new(Duration::from_millis(0)).await;
        let upstream_id = fixture.create_oauth_upstream().await;
        let claims = Arc::new(TestOAuthRefreshClaims::always_contended());
        let config = LazyRefreshContentionConfig::for_tests(
            Duration::from_millis(20),
            Duration::from_millis(5),
        );
        let refresher = fixture.lazy_refresher(claims, config);

        refresher
            .refresh_one(upstream_id)
            .await
            .expect("scheduler-backed wait times out and falls back to in-proc refresh");

        assert_eq!(fixture.refresh_call_count(), 1);
    }

    #[tokio::test]
    async fn lazy_refresher_completes_token_rotation_when_caller_is_cancelled()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
        fixture.hold_refresh_response();
        let upstream_id = fixture.create_oauth_upstream().await;
        let starting_generation = fixture
            .storage
            .read_oauth_token_generation(upstream_id)
            .await?
            .ok_or("missing starting generation")?;
        let claims = Arc::new(TestOAuthRefreshClaims::single_winner());
        let refresher = fixture.lazy_refresher(
            claims.clone(),
            LazyRefreshContentionConfig::for_tests(
                Duration::from_secs(30),
                Duration::from_millis(5),
            ),
        );

        let caller_task = tokio::spawn(async move { refresher.refresh_one(upstream_id).await });
        fixture.wait_for_refresh_start().await;
        let mut completion = claims.completion.subscribe();
        caller_task.abort();
        let _ = caller_task.await;

        fixture.release_refresh_response();
        let completed = tokio::time::timeout(
            Duration::from_secs(5),
            completion.wait_for(|generation| generation.is_some()),
        )
        .await
        .expect("detached refresh completes within 5s")
        .expect("completion watch stays open");
        assert_eq!(*completed, Some(starting_generation + 1));
        assert_eq!(
            fixture
                .storage
                .read_oauth_token_generation(upstream_id)
                .await?
                .ok_or("missing rotated generation")?,
            starting_generation + 1
        );
        assert_eq!(fixture.refresh_call_count(), 1);
        assert_eq!(claims.completed_generation(), Some(starting_generation + 1));
        let stored = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await?
            .ok_or("missing rotated upstream")?;
        let decrypted = stored
            .oauth_credentials
            .as_ref()
            .ok_or("missing rotated credentials")?
            .decrypt(&fixture.aead, upstream_id.as_bytes())?;
        assert_eq!(decrypted.refresh_token, "sk-ant-ort01-new");
        Ok(())
    }

    #[tokio::test]
    async fn lazy_refresher_second_caller_joins_detached_in_process_fallback_after_first_times_out()
    -> Result<(), Box<dyn std::error::Error>> {
        #[derive(Debug)]
        struct DoneWithoutBumpClaims;

        #[async_trait::async_trait]
        impl LazyRefreshClaimGuard for DoneWithoutBumpClaims {
            async fn begin_refresh(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
                _expires_at_unix_secs: u64,
                _now_unix_secs: u64,
            ) -> StorageResult<LazyRefreshClaim> {
                Ok(LazyRefreshClaim::Enqueued {
                    idempotency_key: "done:no-bump".to_owned(),
                })
            }

            async fn complete_and_bump_generation(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
                _generation: u64,
            ) -> StorageResult<bool> {
                Ok(true)
            }

            async fn release_if_holder(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
            ) -> StorageResult<bool> {
                Ok(true)
            }

            async fn task_state(
                &self,
                _idempotency_key: &str,
            ) -> StorageResult<LazyRefreshTaskState> {
                Ok(LazyRefreshTaskState::Done)
            }

            async fn try_claim_fallback(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
                _idempotency_key: &str,
                _generation: u64,
                _now_unix_secs: u64,
            ) -> StorageResult<Option<super::LazyRefreshFallbackLease>> {
                Ok(Some(super::LazyRefreshFallbackLease::from_token(
                    "done-without-bump".to_owned(),
                )))
            }
        }

        let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
        fixture.hold_refresh_response();
        let upstream_id = fixture.create_oauth_upstream().await;
        let starting_generation = fixture
            .storage
            .read_oauth_token_generation(upstream_id)
            .await?
            .ok_or("missing starting generation")?;
        let claims = Arc::new(DoneWithoutBumpClaims);
        let config = LazyRefreshContentionConfig::for_tests(
            Duration::from_secs(5),
            Duration::from_millis(5),
        );
        let refresher1 = fixture.lazy_refresher(claims.clone(), config);
        let refresher2 = refresher1.clone();

        let caller1 = tokio::spawn(async move { refresher1.refresh_one(upstream_id).await });
        fixture.wait_for_refresh_start().await;
        // Abort Caller 1 (simulating WebSocket connect timeout) while its detached in-process
        // exchange is still holding at `/oauth/token`.
        caller1.abort();
        let _ = caller1.await;
        assert_eq!(fixture.refresh_call_count(), 1);

        // Caller 2 arrives for the same upstream: synchronously polling `caller2` once via
        // `select! { biased; ... }` proves it joins Caller 1's shared in-flight watch channel
        // rather than launching a second `/oauth/token` exchange.
        let mut caller2 = Box::pin(refresher2.refresh_one(upstream_id));
        tokio::select! {
            biased;
            res = &mut caller2 => panic!("caller2 unexpectedly completed before token endpoint released: {res:?}"),
            _ = std::future::ready(()) => {}
        }
        assert_eq!(fixture.refresh_call_count(), 1);

        fixture.release_refresh_response();
        caller2.await?;

        assert_eq!(
            fixture.refresh_call_count(),
            1,
            "second caller must share detached in-process refresh instead of exchanging rotating token twice"
        );
        assert_eq!(
            fixture
                .storage
                .read_oauth_token_generation(upstream_id)
                .await?
                .ok_or("missing rotated generation")?,
            starting_generation + 1
        );
        Ok(())
    }

    #[tokio::test]
    async fn lazy_refresher_refuses_in_process_fallback_when_scheduled_worker_is_running()
    -> Result<(), Box<dyn std::error::Error>> {
        // Part A: Mock claim guard simulating an active Running scheduled worker.
        let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
        let upstream_id = fixture.create_oauth_upstream().await;
        let starting_generation = fixture
            .storage
            .read_oauth_token_generation(upstream_id)
            .await?
            .ok_or("missing starting generation")?;
        let claims = Arc::new(TestOAuthRefreshClaims::active_worker_in_progress());
        let config = LazyRefreshContentionConfig::for_tests(
            Duration::from_millis(15),
            Duration::from_millis(5),
        );
        let refresher = fixture.lazy_refresher(claims, config);

        let error = refresher
            .refresh_one(upstream_id)
            .await
            .expect_err("timeout while scheduled worker holds active ownership must fail closed");
        assert!(matches!(
            error,
            crate::refresh::LazyRefreshError::Failed { .. }
        ));
        assert_eq!(
            fixture.refresh_call_count(),
            0,
            "must never invoke /oauth/token while scheduled worker owns the job"
        );

        // Part B: Real SQLite `ApalisLazyRefreshClaimGuard` + `MetaStore` credential lease.
        let guard = Arc::new(super::ApalisLazyRefreshClaimGuard::new(
            fixture.scheduler_backend.clone(),
            fixture.storage.clone(),
        ));
        let job = cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob::new(upstream_id);
        let now = cc_lb_clock::unix_secs(fixture.clock.now());
        let bootstrap_key = cc_lb_scheduler::jobs::watchdog::WatchdogEntityKind::OAuthRefresh
            .bootstrap_key(upstream_id, now);
        let real_refresher = fixture.lazy_refresher(
            guard,
            LazyRefreshContentionConfig::for_tests(
                Duration::from_millis(15),
                Duration::from_millis(5),
            ),
        );
        sqlx::query(
            "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen, started_at) \
             VALUES ('worker-active', 'adaptive', 'default', '', ?1, ?1)",
        )
        .bind(i64::try_from(now)?)
        .execute(&fixture.scheduler_pool)
        .await?;
        fixture
            .scheduler_backend
            .push_or_expedite_adaptive_task(cc_lb_scheduler::worker::SchedulerPushTask {
                args: cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(job.clone()),
                idempotency_key: Some(bootstrap_key.clone()),
                run_at_unix_secs: Some(now),
                max_attempts: None,
            })
            .await?;
        sqlx::query(
            "UPDATE Jobs SET status = 'Running', lock_by = 'worker-active', lock_at = ?1 WHERE idempotency_key = ?2",
        )
        .bind(i64::try_from(now)?)
        .bind(&bootstrap_key)
        .execute(&fixture.scheduler_pool)
        .await?;

        // Simulate a live scheduled worker holding the shared credential lease
        // across job keys (e.g., watchdog bootstrap while expiry-key stays Pending).
        let worker_lease = super::try_acquire_oauth_refresh_lease(
            fixture.storage.as_ref(),
            upstream_id,
            starting_generation,
            now,
        )
        .await?
        .ok_or("expected initial worker lease acquisition")?;

        real_refresher
            .refresh_one(upstream_id)
            .await
            .expect_err("live credential lease must block in-process fallback");
        assert_eq!(fixture.refresh_call_count(), 0);
        assert_eq!(
            fixture
                .storage
                .read_oauth_token_generation(upstream_id)
                .await?
                .ok_or("missing generation after held lease skip")?,
            starting_generation
        );

        // Once the worker releases the lease without rotating (e.g., adaptive worker paused),
        // the proxy acquires exclusive fallback ownership, rotates, releases the lease,
        // and schedules the next proactive OAuthRefreshJob.
        super::release_oauth_refresh_lease(
            fixture.storage.as_ref(),
            upstream_id,
            &worker_lease.token,
            starting_generation,
        )
        .await?;
        real_refresher.refresh_one(upstream_id).await?;
        assert_eq!(fixture.refresh_call_count(), 1);
        let new_generation = fixture
            .storage
            .read_oauth_token_generation(upstream_id)
            .await?
            .ok_or("missing rotated generation after fallback")?;
        assert_eq!(new_generation, starting_generation + 1);
        let lease_after = fixture
            .storage
            .get_meta_value(&super::oauth_refresh_lease_meta_key(upstream_id))
            .await?;
        assert_eq!(
            lease_after.as_deref(),
            Some(format!("idle:{new_generation}").as_str())
        );
        let next_proactive_key = job.generation_idempotency_key(new_generation, now + 3600);
        let pending_tasks = fixture
            .scheduler_backend
            .list_adaptive_tasks(&cc_lb_scheduler::worker::Filter {
                status: Some(cc_lb_scheduler::worker::TaskStatus::Pending),
                page: 1,
                page_size: Some(50),
            })
            .await?;
        let successor = pending_tasks
            .iter()
            .find(|task| task.idempotency_key.as_deref() == Some(next_proactive_key.as_str()))
            .ok_or("expected pending successor oauth refresh job")?;
        let cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(successor_job) = &successor.args
        else {
            panic!("expected AdaptiveJob::OAuthRefresh successor");
        };
        assert_eq!(successor_job.expected_generation, Some(new_generation));
        assert_eq!(
            successor_job.expected_expires_at_unix_secs,
            Some(now + 3600)
        );
        Ok(())
    }

    #[tokio::test]
    async fn lazy_refresher_recovers_after_killed_job_and_expired_lease()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
        let upstream_id = fixture.create_oauth_upstream().await;
        let starting_generation = fixture
            .storage
            .read_oauth_token_generation(upstream_id)
            .await?
            .ok_or("missing starting generation")?;
        let now = cc_lb_clock::unix_secs(fixture.clock.now());
        let job = cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob::for_credential(
            upstream_id,
            starting_generation,
            1,
        );
        let expiry_key = job.generation_idempotency_key(starting_generation, 1);
        fixture
            .scheduler_backend
            .push_or_expedite_adaptive_task(cc_lb_scheduler::worker::SchedulerPushTask {
                args: cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(job.clone()),
                idempotency_key: Some(expiry_key.clone()),
                run_at_unix_secs: Some(now),
                max_attempts: None,
            })
            .await?;
        sqlx::query("UPDATE Jobs SET status = 'Killed', done_at = ?1 WHERE idempotency_key = ?2")
            .bind(i64::try_from(now)?)
            .bind(&expiry_key)
            .execute(&fixture.scheduler_pool)
            .await?;

        // Seed an expired credential lease left behind by a crashed holder.
        let lease_key = super::oauth_refresh_lease_meta_key(upstream_id);
        fixture
            .storage
            .put_meta_value(
                &lease_key,
                &format!("held:{starting_generation}:dead-holder:{}", now - 1),
            )
            .await?;

        let guard = Arc::new(super::ApalisLazyRefreshClaimGuard::new(
            fixture.scheduler_backend.clone(),
            fixture.storage.clone(),
        ));
        let refresher = fixture.lazy_refresher(
            guard,
            LazyRefreshContentionConfig::for_tests(
                Duration::from_millis(15),
                Duration::from_millis(5),
            ),
        );

        refresher.refresh_one(upstream_id).await?;
        assert_eq!(fixture.refresh_call_count(), 1);
        let new_generation = fixture
            .storage
            .read_oauth_token_generation(upstream_id)
            .await?
            .ok_or("missing rotated generation after killed-job recovery")?;
        assert_eq!(new_generation, starting_generation + 1);
        assert_eq!(
            fixture.storage.get_meta_value(&lease_key).await?.as_deref(),
            Some(format!("idle:{new_generation}").as_str())
        );
        let next_proactive_key = job.generation_idempotency_key(new_generation, now + 3600);
        let pending_tasks = fixture
            .scheduler_backend
            .list_adaptive_tasks(&cc_lb_scheduler::worker::Filter {
                status: Some(cc_lb_scheduler::worker::TaskStatus::Pending),
                page: 1,
                page_size: Some(50),
            })
            .await?;
        let successor = pending_tasks
            .iter()
            .find(|task| task.idempotency_key.as_deref() == Some(next_proactive_key.as_str()))
            .ok_or("expected pending successor oauth refresh job after killed recovery")?;
        let cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(successor_job) = &successor.args
        else {
            panic!("expected AdaptiveJob::OAuthRefresh successor");
        };
        assert_eq!(successor_job.expected_generation, Some(new_generation));
        assert_eq!(
            successor_job.expected_expires_at_unix_secs,
            Some(now + 3600)
        );
        Ok(())
    }

    #[tokio::test]
    async fn oauth_refresh_lease_rejects_stale_generation_after_idle_advance()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
        let upstream_id = fixture.create_oauth_upstream().await;
        let starting_generation = fixture
            .storage
            .read_oauth_token_generation(upstream_id)
            .await?
            .ok_or("missing starting generation")?;
        let lease_key = super::oauth_refresh_lease_meta_key(upstream_id);
        let now = cc_lb_clock::unix_secs(fixture.clock.now());

        let idle_next = format!("idle:{}", starting_generation + 1);
        fixture
            .storage
            .put_meta_value(&lease_key, &idle_next)
            .await?;

        let stale = super::try_acquire_oauth_refresh_lease(
            fixture.storage.as_ref(),
            upstream_id,
            starting_generation,
            now,
        )
        .await?;
        assert!(
            stale.is_none(),
            "stale generation N must not acquire lease when idle:(N+1) is recorded"
        );
        assert_eq!(
            fixture.storage.get_meta_value(&lease_key).await?.as_deref(),
            Some(idle_next.as_str())
        );

        let current = super::try_acquire_oauth_refresh_lease(
            fixture.storage.as_ref(),
            upstream_id,
            starting_generation + 1,
            now,
        )
        .await?;
        assert!(
            current.is_some(),
            "current generation N+1 must still be allowed to acquire after idle:(N+1)"
        );
        Ok(())
    }

    #[tokio::test]
    async fn oauth_refresh_lease_heartbeat_renews_and_fences_lost_ownership()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
        let upstream_id = fixture.create_oauth_upstream().await;
        let starting_generation = fixture
            .storage
            .read_oauth_token_generation(upstream_id)
            .await?
            .ok_or("missing starting generation")?;
        let lease_key = super::oauth_refresh_lease_meta_key(upstream_id);
        let test_clock = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let clock_handle: ClockHandle = test_clock.clone();
        let initial_now = cc_lb_clock::unix_secs(test_clock.now());

        // Phase 1: Heartbeat extends lease past original 90s TTL and atomic fenced commit
        // succeeds and releases cleanly to `idle:(N+1)`.
        let initial_lease = super::try_acquire_oauth_refresh_lease(
            fixture.storage.as_ref(),
            upstream_id,
            starting_generation,
            initial_now,
        )
        .await?
        .ok_or("initial lease should succeed")?;
        let parent_cancel = CancellationToken::new();
        let mut active = super::ActiveOAuthRefreshLease::start_with_interval(
            fixture.storage.clone(),
            clock_handle.clone(),
            parent_cancel.clone(),
            initial_lease.clone(),
            starting_generation,
            Duration::from_millis(5),
        );

        test_clock.advance_secs(40);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let current_val = fixture.storage.get_meta_value(&lease_key).await?;
            if current_val.as_deref() != Some(initial_lease.token.as_str()) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "heartbeat did not renew lease before deadline"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        // At T = initial_now + 100 (past initial +90s TTL, before renewed +130s TTL),
        // another acquirer must still see the renewed lease as held.
        assert!(
            super::try_acquire_oauth_refresh_lease(
                fixture.storage.as_ref(),
                upstream_id,
                starting_generation,
                initial_now + 100,
            )
            .await?
            .is_none(),
            "renewed lease must remain exclusive past original 90s TTL"
        );

        let fence = active
            .fence()
            .await
            .map_err(|()| "expected active fence")?
            .ok_or("expected MetaStore fence")?;
        let refreshed_tokens = EncryptedOAuthTokens::encrypt(
            fixture.aead.as_ref(),
            &OAuthTokenBundle {
                oauth_profile: Default::default(),
                access_token: "sk-ant-oat01-fenced-1".to_owned(),
                refresh_token: "sk-ant-ort01-fenced-1".to_owned(),
                expires_at_unix_secs: initial_now + 3600,
                refresh_token_expires_at_unix_secs: None,
                account_id: None,
                project_id: None,
                scopes: vec!["messages".to_owned()],
                never_refresh: false,
            },
            upstream_id.as_bytes(),
        )?;
        let committed = fixture
            .storage
            .complete_refresh_fenced(upstream_id, Uuid::new_v4(), refreshed_tokens, &fence)
            .await?;
        active.mark_committed_and_released().await;
        let gen2 = committed.oauth_token_generation;
        assert_eq!(gen2, starting_generation + 1);
        assert_eq!(
            fixture.storage.get_meta_value(&lease_key).await?.as_deref(),
            Some(format!("idle:{gen2}").as_str())
        );

        // Phase 2: Losing the lease cancels the request token and rejects complete_refresh_fenced.
        let now2 = cc_lb_clock::unix_secs(test_clock.now());
        let lease2 = super::try_acquire_oauth_refresh_lease(
            fixture.storage.as_ref(),
            upstream_id,
            gen2,
            now2,
        )
        .await?
        .ok_or("second lease should succeed")?;
        let active2 = super::ActiveOAuthRefreshLease::start_with_interval(
            fixture.storage.clone(),
            clock_handle.clone(),
            parent_cancel.clone(),
            lease2.clone(),
            gen2,
            Duration::from_millis(5),
        );
        let request_cancel = active2.cancel_token();
        let competing_holder = format!("held:{gen2}:competitor:{}", now2 + 90);
        fixture
            .storage
            .put_meta_value(&lease_key, &competing_holder)
            .await?;

        tokio::time::timeout(Duration::from_secs(2), request_cancel.cancelled())
            .await
            .map_err(|_| "lease loss did not cancel active request token")?;
        assert!(active2.is_lost().await);
        assert!(active2.fence().await.is_err());

        let stale_fence = cc_lb_storage_api::OAuthRefreshFence {
            expected_generation: gen2,
            lease_meta_key: lease_key.clone(),
            expected_lease_prefix: super::oauth_refresh_lease_fence_prefix(&lease2.token),
        };
        let lost_tokens = EncryptedOAuthTokens::encrypt(
            fixture.aead.as_ref(),
            &OAuthTokenBundle {
                oauth_profile: Default::default(),
                access_token: "sk-ant-oat01-lost".to_owned(),
                refresh_token: "sk-ant-ort01-lost".to_owned(),
                expires_at_unix_secs: now2 + 7200,
                refresh_token_expires_at_unix_secs: None,
                account_id: None,
                project_id: None,
                scopes: vec!["messages".to_owned()],
                never_refresh: false,
            },
            upstream_id.as_bytes(),
        )?;
        let conflict = fixture
            .storage
            .complete_refresh_fenced(upstream_id, Uuid::new_v4(), lost_tokens, &stale_fence)
            .await
            .expect_err("lost lease fence must fail closed with Conflict");
        assert!(matches!(
            conflict,
            cc_lb_storage_api::StorageError::Conflict { .. }
        ));
        assert_eq!(
            fixture
                .storage
                .read_oauth_token_generation(upstream_id)
                .await?,
            Some(gen2)
        );
        assert_eq!(
            fixture.storage.get_meta_value(&lease_key).await?.as_deref(),
            Some(competing_holder.as_str())
        );

        // Phase 3: Empty fence prefix fails closed, and prefix-matched fence stays valid
        // across renewals occurring between fence creation and storage commit.
        let empty_prefix_fence = cc_lb_storage_api::OAuthRefreshFence {
            expected_generation: gen2,
            lease_meta_key: lease_key.clone(),
            expected_lease_prefix: String::new(),
        };
        let empty_tokens = EncryptedOAuthTokens::encrypt(
            fixture.aead.as_ref(),
            &OAuthTokenBundle {
                oauth_profile: Default::default(),
                access_token: "sk-ant-oat01-empty".to_owned(),
                refresh_token: "sk-ant-ort01-empty".to_owned(),
                expires_at_unix_secs: now2 + 7200,
                refresh_token_expires_at_unix_secs: None,
                account_id: None,
                project_id: None,
                scopes: vec!["messages".to_owned()],
                never_refresh: false,
            },
            upstream_id.as_bytes(),
        )?;
        assert!(matches!(
            fixture
                .storage
                .complete_refresh_fenced(
                    upstream_id,
                    Uuid::new_v4(),
                    empty_tokens,
                    &empty_prefix_fence
                )
                .await
                .expect_err("empty fence prefix must fail closed"),
            cc_lb_storage_api::StorageError::Conflict { .. }
        ));

        fixture
            .storage
            .put_meta_value(&lease_key, &format!("idle:{gen2}"))
            .await?;
        let now3 = cc_lb_clock::unix_secs(test_clock.now());
        let lease3 = super::try_acquire_oauth_refresh_lease(
            fixture.storage.as_ref(),
            upstream_id,
            gen2,
            now3,
        )
        .await?
        .ok_or("third lease should succeed")?;
        let mut active3 = super::ActiveOAuthRefreshLease::start_with_interval(
            fixture.storage.clone(),
            clock_handle,
            parent_cancel,
            lease3.clone(),
            gen2,
            Duration::from_millis(5),
        );
        // Obtain the fence BEFORE heartbeat renews the token timestamp.
        let pre_renew_fence = active3
            .fence()
            .await
            .map_err(|()| "expected active pre-renewal fence")?
            .ok_or("expected MetaStore fence")?;
        test_clock.advance_secs(40);
        let renew_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(current_val) = fixture.storage.get_meta_value(&lease_key).await?
                && current_val != lease3.token
            {
                assert!(current_val.starts_with(&pre_renew_fence.expected_lease_prefix));
                break;
            }
            assert!(
                tokio::time::Instant::now() < renew_deadline,
                "lease did not keep renewing after fence() call"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        // Another acquirer at now3 + 100 still sees the renewed lease as held.
        assert!(
            super::try_acquire_oauth_refresh_lease(
                fixture.storage.as_ref(),
                upstream_id,
                gen2,
                now3 + 100,
            )
            .await?
            .is_none()
        );
        let commit3_tokens = EncryptedOAuthTokens::encrypt(
            fixture.aead.as_ref(),
            &OAuthTokenBundle {
                oauth_profile: Default::default(),
                access_token: "sk-ant-oat01-fenced-renewed".to_owned(),
                refresh_token: "sk-ant-ort01-fenced-renewed".to_owned(),
                expires_at_unix_secs: now3 + 7200,
                refresh_token_expires_at_unix_secs: None,
                account_id: None,
                project_id: None,
                scopes: vec!["messages".to_owned()],
                never_refresh: false,
            },
            upstream_id.as_bytes(),
        )?;
        let committed3 = fixture
            .storage
            .complete_refresh_fenced(
                upstream_id,
                Uuid::new_v4(),
                commit3_tokens,
                &pre_renew_fence,
            )
            .await?;
        active3.mark_committed_and_released().await;
        assert_eq!(committed3.oauth_token_generation, gen2 + 1);
        assert_eq!(
            fixture.storage.get_meta_value(&lease_key).await?.as_deref(),
            Some(format!("idle:{}", gen2 + 1).as_str())
        );

        // Prefix-aware release clears a renewed timestamp for the same holder,
        // while leaving a competitor holder untouched.
        let lease4 = super::try_acquire_oauth_refresh_lease(
            fixture.storage.as_ref(),
            upstream_id,
            gen2 + 1,
            now3 + 200,
        )
        .await?
        .ok_or("fourth lease should succeed")?;
        let renewed4 = super::renew_oauth_refresh_lease(
            fixture.storage.as_ref(),
            upstream_id,
            &lease4.token,
            now3 + 230,
        )
        .await?
        .ok_or("renew4 should succeed")?;
        assert_ne!(renewed4, lease4.token);
        super::release_oauth_refresh_lease(
            fixture.storage.as_ref(),
            upstream_id,
            &lease4.token,
            gen2 + 1,
        )
        .await?;
        assert_eq!(
            fixture.storage.get_meta_value(&lease_key).await?.as_deref(),
            Some(format!("idle:{}", gen2 + 1).as_str())
        );
        let competitor4 = format!("held:{}:other:{}", gen2 + 1, now3 + 300);
        fixture
            .storage
            .put_meta_value(&lease_key, &competitor4)
            .await?;
        super::release_oauth_refresh_lease(
            fixture.storage.as_ref(),
            upstream_id,
            &lease4.token,
            gen2 + 99,
        )
        .await?;
        assert_eq!(
            fixture.storage.get_meta_value(&lease_key).await?.as_deref(),
            Some(competitor4.as_str())
        );
        Ok(())
    }

    #[tokio::test]
    async fn oauth_refresh_lease_cancels_at_expiry_deadline_despite_transient_renewal_errors()
    -> Result<(), Box<dyn std::error::Error>> {
        struct ErrorRenewController {
            calls: Arc<AtomicUsize>,
        }

        #[async_trait::async_trait]
        impl super::OAuthLeaseController for ErrorRenewController {
            async fn renew(
                &self,
                _upstream_id: Uuid,
                _current_token: &str,
                _now_unix_secs: u64,
            ) -> StorageResult<Option<String>> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Err(cc_lb_storage_api::StorageError::Unavailable {
                    message: "transient storage timeout".to_owned(),
                })
            }

            async fn release(
                &self,
                _upstream_id: Uuid,
                _lease_token: &str,
                _final_generation: u64,
            ) -> StorageResult<()> {
                Ok(())
            }

            fn meta_key(&self, upstream_id: Uuid) -> Option<String> {
                Some(super::oauth_refresh_lease_meta_key(upstream_id))
            }
        }

        struct HungRenewController;

        #[async_trait::async_trait]
        impl super::OAuthLeaseController for HungRenewController {
            async fn renew(
                &self,
                _upstream_id: Uuid,
                _current_token: &str,
                _now_unix_secs: u64,
            ) -> StorageResult<Option<String>> {
                std::future::pending::<()>().await;
                Ok(None)
            }

            async fn release(
                &self,
                _upstream_id: Uuid,
                _lease_token: &str,
                _final_generation: u64,
            ) -> StorageResult<()> {
                Ok(())
            }

            fn meta_key(&self, upstream_id: Uuid) -> Option<String> {
                Some(super::oauth_refresh_lease_meta_key(upstream_id))
            }
        }

        let upstream_id = Uuid::new_v4();
        let clock = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let now = cc_lb_clock::unix_secs(clock.now());
        let token = format!("held:1:{}:{}", Uuid::new_v4(), now + 90);
        let parent_cancel = CancellationToken::new();

        // Case 1: Repeated transient storage errors never extend the expiry deadline,
        // so the exchange cancels strictly at `effective_ttl`.
        let calls = Arc::new(AtomicUsize::new(0));
        let active_err = super::ActiveOAuthRefreshLease::start_with_controller_for_tests(
            Arc::new(ErrorRenewController {
                calls: Arc::clone(&calls),
            }),
            clock.clone(),
            parent_cancel.clone(),
            upstream_id,
            token.clone(),
            1,
            Duration::from_millis(10),
            Duration::from_millis(55),
            Duration::ZERO,
        );
        tokio::time::timeout(
            Duration::from_secs(2),
            active_err.cancel_token().cancelled(),
        )
        .await
        .map_err(|_| "expiry deadline did not cancel on repeated transient errors")?;
        assert!(active_err.is_lost().await);
        assert!(active_err.fence().await.is_err());
        assert!(calls.load(Ordering::SeqCst) >= 1);

        // Case 2: A slow/hung storage renew call is itself bounded by the expiry deadline.
        let active_hung = super::ActiveOAuthRefreshLease::start_with_controller_for_tests(
            Arc::new(HungRenewController),
            clock,
            parent_cancel,
            upstream_id,
            token,
            1,
            Duration::from_millis(5),
            Duration::from_millis(45),
            Duration::ZERO,
        );
        tokio::time::timeout(
            Duration::from_secs(2),
            active_hung.cancel_token().cancelled(),
        )
        .await
        .map_err(|_| "expiry deadline did not cancel while renew was hung")?;
        assert!(active_hung.is_lost().await);
        assert!(active_hung.fence().await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn lazy_refresher_expedites_generation_keyed_reconnect_job_without_legacy_collision()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
        let upstream_id = fixture.create_oauth_upstream().await;
        let now = cc_lb_clock::unix_secs(fixture.clock.now());
        let future_expiry = now + 3_600;

        // Reconnect updates token to generation 2 at `future_expiry`.
        let record = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await?
            .ok_or("missing upstream")?;
        let rotated_tokens = EncryptedOAuthTokens::encrypt(
            fixture.aead.as_ref(),
            &OAuthTokenBundle {
                oauth_profile: Default::default(),
                access_token: "sk-ant-oat01-reconnected".to_owned(),
                refresh_token: "sk-ant-ort01-reconnected".to_owned(),
                expires_at_unix_secs: future_expiry,
                refresh_token_expires_at_unix_secs: None,
                account_id: None,
                project_id: None,
                scopes: vec!["messages".to_owned()],
                never_refresh: false,
            },
            upstream_id.as_bytes(),
        )?;
        let reconnected = fixture
            .storage
            .store_oauth_tokens(upstream_id, record.revision, rotated_tokens, false)
            .await?;
        let gen2 = reconnected.oauth_token_generation;

        // An obsolete generation-1 Done job occupies the legacy unsuffixed expiry key,
        // while reconnect queued the generation-2 job at the canonical generation-keyed slot
        // with future `run_at`.
        let legacy_job = cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob::new(upstream_id);
        let legacy_key = legacy_job.idempotency_key(future_expiry);
        fixture
            .scheduler_backend
            .push_or_expedite_adaptive_task(cc_lb_scheduler::worker::SchedulerPushTask {
                args: cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(legacy_job),
                idempotency_key: Some(legacy_key.clone()),
                run_at_unix_secs: Some(now),
                max_attempts: None,
            })
            .await?;
        sqlx::query("UPDATE Jobs SET status = 'Done', done_at = ?1 WHERE idempotency_key = ?2")
            .bind(i64::try_from(now)?)
            .bind(&legacy_key)
            .execute(&fixture.scheduler_pool)
            .await?;

        let current_job = cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob::for_credential(
            upstream_id,
            gen2,
            future_expiry,
        );
        let current_key = current_job.generation_idempotency_key(gen2, future_expiry);
        fixture
            .scheduler_backend
            .push_or_expedite_adaptive_task(cc_lb_scheduler::worker::SchedulerPushTask {
                args: cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(current_job),
                idempotency_key: Some(current_key.clone()),
                run_at_unix_secs: Some(future_expiry),
                max_attempts: None,
            })
            .await?;

        let guard = Arc::new(super::ApalisLazyRefreshClaimGuard::new(
            fixture.scheduler_backend.clone(),
            fixture.storage.clone(),
        ));
        let refresher = fixture.lazy_refresher(
            guard,
            LazyRefreshContentionConfig::for_tests(
                Duration::from_millis(15),
                Duration::from_millis(5),
            ),
        );

        refresher.enqueue_only(upstream_id).await?;
        let run_at: i64 = sqlx::query_scalar("SELECT run_at FROM Jobs WHERE idempotency_key = ?1")
            .bind(&current_key)
            .fetch_one(&fixture.scheduler_pool)
            .await?;
        assert!(
            u64::try_from(run_at)? <= now,
            "enqueue_only must expedite the canonical generation-2 job rather than colliding on legacy Done key"
        );
        Ok(())
    }

    #[tokio::test]
    async fn lazy_refresher_joins_and_expedites_watchdog_recovery_from_killed_canonical_row()
    -> Result<(), Box<dyn std::error::Error>> {
        use tokio::sync::watch;

        struct ObservingRecoveryClaims {
            inner: super::ApalisLazyRefreshClaimGuard,
            active_count: watch::Sender<usize>,
            fallback_none_count: watch::Sender<usize>,
            seen_task_keys: Arc<std::sync::Mutex<Vec<(String, super::LazyRefreshTaskState)>>>,
        }

        #[async_trait::async_trait]
        impl super::LazyRefreshClaimGuard for ObservingRecoveryClaims {
            async fn begin_refresh(
                &self,
                upstream_id: Uuid,
                holder: &str,
                expires_at_unix_secs: u64,
                now_unix_secs: u64,
            ) -> StorageResult<super::LazyRefreshClaim> {
                self.inner
                    .begin_refresh(upstream_id, holder, expires_at_unix_secs, now_unix_secs)
                    .await
            }

            async fn begin_refresh_for_generation(
                &self,
                upstream_id: Uuid,
                holder: &str,
                expires_at_unix_secs: u64,
                expected_generation: u64,
                now_unix_secs: u64,
            ) -> StorageResult<super::LazyRefreshClaim> {
                self.inner
                    .begin_refresh_for_generation(
                        upstream_id,
                        holder,
                        expires_at_unix_secs,
                        expected_generation,
                        now_unix_secs,
                    )
                    .await
            }

            async fn complete_and_bump_generation(
                &self,
                upstream_id: Uuid,
                holder: &str,
                generation: u64,
            ) -> StorageResult<bool> {
                self.inner
                    .complete_and_bump_generation(upstream_id, holder, generation)
                    .await
            }

            async fn release_if_holder(
                &self,
                upstream_id: Uuid,
                holder: &str,
            ) -> StorageResult<bool> {
                self.inner.release_if_holder(upstream_id, holder).await
            }

            async fn task_state(
                &self,
                idempotency_key: &str,
            ) -> StorageResult<super::LazyRefreshTaskState> {
                let state = self.inner.task_state(idempotency_key).await?;
                self.seen_task_keys
                    .lock()
                    .expect("seen_task_keys lock")
                    .push((idempotency_key.to_owned(), state.clone()));
                if matches!(state, super::LazyRefreshTaskState::Active) {
                    let _ = self.active_count.send_modify(|count| *count += 1);
                }
                Ok(state)
            }

            async fn try_claim_fallback(
                &self,
                upstream_id: Uuid,
                holder: &str,
                idempotency_key: &str,
                generation: u64,
                now_unix_secs: u64,
            ) -> StorageResult<Option<super::LazyRefreshFallbackLease>> {
                let claimed = self
                    .inner
                    .try_claim_fallback(
                        upstream_id,
                        holder,
                        idempotency_key,
                        generation,
                        now_unix_secs,
                    )
                    .await?;
                if claimed.is_none() {
                    let _ = self.fallback_none_count.send_modify(|count| *count += 1);
                }
                Ok(claimed)
            }

            async fn renew_fallback_claim(
                &self,
                upstream_id: Uuid,
                lease: &super::LazyRefreshFallbackLease,
                now_unix_secs: u64,
            ) -> StorageResult<Option<super::LazyRefreshFallbackLease>> {
                self.inner
                    .renew_fallback_claim(upstream_id, lease, now_unix_secs)
                    .await
            }

            fn fallback_lease_meta_key(&self, upstream_id: Uuid) -> Option<String> {
                self.inner.fallback_lease_meta_key(upstream_id)
            }

            async fn finish_fallback_claim(
                &self,
                upstream_id: Uuid,
                lease: &super::LazyRefreshFallbackLease,
                final_generation: u64,
            ) -> StorageResult<()> {
                self.inner
                    .finish_fallback_claim(upstream_id, lease, final_generation)
                    .await
            }
        }

        // Sub-case A: Pending watchdog recovery row behind Killed canonical row gets expedited.
        let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
        let upstream_id = fixture.create_oauth_upstream().await;
        let record = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await?
            .ok_or("missing upstream")?;
        let bundle = super::oauth_token_bundle(&record, fixture.aead.as_ref())
            .map_err(|e| format!("{e}"))?;
        let gen1 = record.oauth_token_generation;
        let exp1 = bundle.expires_at_unix_secs;
        let now = cc_lb_clock::unix_secs(fixture.clock.now());

        let job = cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob::for_credential(
            upstream_id,
            gen1,
            exp1,
        );
        let canonical_key = job.generation_idempotency_key(gen1, exp1);
        let recovery_key = job.watchdog_recovery_idempotency_key(gen1, exp1, now);
        fixture
            .scheduler_backend
            .push_adaptive_task(cc_lb_scheduler::worker::SchedulerPushTask {
                args: cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(job.clone()),
                idempotency_key: Some(canonical_key.clone()),
                run_at_unix_secs: Some(now),
                max_attempts: None,
            })
            .await?;
        sqlx::query("UPDATE Jobs SET status = 'Killed', done_at = ?1 WHERE idempotency_key = ?2")
            .bind(i64::try_from(now)?)
            .bind(&canonical_key)
            .execute(&fixture.scheduler_pool)
            .await?;
        fixture
            .scheduler_backend
            .push_adaptive_task(cc_lb_scheduler::worker::SchedulerPushTask {
                args: cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(job.clone()),
                idempotency_key: Some(recovery_key.clone()),
                run_at_unix_secs: Some(now + 3_000),
                max_attempts: None,
            })
            .await?;

        let (active_tx, mut active_rx) = watch::channel(0usize);
        let (fallback_none_tx, mut fallback_none_rx) = watch::channel(0usize);
        let seen_task_keys = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observer = Arc::new(ObservingRecoveryClaims {
            inner: super::ApalisLazyRefreshClaimGuard::new(
                fixture.scheduler_backend.clone(),
                fixture.storage.clone(),
            ),
            active_count: active_tx,
            fallback_none_count: fallback_none_tx,
            seen_task_keys: Arc::clone(&seen_task_keys),
        });
        let refresher = fixture.lazy_refresher(
            observer,
            LazyRefreshContentionConfig::for_tests(
                Duration::from_secs(2),
                Duration::from_millis(5),
            ),
        );

        refresher.enqueue_only(upstream_id).await?;
        let recovery_run_at: i64 =
            sqlx::query_scalar("SELECT run_at FROM Jobs WHERE idempotency_key = ?1")
                .bind(&recovery_key)
                .fetch_one(&fixture.scheduler_pool)
                .await?;
        assert!(
            u64::try_from(recovery_run_at)? <= now,
            "enqueue_only must expedite active watchdog recovery row behind Killed canonical row"
        );

        // Sub-case B: Running watchdog recovery row behind Killed canonical row causes waiter to
        // join the active recovery worker with zero duplicate HTTP calls.
        sqlx::query(
            "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen, started_at) \
             VALUES ('recovery-worker', 'adaptive', 'default', '', ?1, ?1)",
        )
        .bind(i64::try_from(now)?)
        .execute(&fixture.scheduler_pool)
        .await?;
        sqlx::query(
            "UPDATE Jobs SET status = 'Running', lock_by = 'recovery-worker', lock_at = ?1 WHERE idempotency_key = ?2",
        )
        .bind(i64::try_from(now)?)
        .bind(&recovery_key)
        .execute(&fixture.scheduler_pool)
        .await?;
        let worker_lease_b = super::try_acquire_oauth_refresh_lease(
            fixture.storage.as_ref(),
            upstream_id,
            gen1,
            now,
        )
        .await?
        .ok_or("worker_lease_b must succeed")?;

        let refresher_b = refresher.clone();
        let wait_task_b = tokio::spawn(async move { refresher_b.refresh_one(upstream_id).await });
        tokio::time::timeout(
            Duration::from_secs(2),
            active_rx.wait_for(|&count| count >= 1),
        )
        .await??;
        assert!(
            seen_task_keys
                .lock()
                .expect("seen lock")
                .iter()
                .any(|(k, s)| {
                    k == &recovery_key && matches!(s, super::LazyRefreshTaskState::Active)
                }),
            "waiter must observe Active state for recovery_key"
        );

        let tokens_gen2 = EncryptedOAuthTokens::encrypt(
            fixture.aead.as_ref(),
            &OAuthTokenBundle {
                oauth_profile: Default::default(),
                access_token: "sk-ant-oat01-rec-2".to_owned(),
                refresh_token: "sk-ant-ort01-rec-2".to_owned(),
                expires_at_unix_secs: now + 3_600,
                refresh_token_expires_at_unix_secs: None,
                account_id: None,
                project_id: None,
                scopes: vec!["messages".to_owned()],
                never_refresh: false,
            },
            upstream_id.as_bytes(),
        )?;
        let gen2 = fixture
            .storage
            .complete_refresh_fenced(
                upstream_id,
                Uuid::new_v4(),
                tokens_gen2,
                &cc_lb_storage_api::OAuthRefreshFence {
                    expected_generation: gen1,
                    lease_meta_key: super::oauth_refresh_lease_meta_key(upstream_id),
                    expected_lease_prefix: super::oauth_refresh_lease_fence_prefix(
                        &worker_lease_b.token,
                    ),
                },
            )
            .await?
            .oauth_token_generation;
        sqlx::query("UPDATE Jobs SET status = 'Done', done_at = ?1 WHERE idempotency_key = ?2")
            .bind(i64::try_from(now)?)
            .bind(&recovery_key)
            .execute(&fixture.scheduler_pool)
            .await?;
        wait_task_b.await??;
        assert_eq!(fixture.refresh_call_count(), 0);

        // Sub-case C: Canonical row at gen2 is Killed (no recovery row yet), while another holder
        // owns the MetaStore lease -> waiter preserves its original wait budget instead of failing immediately.
        let canonical_key_2 = job.generation_idempotency_key(gen2, now + 3_600);
        fixture
            .scheduler_backend
            .push_adaptive_task(cc_lb_scheduler::worker::SchedulerPushTask {
                args: cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(
                    cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob::for_credential(
                        upstream_id,
                        gen2,
                        now + 3_600,
                    ),
                ),
                idempotency_key: Some(canonical_key_2.clone()),
                run_at_unix_secs: Some(now),
                max_attempts: None,
            })
            .await?;
        sqlx::query("UPDATE Jobs SET status = 'Killed', done_at = ?1 WHERE idempotency_key = ?2")
            .bind(i64::try_from(now)?)
            .bind(&canonical_key_2)
            .execute(&fixture.scheduler_pool)
            .await?;
        let worker_lease_c = super::try_acquire_oauth_refresh_lease(
            fixture.storage.as_ref(),
            upstream_id,
            gen2,
            now,
        )
        .await?
        .ok_or("worker_lease_c must succeed")?;

        let wait_task_c = tokio::spawn(async move { refresher.refresh_one(upstream_id).await });
        tokio::time::timeout(
            Duration::from_secs(2),
            fallback_none_rx.wait_for(|&count| count >= 1),
        )
        .await??;
        assert!(
            !wait_task_c.is_finished(),
            "waiter must preserve wait budget while another holder owns the MetaStore lease"
        );

        let tokens_gen3 = EncryptedOAuthTokens::encrypt(
            fixture.aead.as_ref(),
            &OAuthTokenBundle {
                oauth_profile: Default::default(),
                access_token: "sk-ant-oat01-rec-3".to_owned(),
                refresh_token: "sk-ant-ort01-rec-3".to_owned(),
                expires_at_unix_secs: now + 7_200,
                refresh_token_expires_at_unix_secs: None,
                account_id: None,
                project_id: None,
                scopes: vec!["messages".to_owned()],
                never_refresh: false,
            },
            upstream_id.as_bytes(),
        )?;
        fixture
            .storage
            .complete_refresh_fenced(
                upstream_id,
                Uuid::new_v4(),
                tokens_gen3,
                &cc_lb_storage_api::OAuthRefreshFence {
                    expected_generation: gen2,
                    lease_meta_key: super::oauth_refresh_lease_meta_key(upstream_id),
                    expected_lease_prefix: super::oauth_refresh_lease_fence_prefix(
                        &worker_lease_c.token,
                    ),
                },
            )
            .await?;
        wait_task_c.await??;
        assert_eq!(fixture.refresh_call_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn lazy_refresher_keeps_waiting_when_recovery_job_reschedules_from_running_to_pending()
    -> Result<(), Box<dyn std::error::Error>> {
        use tokio::sync::watch;

        struct ObservingRescheduleClaims {
            inner: super::ApalisLazyRefreshClaimGuard,
            active_count: watch::Sender<usize>,
        }

        #[async_trait::async_trait]
        impl super::LazyRefreshClaimGuard for ObservingRescheduleClaims {
            async fn begin_refresh(
                &self,
                upstream_id: Uuid,
                holder: &str,
                expires_at_unix_secs: u64,
                now_unix_secs: u64,
            ) -> StorageResult<super::LazyRefreshClaim> {
                self.inner
                    .begin_refresh(upstream_id, holder, expires_at_unix_secs, now_unix_secs)
                    .await
            }

            async fn begin_refresh_for_generation(
                &self,
                upstream_id: Uuid,
                holder: &str,
                expires_at_unix_secs: u64,
                expected_generation: u64,
                now_unix_secs: u64,
            ) -> StorageResult<super::LazyRefreshClaim> {
                self.inner
                    .begin_refresh_for_generation(
                        upstream_id,
                        holder,
                        expires_at_unix_secs,
                        expected_generation,
                        now_unix_secs,
                    )
                    .await
            }

            async fn complete_and_bump_generation(
                &self,
                upstream_id: Uuid,
                holder: &str,
                generation: u64,
            ) -> StorageResult<bool> {
                self.inner
                    .complete_and_bump_generation(upstream_id, holder, generation)
                    .await
            }

            async fn release_if_holder(
                &self,
                upstream_id: Uuid,
                holder: &str,
            ) -> StorageResult<bool> {
                self.inner.release_if_holder(upstream_id, holder).await
            }

            async fn task_state(
                &self,
                idempotency_key: &str,
            ) -> StorageResult<super::LazyRefreshTaskState> {
                let state = self.inner.task_state(idempotency_key).await?;
                if matches!(state, super::LazyRefreshTaskState::Active) {
                    let _ = self.active_count.send_modify(|count| *count += 1);
                }
                Ok(state)
            }

            async fn try_claim_fallback(
                &self,
                upstream_id: Uuid,
                holder: &str,
                idempotency_key: &str,
                generation: u64,
                now_unix_secs: u64,
            ) -> StorageResult<Option<super::LazyRefreshFallbackLease>> {
                self.inner
                    .try_claim_fallback(
                        upstream_id,
                        holder,
                        idempotency_key,
                        generation,
                        now_unix_secs,
                    )
                    .await
            }

            async fn renew_fallback_claim(
                &self,
                upstream_id: Uuid,
                lease: &super::LazyRefreshFallbackLease,
                now_unix_secs: u64,
            ) -> StorageResult<Option<super::LazyRefreshFallbackLease>> {
                self.inner
                    .renew_fallback_claim(upstream_id, lease, now_unix_secs)
                    .await
            }

            fn fallback_lease_meta_key(&self, upstream_id: Uuid) -> Option<String> {
                self.inner.fallback_lease_meta_key(upstream_id)
            }

            async fn finish_fallback_claim(
                &self,
                upstream_id: Uuid,
                lease: &super::LazyRefreshFallbackLease,
                final_generation: u64,
            ) -> StorageResult<()> {
                self.inner
                    .finish_fallback_claim(upstream_id, lease, final_generation)
                    .await
            }
        }

        let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
        let upstream_id = fixture.create_oauth_upstream().await;
        let record = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await?
            .ok_or("missing upstream")?;
        let bundle = super::oauth_token_bundle(&record, fixture.aead.as_ref())
            .map_err(|e| format!("{e}"))?;
        let gen1 = record.oauth_token_generation;
        let exp1 = bundle.expires_at_unix_secs;
        let now = cc_lb_clock::unix_secs(fixture.clock.now());

        let job = cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob::for_credential(
            upstream_id,
            gen1,
            exp1,
        );
        let canonical_key = job.generation_idempotency_key(gen1, exp1);
        let recovery_key = job.watchdog_recovery_idempotency_key(gen1, exp1, now);
        fixture
            .scheduler_backend
            .push_adaptive_task(cc_lb_scheduler::worker::SchedulerPushTask {
                args: cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(job.clone()),
                idempotency_key: Some(canonical_key.clone()),
                run_at_unix_secs: Some(now),
                max_attempts: None,
            })
            .await?;
        sqlx::query("UPDATE Jobs SET status = 'Killed', done_at = ?1 WHERE idempotency_key = ?2")
            .bind(i64::try_from(now)?)
            .bind(&canonical_key)
            .execute(&fixture.scheduler_pool)
            .await?;

        sqlx::query(
            "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen, started_at) \
             VALUES ('recovery-worker', 'adaptive', 'default', '', ?1, ?1)",
        )
        .bind(i64::try_from(now)?)
        .execute(&fixture.scheduler_pool)
        .await?;
        fixture
            .scheduler_backend
            .push_adaptive_task(cc_lb_scheduler::worker::SchedulerPushTask {
                args: cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(job),
                idempotency_key: Some(recovery_key.clone()),
                run_at_unix_secs: Some(now),
                max_attempts: Some(5),
            })
            .await?;
        sqlx::query(
            "UPDATE Jobs SET status = 'Running', lock_by = 'recovery-worker', lock_at = ?1 WHERE idempotency_key = ?2",
        )
        .bind(i64::try_from(now)?)
        .bind(&recovery_key)
        .execute(&fixture.scheduler_pool)
        .await?;

        let worker_lease = super::try_acquire_oauth_refresh_lease(
            fixture.storage.as_ref(),
            upstream_id,
            gen1,
            now,
        )
        .await?
        .ok_or("worker_lease must succeed")?;

        let (active_tx, mut active_rx) = watch::channel(0usize);
        let base_guard = super::ApalisLazyRefreshClaimGuard::new(
            fixture.scheduler_backend.clone(),
            fixture.storage.clone(),
        );
        let observer = Arc::new(ObservingRescheduleClaims {
            inner: base_guard.clone(),
            active_count: active_tx,
        });
        let refresher = fixture.lazy_refresher(
            observer,
            LazyRefreshContentionConfig::for_tests(
                Duration::from_secs(2),
                Duration::from_millis(5),
            ),
        );

        let wait_task = tokio::spawn(async move { refresher.refresh_one(upstream_id).await });
        tokio::time::timeout(
            Duration::from_secs(2),
            active_rx.wait_for(|&count| count >= 1),
        )
        .await??;

        // Reschedule the recovery job from Running back to retryable Pending AND release the lease.
        sqlx::query(
            "UPDATE Jobs SET status = 'Pending', attempts = 1, max_attempts = 5, lock_by = NULL, lock_at = NULL, run_at = ?1 WHERE idempotency_key = ?2",
        )
        .bind(i64::try_from(now + 30)?)
        .bind(&recovery_key)
        .execute(&fixture.scheduler_pool)
        .await?;
        super::release_oauth_refresh_lease(
            fixture.storage.as_ref(),
            upstream_id,
            &worker_lease.token,
            gen1,
        )
        .await?;

        let after_reschedule = *active_rx.borrow();
        tokio::time::timeout(
            Duration::from_secs(2),
            active_rx.wait_for(|&count| count > after_reschedule),
        )
        .await??;
        assert!(!wait_task.is_finished());
        assert_eq!(fixture.refresh_call_count(), 0);
        assert_eq!(
            base_guard.task_state(&canonical_key).await?,
            super::LazyRefreshTaskState::Active
        );
        assert_eq!(
            base_guard.task_state(&recovery_key).await?,
            super::LazyRefreshTaskState::Active
        );

        let tokens_gen2 = EncryptedOAuthTokens::encrypt(
            fixture.aead.as_ref(),
            &OAuthTokenBundle {
                oauth_profile: Default::default(),
                access_token: "sk-ant-oat01-resched-2".to_owned(),
                refresh_token: "sk-ant-ort01-resched-2".to_owned(),
                expires_at_unix_secs: now + 3_600,
                refresh_token_expires_at_unix_secs: None,
                account_id: None,
                project_id: None,
                scopes: vec!["messages".to_owned()],
                never_refresh: false,
            },
            upstream_id.as_bytes(),
        )?;
        fixture
            .storage
            .complete_refresh(upstream_id, Uuid::new_v4(), tokens_gen2)
            .await?;
        sqlx::query("UPDATE Jobs SET status = 'Done', done_at = ?1 WHERE idempotency_key = ?2")
            .bind(i64::try_from(now)?)
            .bind(&recovery_key)
            .execute(&fixture.scheduler_pool)
            .await?;
        wait_task.await??;
        assert_eq!(fixture.refresh_call_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn lazy_refresher_bounds_contended_fallback_storage_poll_by_wait_deadline()
    -> Result<(), Box<dyn std::error::Error>> {
        use tokio::sync::watch;

        struct HangOnSecondClaimGuard {
            inner: super::ApalisLazyRefreshClaimGuard,
            calls: Arc<AtomicUsize>,
            second_entered: watch::Sender<bool>,
        }

        #[async_trait::async_trait]
        impl super::LazyRefreshClaimGuard for HangOnSecondClaimGuard {
            async fn begin_refresh(
                &self,
                upstream_id: Uuid,
                holder: &str,
                expires_at_unix_secs: u64,
                now_unix_secs: u64,
            ) -> StorageResult<super::LazyRefreshClaim> {
                self.inner
                    .begin_refresh(upstream_id, holder, expires_at_unix_secs, now_unix_secs)
                    .await
            }

            async fn begin_refresh_for_generation(
                &self,
                upstream_id: Uuid,
                holder: &str,
                expires_at_unix_secs: u64,
                expected_generation: u64,
                now_unix_secs: u64,
            ) -> StorageResult<super::LazyRefreshClaim> {
                self.inner
                    .begin_refresh_for_generation(
                        upstream_id,
                        holder,
                        expires_at_unix_secs,
                        expected_generation,
                        now_unix_secs,
                    )
                    .await
            }

            async fn complete_and_bump_generation(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
                _generation: u64,
            ) -> StorageResult<bool> {
                Ok(false)
            }

            async fn release_if_holder(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
            ) -> StorageResult<bool> {
                Ok(false)
            }

            async fn task_state(
                &self,
                idempotency_key: &str,
            ) -> StorageResult<super::LazyRefreshTaskState> {
                self.inner.task_state(idempotency_key).await
            }

            async fn try_claim_fallback(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
                _idempotency_key: &str,
                _generation: u64,
                _now_unix_secs: u64,
            ) -> StorageResult<Option<super::LazyRefreshFallbackLease>> {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(None)
                } else {
                    self.second_entered.send_replace(true);
                    std::future::pending::<()>().await;
                    Ok(None)
                }
            }
        }

        struct CountingFailClosedGuard {
            inner: super::ApalisLazyRefreshClaimGuard,
            calls: Arc<AtomicUsize>,
        }

        #[async_trait::async_trait]
        impl super::LazyRefreshClaimGuard for CountingFailClosedGuard {
            async fn begin_refresh(
                &self,
                upstream_id: Uuid,
                holder: &str,
                expires_at_unix_secs: u64,
                now_unix_secs: u64,
            ) -> StorageResult<super::LazyRefreshClaim> {
                self.inner
                    .begin_refresh(upstream_id, holder, expires_at_unix_secs, now_unix_secs)
                    .await
            }

            async fn begin_refresh_for_generation(
                &self,
                upstream_id: Uuid,
                holder: &str,
                expires_at_unix_secs: u64,
                expected_generation: u64,
                now_unix_secs: u64,
            ) -> StorageResult<super::LazyRefreshClaim> {
                self.inner
                    .begin_refresh_for_generation(
                        upstream_id,
                        holder,
                        expires_at_unix_secs,
                        expected_generation,
                        now_unix_secs,
                    )
                    .await
            }

            async fn complete_and_bump_generation(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
                _generation: u64,
            ) -> StorageResult<bool> {
                Ok(false)
            }

            async fn release_if_holder(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
            ) -> StorageResult<bool> {
                Ok(false)
            }

            async fn task_state(
                &self,
                idempotency_key: &str,
            ) -> StorageResult<super::LazyRefreshTaskState> {
                self.inner.task_state(idempotency_key).await
            }

            async fn try_claim_fallback(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
                _idempotency_key: &str,
                _generation: u64,
                _now_unix_secs: u64,
            ) -> StorageResult<Option<super::LazyRefreshFallbackLease>> {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(None)
                } else {
                    Ok(Some(super::LazyRefreshFallbackLease::from_token(
                        "unexpected-post-deadline-lease".to_owned(),
                    )))
                }
            }
        }

        // Case 1: A contended fallback storage call hanging across wait_deadline is aborted when the budget expires.
        let fixture1 = LazyRefreshFixture::new(Duration::ZERO).await;
        let upstream1 = fixture1.create_oauth_upstream().await;
        let record1 = UpstreamStore::get_by_id(fixture1.storage.as_ref(), upstream1)
            .await?
            .ok_or("missing upstream1")?;
        let bundle1 = super::oauth_token_bundle(&record1, fixture1.aead.as_ref())
            .map_err(|e| format!("{e}"))?;
        let now1 = cc_lb_clock::unix_secs(fixture1.clock.now());
        let job1 = cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob::for_credential(
            upstream1,
            record1.oauth_token_generation,
            bundle1.expires_at_unix_secs,
        );
        let key1 = job1.generation_idempotency_key(
            record1.oauth_token_generation,
            bundle1.expires_at_unix_secs,
        );
        fixture1
            .scheduler_backend
            .push_adaptive_task(cc_lb_scheduler::worker::SchedulerPushTask {
                args: cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(job1),
                idempotency_key: Some(key1.clone()),
                run_at_unix_secs: Some(now1),
                max_attempts: None,
            })
            .await?;
        sqlx::query("UPDATE Jobs SET status = 'Killed', done_at = ?1 WHERE idempotency_key = ?2")
            .bind(i64::try_from(now1)?)
            .bind(&key1)
            .execute(&fixture1.scheduler_pool)
            .await?;

        let (entered_tx, mut entered_rx) = watch::channel(false);
        let hang_calls = Arc::new(AtomicUsize::new(0));
        let refresher1 = fixture1.lazy_refresher(
            Arc::new(HangOnSecondClaimGuard {
                inner: super::ApalisLazyRefreshClaimGuard::new(
                    fixture1.scheduler_backend.clone(),
                    fixture1.storage.clone(),
                ),
                calls: Arc::clone(&hang_calls),
                second_entered: entered_tx,
            }),
            LazyRefreshContentionConfig::for_tests(
                Duration::from_millis(500),
                Duration::from_millis(5),
            ),
        );
        let handle1 = tokio::spawn(async move { refresher1.refresh_one(upstream1).await });
        tokio::time::timeout(
            Duration::from_secs(2),
            entered_rx.wait_for(|&entered| entered),
        )
        .await??;
        let err1 = tokio::time::timeout(Duration::from_secs(2), handle1)
            .await??
            .expect_err("hung contended poll must fail at wait_deadline");
        assert!(matches!(err1, super::LazyRefreshError::Failed { .. }));
        assert_eq!(fixture1.refresh_call_count(), 0);
        assert_eq!(hang_calls.load(Ordering::SeqCst), 2);

        // Case 2: Sleeping to wait_deadline never starts another storage poll/acquisition after expiry.
        let fixture2 = LazyRefreshFixture::new(Duration::ZERO).await;
        let upstream2 = fixture2.create_oauth_upstream().await;
        let record2 = UpstreamStore::get_by_id(fixture2.storage.as_ref(), upstream2)
            .await?
            .ok_or("missing upstream2")?;
        let bundle2 = super::oauth_token_bundle(&record2, fixture2.aead.as_ref())
            .map_err(|e| format!("{e}"))?;
        let now2 = cc_lb_clock::unix_secs(fixture2.clock.now());
        let job2 = cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob::for_credential(
            upstream2,
            record2.oauth_token_generation,
            bundle2.expires_at_unix_secs,
        );
        let key2 = job2.generation_idempotency_key(
            record2.oauth_token_generation,
            bundle2.expires_at_unix_secs,
        );
        fixture2
            .scheduler_backend
            .push_adaptive_task(cc_lb_scheduler::worker::SchedulerPushTask {
                args: cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(job2),
                idempotency_key: Some(key2.clone()),
                run_at_unix_secs: Some(now2),
                max_attempts: None,
            })
            .await?;
        sqlx::query("UPDATE Jobs SET status = 'Killed', done_at = ?1 WHERE idempotency_key = ?2")
            .bind(i64::try_from(now2)?)
            .bind(&key2)
            .execute(&fixture2.scheduler_pool)
            .await?;

        let post_calls = Arc::new(AtomicUsize::new(0));
        let refresher2 = fixture2.lazy_refresher(
            Arc::new(CountingFailClosedGuard {
                inner: super::ApalisLazyRefreshClaimGuard::new(
                    fixture2.scheduler_backend.clone(),
                    fixture2.storage.clone(),
                ),
                calls: Arc::clone(&post_calls),
            }),
            LazyRefreshContentionConfig::for_tests(
                Duration::from_millis(30),
                Duration::from_millis(100),
            ),
        );
        let err2 = tokio::time::timeout(Duration::from_secs(2), refresher2.refresh_one(upstream2))
            .await?
            .expect_err("expired contention sleep must not invoke a post-deadline poll");
        assert!(matches!(err2, super::LazyRefreshError::Failed { .. }));
        assert_eq!(post_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fixture2.refresh_call_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn lazy_refresher_releases_committed_lease_when_contended_claim_cancels_before_delivery()
    -> Result<(), Box<dyn std::error::Error>> {
        use tokio::sync::watch;

        struct DelayedDeliveryAfterCommitGuard {
            inner: super::ApalisLazyRefreshClaimGuard,
            calls: AtomicUsize,
            pause_on_call_index: usize,
            committed: watch::Sender<bool>,
            allow_return: watch::Receiver<bool>,
            released: watch::Sender<bool>,
        }

        #[async_trait::async_trait]
        impl super::LazyRefreshClaimGuard for DelayedDeliveryAfterCommitGuard {
            async fn begin_refresh(
                &self,
                upstream_id: Uuid,
                holder: &str,
                expires_at_unix_secs: u64,
                now_unix_secs: u64,
            ) -> StorageResult<super::LazyRefreshClaim> {
                self.inner
                    .begin_refresh(upstream_id, holder, expires_at_unix_secs, now_unix_secs)
                    .await
            }

            async fn begin_refresh_for_generation(
                &self,
                upstream_id: Uuid,
                holder: &str,
                expires_at_unix_secs: u64,
                expected_generation: u64,
                now_unix_secs: u64,
            ) -> StorageResult<super::LazyRefreshClaim> {
                self.inner
                    .begin_refresh_for_generation(
                        upstream_id,
                        holder,
                        expires_at_unix_secs,
                        expected_generation,
                        now_unix_secs,
                    )
                    .await
            }

            async fn complete_and_bump_generation(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
                _generation: u64,
            ) -> StorageResult<bool> {
                Ok(false)
            }

            async fn release_if_holder(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
            ) -> StorageResult<bool> {
                Ok(false)
            }

            async fn task_state(
                &self,
                idempotency_key: &str,
            ) -> StorageResult<super::LazyRefreshTaskState> {
                self.inner.task_state(idempotency_key).await
            }

            async fn try_claim_fallback(
                &self,
                upstream_id: Uuid,
                holder: &str,
                idempotency_key: &str,
                generation: u64,
                now_unix_secs: u64,
            ) -> StorageResult<Option<super::LazyRefreshFallbackLease>> {
                if self.calls.fetch_add(1, Ordering::SeqCst) < self.pause_on_call_index {
                    return Ok(None);
                }
                let lease = self
                    .inner
                    .try_claim_fallback(
                        upstream_id,
                        holder,
                        idempotency_key,
                        generation,
                        now_unix_secs,
                    )
                    .await?;
                assert!(
                    lease.is_some(),
                    "inner SQLite MetaStore claim must commit before pausing delivery"
                );
                self.committed.send_replace(true);
                let mut allow_return = self.allow_return.clone();
                let _ = allow_return.wait_for(|&go| go).await;
                Ok(lease)
            }

            async fn renew_fallback_claim(
                &self,
                upstream_id: Uuid,
                lease: &super::LazyRefreshFallbackLease,
                now_unix_secs: u64,
            ) -> StorageResult<Option<super::LazyRefreshFallbackLease>> {
                self.inner
                    .renew_fallback_claim(upstream_id, lease, now_unix_secs)
                    .await
            }

            fn fallback_lease_meta_key(&self, upstream_id: Uuid) -> Option<String> {
                self.inner.fallback_lease_meta_key(upstream_id)
            }

            async fn finish_fallback_claim(
                &self,
                upstream_id: Uuid,
                lease: &super::LazyRefreshFallbackLease,
                final_generation: u64,
            ) -> StorageResult<()> {
                let res = self
                    .inner
                    .finish_fallback_claim(upstream_id, lease, final_generation)
                    .await;
                self.released.send_replace(true);
                res
            }
        }

        async fn setup_killed_job_fixture()
        -> Result<(LazyRefreshFixture, Uuid, u64), Box<dyn std::error::Error>> {
            let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
            let upstream_id = fixture.create_oauth_upstream().await;
            let record = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
                .await?
                .ok_or("missing upstream")?;
            let bundle = super::oauth_token_bundle(&record, fixture.aead.as_ref())
                .map_err(|e| format!("{e}"))?;
            let now = cc_lb_clock::unix_secs(fixture.clock.now());
            let job = cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob::for_credential(
                upstream_id,
                record.oauth_token_generation,
                bundle.expires_at_unix_secs,
            );
            let key = job.generation_idempotency_key(
                record.oauth_token_generation,
                bundle.expires_at_unix_secs,
            );
            fixture
                .scheduler_backend
                .push_adaptive_task(cc_lb_scheduler::worker::SchedulerPushTask {
                    args: cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(job),
                    idempotency_key: Some(key.clone()),
                    run_at_unix_secs: Some(now),
                    max_attempts: None,
                })
                .await?;
            sqlx::query(
                "UPDATE Jobs SET status = 'Killed', done_at = ?1 WHERE idempotency_key = ?2",
            )
            .bind(i64::try_from(now)?)
            .bind(&key)
            .execute(&fixture.scheduler_pool)
            .await?;
            Ok((fixture, upstream_id, record.oauth_token_generation))
        }

        // Case 1: wait_deadline expires after SQLite commits `held:...` and before delivery reaches the waiter.
        let (fixture1, upstream1, gen1) = setup_killed_job_fixture().await?;
        let lease_key1 = super::oauth_refresh_lease_meta_key(upstream1);
        let (committed_tx1, mut committed_rx1) = watch::channel(false);
        let (allow_tx1, allow_rx1) = watch::channel(false);
        let (released_tx1, mut released_rx1) = watch::channel(false);
        let refresher1 = fixture1.lazy_refresher(
            Arc::new(DelayedDeliveryAfterCommitGuard {
                inner: super::ApalisLazyRefreshClaimGuard::new(
                    fixture1.scheduler_backend.clone(),
                    fixture1.storage.clone(),
                ),
                calls: AtomicUsize::new(0),
                pause_on_call_index: 1,
                committed: committed_tx1,
                allow_return: allow_rx1,
                released: released_tx1,
            }),
            LazyRefreshContentionConfig::for_tests(
                Duration::from_millis(500),
                Duration::from_millis(5),
            ),
        );
        let handle1 = tokio::spawn(async move { refresher1.refresh_one(upstream1).await });
        tokio::time::timeout(
            Duration::from_secs(2),
            committed_rx1.wait_for(|&committed| committed),
        )
        .await??;
        let held1 = fixture1
            .storage
            .get_meta_value(&lease_key1)
            .await?
            .ok_or("missing committed lease1")?;
        assert!(held1.starts_with(&format!("held:{gen1}:")));
        let err1 = tokio::time::timeout(Duration::from_secs(2), handle1)
            .await??
            .expect_err("deadline timeout must abort contended fallback claim");
        assert!(matches!(err1, super::LazyRefreshError::Failed { .. }));
        allow_tx1.send_replace(true);
        tokio::time::timeout(
            Duration::from_secs(2),
            released_rx1.wait_for(|&released| released),
        )
        .await??;
        assert_eq!(
            fixture1
                .storage
                .get_meta_value(&lease_key1)
                .await?
                .as_deref(),
            Some(format!("idle:{gen1}").as_str())
        );
        let healthy1 = super::ApalisLazyRefreshClaimGuard::new(
            fixture1.scheduler_backend.clone(),
            fixture1.storage.clone(),
        );
        let healthy_lease1 = healthy1
            .try_claim_fallback(
                upstream1,
                "healthy-worker-1",
                "k1",
                gen1,
                cc_lb_clock::unix_secs(fixture1.clock.now()),
            )
            .await?
            .ok_or("healthy worker 1 must immediately acquire released fallback lease")?;
        healthy1
            .finish_fallback_claim(upstream1, &healthy_lease1, gen1)
            .await?;
        assert_eq!(fixture1.refresh_call_count(), 0);

        // Case 2: shutdown cancellation wins after SQLite commits `held:...` and before delivery reaches the waiter.
        let (fixture2, upstream2, gen2) = setup_killed_job_fixture().await?;
        let lease_key2 = super::oauth_refresh_lease_meta_key(upstream2);
        let (committed_tx2, mut committed_rx2) = watch::channel(false);
        let (allow_tx2, allow_rx2) = watch::channel(false);
        let (released_tx2, mut released_rx2) = watch::channel(false);
        let cancel2 = CancellationToken::new();
        let refresher2 = super::LazyRefresher::new_with_claim_guard_for_tests(
            LazyRefresherDeps {
                google_config: Default::default(),
                stores: fixture2.stores.clone(),
                aead: fixture2.aead.clone(),
                oauth_cfg: fixture2.oauth_cfg.clone(),
                clock: fixture2.clock.clone(),
            },
            Uuid::new_v4(),
            None,
            cancel2.clone(),
            Arc::new(DelayedDeliveryAfterCommitGuard {
                inner: super::ApalisLazyRefreshClaimGuard::new(
                    fixture2.scheduler_backend.clone(),
                    fixture2.storage.clone(),
                ),
                calls: AtomicUsize::new(0),
                pause_on_call_index: 1,
                committed: committed_tx2,
                allow_return: allow_rx2,
                released: released_tx2,
            }),
            LazyRefreshContentionConfig::for_tests(
                Duration::from_secs(5),
                Duration::from_millis(5),
            ),
            fixture2.scheduler_backend.clone(),
        );
        let handle2 = tokio::spawn(async move { refresher2.refresh_one(upstream2).await });
        tokio::time::timeout(
            Duration::from_secs(2),
            committed_rx2.wait_for(|&committed| committed),
        )
        .await??;
        let held2 = fixture2
            .storage
            .get_meta_value(&lease_key2)
            .await?
            .ok_or("missing committed lease2")?;
        assert!(held2.starts_with(&format!("held:{gen2}:")));
        cancel2.cancel();
        let err2 = tokio::time::timeout(Duration::from_secs(2), handle2)
            .await??
            .expect_err("shutdown cancellation must abort contended fallback claim");
        assert!(matches!(
            err2,
            super::LazyRefreshError::Failed { ref reason } if reason == "oauth refresh cancelled"
        ));
        allow_tx2.send_replace(true);
        tokio::time::timeout(
            Duration::from_secs(2),
            released_rx2.wait_for(|&released| released),
        )
        .await??;
        assert_eq!(
            fixture2
                .storage
                .get_meta_value(&lease_key2)
                .await?
                .as_deref(),
            Some(format!("idle:{gen2}").as_str())
        );
        let healthy2 = super::ApalisLazyRefreshClaimGuard::new(
            fixture2.scheduler_backend.clone(),
            fixture2.storage.clone(),
        );
        let healthy_lease2 = healthy2
            .try_claim_fallback(
                upstream2,
                "healthy-worker-2",
                "k2",
                gen2,
                cc_lb_clock::unix_secs(fixture2.clock.now()),
            )
            .await?
            .ok_or("healthy worker 2 must immediately acquire released fallback lease")?;
        healthy2
            .finish_fallback_claim(upstream2, &healthy_lease2, gen2)
            .await?;
        assert_eq!(fixture2.refresh_call_count(), 0);

        // Case 3: waiter future is dropped after `try_claim_fallback` stored `Delivered(lease)` and queued the oneshot.
        struct NotifyWake(Arc<Notify>);
        impl std::task::Wake for NotifyWake {
            fn wake(self: Arc<Self>) {
                self.0.notify_one();
            }
        }

        let (fixture3, upstream3, gen3) = setup_killed_job_fixture().await?;
        let lease_key3 = super::oauth_refresh_lease_meta_key(upstream3);
        let (committed_tx3, mut committed_rx3) = watch::channel(false);
        let (allow_tx3, allow_rx3) = watch::channel(false);
        let (released_tx3, mut released_rx3) = watch::channel(false);
        let guard3: Arc<dyn LazyRefreshClaimGuard> = Arc::new(DelayedDeliveryAfterCommitGuard {
            inner: super::ApalisLazyRefreshClaimGuard::new(
                fixture3.scheduler_backend.clone(),
                fixture3.storage.clone(),
            ),
            calls: AtomicUsize::new(0),
            pause_on_call_index: 0,
            committed: committed_tx3,
            allow_return: allow_rx3,
            released: released_tx3,
        });
        let notify3 = Arc::new(Notify::new());
        let waker3 = std::task::Waker::from(Arc::new(NotifyWake(Arc::clone(&notify3))));
        let mut cx3 = std::task::Context::from_waker(&waker3);
        let mut claim_fut3 = Box::pin(super::try_claim_fallback_cancellation_safe(
            super::FallbackClaimTarget {
                claim_guard: Arc::clone(&guard3),
                stores: fixture3.stores.clone(),
                upstream_id: upstream3,
                idempotency_key: "k3".to_owned(),
                generation: gen3,
            },
            cc_lb_clock::unix_secs(fixture3.clock.now()),
        ));
        assert!(claim_fut3.as_mut().poll(&mut cx3).is_pending());
        tokio::time::timeout(
            Duration::from_secs(2),
            committed_rx3.wait_for(|&committed| committed),
        )
        .await??;
        allow_tx3.send_replace(true);
        tokio::time::timeout(Duration::from_secs(2), notify3.notified()).await?;
        drop(claim_fut3);
        tokio::time::timeout(
            Duration::from_secs(2),
            released_rx3.wait_for(|&released| released),
        )
        .await??;
        assert_eq!(
            fixture3
                .storage
                .get_meta_value(&lease_key3)
                .await?
                .as_deref(),
            Some(format!("idle:{gen3}").as_str())
        );
        let healthy3 = super::ApalisLazyRefreshClaimGuard::new(
            fixture3.scheduler_backend.clone(),
            fixture3.storage.clone(),
        );
        let healthy_lease3 = healthy3
            .try_claim_fallback(
                upstream3,
                "healthy-worker-3",
                "k3",
                gen3,
                cc_lb_clock::unix_secs(fixture3.clock.now()),
            )
            .await?
            .ok_or("healthy worker 3 must immediately acquire released fallback lease")?;
        healthy3
            .finish_fallback_claim(upstream3, &healthy_lease3, gen3)
            .await?;
        Ok(())
    }

    async fn setup_killed_oauth_job_fixture()
    -> Result<(LazyRefreshFixture, Uuid, u64), Box<dyn std::error::Error>> {
        let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
        let upstream_id = fixture.create_oauth_upstream().await;
        let record = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await?
            .ok_or("missing upstream")?;
        let bundle = super::oauth_token_bundle(&record, fixture.aead.as_ref())
            .map_err(|e| format!("{e}"))?;
        let now = cc_lb_clock::unix_secs(fixture.clock.now());
        let job = cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob::for_credential(
            upstream_id,
            record.oauth_token_generation,
            bundle.expires_at_unix_secs,
        );
        let key = job
            .generation_idempotency_key(record.oauth_token_generation, bundle.expires_at_unix_secs);
        fixture
            .scheduler_backend
            .push_adaptive_task(cc_lb_scheduler::worker::SchedulerPushTask {
                args: cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(job),
                idempotency_key: Some(key.clone()),
                run_at_unix_secs: Some(now),
                max_attempts: None,
            })
            .await?;
        sqlx::query("UPDATE Jobs SET status = 'Killed', done_at = ?1 WHERE idempotency_key = ?2")
            .bind(i64::try_from(now)?)
            .bind(&key)
            .execute(&fixture.scheduler_pool)
            .await?;
        Ok((fixture, upstream_id, record.oauth_token_generation))
    }

    #[tokio::test]
    async fn oauth_refresh_lease_reconciles_exact_claim_after_lost_commit_ack()
    -> Result<(), Box<dyn std::error::Error>> {
        use tokio::sync::watch;

        struct LostCommitAckMetaStore {
            inner: Arc<cc_lb_storage_sqlite::SqliteStorage>,
            fail_next_put: AtomicBool,
        }

        #[async_trait::async_trait]
        impl MetaStore for LostCommitAckMetaStore {
            async fn initialize(&self) -> StorageResult<()> {
                self.inner.initialize().await
            }

            async fn backend_kind(&self) -> StorageResult<cc_lb_storage_api::BackendKind> {
                self.inner.backend_kind().await
            }

            async fn get_meta_value(&self, key: &str) -> StorageResult<Option<String>> {
                self.inner.get_meta_value(key).await
            }

            async fn put_meta_value(&self, key: &str, value: &str) -> StorageResult<()> {
                self.inner.put_meta_value(key, value).await
            }

            async fn compare_and_put_meta_value(
                &self,
                key: &str,
                expected: Option<&str>,
                value: &str,
            ) -> StorageResult<bool> {
                let applied = self
                    .inner
                    .compare_and_put_meta_value(key, expected, value)
                    .await?;
                if self.fail_next_put.swap(false, Ordering::SeqCst) {
                    return Err(cc_lb_storage_api::StorageError::Transient {
                        retryable: true,
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::ConnectionReset,
                            "simulated lost COMMIT ack",
                        )),
                    });
                }
                Ok(applied)
            }

            async fn verify_schema_current(&self) -> StorageResult<()> {
                self.inner.verify_schema_current().await
            }
        }

        struct LostAckAfterCommitClaimGuard {
            inner: super::ApalisLazyRefreshClaimGuard,
            calls: AtomicUsize,
            pause_on_call_index: usize,
            committed: watch::Sender<bool>,
            allow_return: watch::Receiver<bool>,
            released: watch::Sender<bool>,
        }

        #[async_trait::async_trait]
        impl super::LazyRefreshClaimGuard for LostAckAfterCommitClaimGuard {
            async fn begin_refresh(
                &self,
                upstream_id: Uuid,
                holder: &str,
                expires_at_unix_secs: u64,
                now_unix_secs: u64,
            ) -> StorageResult<super::LazyRefreshClaim> {
                self.inner
                    .begin_refresh(upstream_id, holder, expires_at_unix_secs, now_unix_secs)
                    .await
            }

            async fn begin_refresh_for_generation(
                &self,
                upstream_id: Uuid,
                holder: &str,
                expires_at_unix_secs: u64,
                expected_generation: u64,
                now_unix_secs: u64,
            ) -> StorageResult<super::LazyRefreshClaim> {
                self.inner
                    .begin_refresh_for_generation(
                        upstream_id,
                        holder,
                        expires_at_unix_secs,
                        expected_generation,
                        now_unix_secs,
                    )
                    .await
            }

            async fn complete_and_bump_generation(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
                _generation: u64,
            ) -> StorageResult<bool> {
                Ok(false)
            }

            async fn release_if_holder(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
            ) -> StorageResult<bool> {
                Ok(false)
            }

            async fn task_state(
                &self,
                idempotency_key: &str,
            ) -> StorageResult<super::LazyRefreshTaskState> {
                self.inner.task_state(idempotency_key).await
            }

            async fn try_claim_fallback(
                &self,
                upstream_id: Uuid,
                holder: &str,
                idempotency_key: &str,
                generation: u64,
                now_unix_secs: u64,
            ) -> StorageResult<Option<super::LazyRefreshFallbackLease>> {
                if self.calls.fetch_add(1, Ordering::SeqCst) < self.pause_on_call_index {
                    return Ok(None);
                }
                let committed = self
                    .inner
                    .try_claim_fallback(
                        upstream_id,
                        holder,
                        idempotency_key,
                        generation,
                        now_unix_secs,
                    )
                    .await?;
                assert!(
                    committed.is_some(),
                    "inner SQLite MetaStore claim must commit before simulating lost ack"
                );
                self.committed.send_replace(true);
                let mut allow_return = self.allow_return.clone();
                let _ = allow_return.wait_for(|&go| go).await;
                Err(cc_lb_storage_api::StorageError::Transient {
                    retryable: true,
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "simulated lost COMMIT ack after wrapper delay",
                    )),
                })
            }

            async fn renew_fallback_claim(
                &self,
                upstream_id: Uuid,
                lease: &super::LazyRefreshFallbackLease,
                now_unix_secs: u64,
            ) -> StorageResult<Option<super::LazyRefreshFallbackLease>> {
                self.inner
                    .renew_fallback_claim(upstream_id, lease, now_unix_secs)
                    .await
            }

            fn fallback_lease_meta_key(&self, upstream_id: Uuid) -> Option<String> {
                self.inner.fallback_lease_meta_key(upstream_id)
            }

            async fn finish_fallback_claim(
                &self,
                upstream_id: Uuid,
                lease: &super::LazyRefreshFallbackLease,
                final_generation: u64,
            ) -> StorageResult<()> {
                let res = self
                    .inner
                    .finish_fallback_claim(upstream_id, lease, final_generation)
                    .await;
                self.released.send_replace(true);
                res
            }
        }

        // Sub-case A: `try_acquire_oauth_refresh_lease` commits in SQLite before returning Err and
        // reconciles its own proposed lease token back to `idle:{gen}`.
        let (fixture_a, upstream_a, gen_a) = setup_killed_oauth_job_fixture().await?;
        let lease_key_a = super::oauth_refresh_lease_meta_key(upstream_a);
        let lost_ack_meta_a = Arc::new(LostCommitAckMetaStore {
            inner: fixture_a.storage.clone(),
            fail_next_put: AtomicBool::new(true),
        });
        let now_a = cc_lb_clock::unix_secs(fixture_a.clock.now());
        let err_a = super::try_acquire_oauth_refresh_lease(
            lost_ack_meta_a.as_ref(),
            upstream_a,
            gen_a,
            now_a,
        )
        .await
        .expect_err("lost commit ack must surface error");
        assert!(matches!(
            err_a,
            cc_lb_storage_api::StorageError::Transient { .. }
        ));
        assert_eq!(
            fixture_a
                .storage
                .get_meta_value(&lease_key_a)
                .await?
                .as_deref(),
            Some(format!("idle:{gen_a}").as_str())
        );
        let healthy_lease_a = super::try_acquire_oauth_refresh_lease(
            fixture_a.storage.as_ref(),
            upstream_a,
            gen_a,
            now_a,
        )
        .await?
        .ok_or("healthy worker A must immediately acquire reconciled lease")?;
        super::release_oauth_refresh_lease(
            fixture_a.storage.as_ref(),
            upstream_a,
            &healthy_lease_a.token,
            gen_a,
        )
        .await?;

        // Sub-case B1: Through `LazyRefresher::refresh_one` using `ApalisLazyRefreshClaimGuard` +
        // `LostCommitAckMetaStore`, the fallback lease is reconciled before returning Err.
        let (fixture_b1, upstream_b1, gen_b1) = setup_killed_oauth_job_fixture().await?;
        let lease_key_b1 = super::oauth_refresh_lease_meta_key(upstream_b1);
        let lost_ack_meta_b1 = Arc::new(LostCommitAckMetaStore {
            inner: fixture_b1.storage.clone(),
            fail_next_put: AtomicBool::new(true),
        });
        let refresher_b1 = fixture_b1.lazy_refresher(
            Arc::new(super::ApalisLazyRefreshClaimGuard::new(
                fixture_b1.scheduler_backend.clone(),
                lost_ack_meta_b1,
            )),
            LazyRefreshContentionConfig::for_tests(
                Duration::from_secs(5),
                Duration::from_millis(5),
            ),
        );
        let err_b1 = refresher_b1
            .refresh_one(upstream_b1)
            .await
            .expect_err("lost commit ack in fallback claim must return error");
        assert!(matches!(err_b1, super::LazyRefreshError::Failed { .. }));
        assert_eq!(
            fixture_b1
                .storage
                .get_meta_value(&lease_key_b1)
                .await?
                .as_deref(),
            Some(format!("idle:{gen_b1}").as_str())
        );
        let healthy_b1 = super::ApalisLazyRefreshClaimGuard::new(
            fixture_b1.scheduler_backend.clone(),
            fixture_b1.storage.clone(),
        );
        let healthy_lease_b1 = healthy_b1
            .try_claim_fallback(
                upstream_b1,
                "healthy-worker-b1",
                "kb1",
                gen_b1,
                cc_lb_clock::unix_secs(fixture_b1.clock.now()),
            )
            .await?
            .ok_or("healthy worker B1 must immediately acquire reconciled fallback lease")?;
        healthy_b1
            .finish_fallback_claim(upstream_b1, &healthy_lease_b1, gen_b1)
            .await?;
        assert_eq!(fixture_b1.refresh_call_count(), 0);

        // Sub-case B2: Waiter cancels while claim guard commits to SQLite and then fails with an
        // ambiguous Err; the claim owner retains the proposed token and releases the exact lease.
        let (fixture_b2, upstream_b2, gen_b2) = setup_killed_oauth_job_fixture().await?;
        let lease_key_b2 = super::oauth_refresh_lease_meta_key(upstream_b2);
        let (committed_tx_b2, mut committed_rx_b2) = watch::channel(false);
        let (allow_tx_b2, allow_rx_b2) = watch::channel(false);
        let (released_tx_b2, mut released_rx_b2) = watch::channel(false);
        let cancel_b2 = CancellationToken::new();
        let refresher_b2 = super::LazyRefresher::new_with_claim_guard_for_tests(
            LazyRefresherDeps {
                google_config: Default::default(),
                stores: fixture_b2.stores.clone(),
                aead: fixture_b2.aead.clone(),
                oauth_cfg: fixture_b2.oauth_cfg.clone(),
                clock: fixture_b2.clock.clone(),
            },
            Uuid::new_v4(),
            None,
            cancel_b2.clone(),
            Arc::new(LostAckAfterCommitClaimGuard {
                inner: super::ApalisLazyRefreshClaimGuard::new(
                    fixture_b2.scheduler_backend.clone(),
                    fixture_b2.storage.clone(),
                ),
                calls: AtomicUsize::new(0),
                pause_on_call_index: 1,
                committed: committed_tx_b2,
                allow_return: allow_rx_b2,
                released: released_tx_b2,
            }),
            LazyRefreshContentionConfig::for_tests(
                Duration::from_secs(5),
                Duration::from_millis(5),
            ),
            fixture_b2.scheduler_backend.clone(),
        );
        let handle_b2 = tokio::spawn(async move { refresher_b2.refresh_one(upstream_b2).await });
        tokio::time::timeout(
            Duration::from_secs(2),
            committed_rx_b2.wait_for(|&committed| committed),
        )
        .await??;
        let held_b2 = fixture_b2
            .storage
            .get_meta_value(&lease_key_b2)
            .await?
            .ok_or("missing committed lease_b2")?;
        assert!(held_b2.starts_with(&format!("held:{gen_b2}:")));
        cancel_b2.cancel();
        let err_b2 = tokio::time::timeout(Duration::from_secs(2), handle_b2)
            .await??
            .expect_err("cancellation must abort waiter while lost-ack commit settles");
        assert!(matches!(
            err_b2,
            super::LazyRefreshError::Failed { ref reason } if reason == "oauth refresh cancelled"
        ));
        allow_tx_b2.send_replace(true);
        tokio::time::timeout(
            Duration::from_secs(2),
            released_rx_b2.wait_for(|&released| released),
        )
        .await??;
        assert_eq!(
            fixture_b2
                .storage
                .get_meta_value(&lease_key_b2)
                .await?
                .as_deref(),
            Some(format!("idle:{gen_b2}").as_str())
        );
        let healthy_b2 = super::ApalisLazyRefreshClaimGuard::new(
            fixture_b2.scheduler_backend.clone(),
            fixture_b2.storage.clone(),
        );
        let healthy_lease_b2 = healthy_b2
            .try_claim_fallback(
                upstream_b2,
                "healthy-worker-b2",
                "kb2",
                gen_b2,
                cc_lb_clock::unix_secs(fixture_b2.clock.now()),
            )
            .await?
            .ok_or("healthy worker B2 must immediately acquire reconciled fallback lease")?;
        healthy_b2
            .finish_fallback_claim(upstream_b2, &healthy_lease_b2, gen_b2)
            .await?;
        assert_eq!(fixture_b2.refresh_call_count(), 0);

        // Sub-case C: Another holder owns an active lease when a failing claim errors without
        // committing; reconciliation must check the exact token and leave the other holder untouched.
        struct FailBeforeCommitClaimGuard(super::ApalisLazyRefreshClaimGuard);

        #[async_trait::async_trait]
        impl super::LazyRefreshClaimGuard for FailBeforeCommitClaimGuard {
            async fn begin_refresh(
                &self,
                upstream_id: Uuid,
                holder: &str,
                expires_at_unix_secs: u64,
                now_unix_secs: u64,
            ) -> StorageResult<super::LazyRefreshClaim> {
                self.0
                    .begin_refresh(upstream_id, holder, expires_at_unix_secs, now_unix_secs)
                    .await
            }

            async fn complete_and_bump_generation(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
                _generation: u64,
            ) -> StorageResult<bool> {
                Ok(false)
            }

            async fn release_if_holder(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
            ) -> StorageResult<bool> {
                Ok(false)
            }

            async fn try_claim_fallback(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
                _idempotency_key: &str,
                _generation: u64,
                _now_unix_secs: u64,
            ) -> StorageResult<Option<super::LazyRefreshFallbackLease>> {
                Err(cc_lb_storage_api::StorageError::Unavailable {
                    message: "transient error before write".to_owned(),
                })
            }

            async fn finish_fallback_claim(
                &self,
                upstream_id: Uuid,
                lease: &super::LazyRefreshFallbackLease,
                final_generation: u64,
            ) -> StorageResult<()> {
                self.0
                    .finish_fallback_claim(upstream_id, lease, final_generation)
                    .await
            }
        }

        let (fixture_c, upstream_c, gen_c) = setup_killed_oauth_job_fixture().await?;
        let lease_key_c = super::oauth_refresh_lease_meta_key(upstream_c);
        let apalis_c = super::ApalisLazyRefreshClaimGuard::new(
            fixture_c.scheduler_backend.clone(),
            fixture_c.storage.clone(),
        );
        let now_c = cc_lb_clock::unix_secs(fixture_c.clock.now());
        let other_holder = Uuid::new_v4().to_string();
        let other_lease = apalis_c
            .try_claim_fallback(upstream_c, &other_holder, "kc", gen_c, now_c)
            .await?
            .ok_or("healthy worker C must hold initial lease")?;
        let failing_guard: Arc<dyn LazyRefreshClaimGuard> =
            Arc::new(FailBeforeCommitClaimGuard(apalis_c.clone()));
        let err_c = super::try_claim_fallback_cancellation_safe(
            super::FallbackClaimTarget {
                claim_guard: failing_guard,
                stores: fixture_c.stores.clone(),
                upstream_id: upstream_c,
                idempotency_key: "kc".to_owned(),
                generation: gen_c,
            },
            now_c,
        )
        .await
        .expect_err("failing guard must propagate error");
        assert!(matches!(
            err_c,
            super::LazyRefreshError::Failed { ref reason }
                if reason.contains("transient error before write")
        ));
        assert_eq!(
            fixture_c
                .storage
                .get_meta_value(&lease_key_c)
                .await?
                .as_deref(),
            Some(other_lease.token())
        );
        apalis_c
            .finish_fallback_claim(upstream_c, &other_lease, gen_c)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn lazy_refresher_releases_fallback_lease_when_post_claim_generation_read_fails()
    -> Result<(), Box<dyn std::error::Error>> {
        struct FailPostClaimGenerationReadStore {
            inner: Arc<cc_lb_storage_sqlite::SqliteStorage>,
            fail_reads: Arc<AtomicBool>,
        }

        #[async_trait::async_trait]
        impl UpstreamStore for FailPostClaimGenerationReadStore {
            async fn create(
                &self,
                create: UpstreamCreate,
            ) -> StorageResult<cc_lb_storage_api::UpstreamRecord> {
                self.inner.create(create).await
            }

            async fn get_by_name(
                &self,
                name: &str,
            ) -> StorageResult<Option<cc_lb_storage_api::UpstreamRecord>> {
                self.inner.get_by_name(name).await
            }

            async fn get_by_id(
                &self,
                id: Uuid,
            ) -> StorageResult<Option<cc_lb_storage_api::UpstreamRecord>> {
                self.inner.get_by_id(id).await
            }

            async fn list(
                &self,
                after: Option<Uuid>,
                limit: usize,
            ) -> StorageResult<Vec<cc_lb_storage_api::UpstreamRecord>> {
                self.inner.list(after, limit).await
            }

            async fn update(
                &self,
                id: Uuid,
                expected_revision: u64,
                update: cc_lb_storage_api::UpstreamUpdate,
            ) -> StorageResult<cc_lb_storage_api::UpstreamRecord> {
                self.inner.update(id, expected_revision, update).await
            }

            async fn set_status(
                &self,
                id: Uuid,
                status: cc_lb_storage_api::UpstreamStatusUpdate,
            ) -> StorageResult<()> {
                self.inner.set_status(id, status).await
            }

            async fn store_oauth_tokens(
                &self,
                id: Uuid,
                expected_revision: u64,
                tokens: EncryptedOAuthTokens,
                never_refresh: bool,
            ) -> StorageResult<cc_lb_storage_api::UpstreamRecord> {
                self.inner
                    .store_oauth_tokens(id, expected_revision, tokens, never_refresh)
                    .await
            }

            async fn complete_refresh(
                &self,
                id: Uuid,
                holder: Uuid,
                tokens: EncryptedOAuthTokens,
            ) -> StorageResult<cc_lb_storage_api::UpstreamRecord> {
                self.inner.complete_refresh(id, holder, tokens).await
            }

            async fn complete_refresh_fenced(
                &self,
                id: Uuid,
                holder: Uuid,
                tokens: EncryptedOAuthTokens,
                fence: &cc_lb_storage_api::OAuthRefreshFence,
            ) -> StorageResult<cc_lb_storage_api::UpstreamRecord> {
                self.inner
                    .complete_refresh_fenced(id, holder, tokens, fence)
                    .await
            }

            async fn read_oauth_token_generation(&self, id: Uuid) -> StorageResult<Option<u64>> {
                if self.fail_reads.load(Ordering::SeqCst) {
                    return Err(cc_lb_storage_api::StorageError::Unavailable {
                        message: "transient post-claim generation read failure".to_owned(),
                    });
                }
                self.inner.read_oauth_token_generation(id).await
            }

            async fn set_last_apply_error(
                &self,
                id: Uuid,
                error: Option<String>,
            ) -> StorageResult<()> {
                self.inner.set_last_apply_error(id, error).await
            }

            async fn soft_delete(&self, id: Uuid, expected_revision: u64) -> StorageResult<()> {
                self.inner.soft_delete(id, expected_revision).await
            }

            async fn hard_delete(&self, id: Uuid) -> StorageResult<()> {
                self.inner.hard_delete(id).await
            }

            async fn clear_warmup_dialect_plugin(
                &self,
                id: Uuid,
                expected_revision: u64,
            ) -> StorageResult<Option<cc_lb_storage_api::UpstreamRecord>> {
                self.inner
                    .clear_warmup_dialect_plugin(id, expected_revision)
                    .await
            }
        }

        struct ArmFailureAfterClaimGuard {
            inner: super::ApalisLazyRefreshClaimGuard,
            fail_reads: Arc<AtomicBool>,
        }

        #[async_trait::async_trait]
        impl super::LazyRefreshClaimGuard for ArmFailureAfterClaimGuard {
            async fn begin_refresh(
                &self,
                upstream_id: Uuid,
                holder: &str,
                expires_at_unix_secs: u64,
                now_unix_secs: u64,
            ) -> StorageResult<super::LazyRefreshClaim> {
                self.inner
                    .begin_refresh(upstream_id, holder, expires_at_unix_secs, now_unix_secs)
                    .await
            }

            async fn begin_refresh_for_generation(
                &self,
                upstream_id: Uuid,
                holder: &str,
                expires_at_unix_secs: u64,
                expected_generation: u64,
                now_unix_secs: u64,
            ) -> StorageResult<super::LazyRefreshClaim> {
                self.inner
                    .begin_refresh_for_generation(
                        upstream_id,
                        holder,
                        expires_at_unix_secs,
                        expected_generation,
                        now_unix_secs,
                    )
                    .await
            }

            async fn complete_and_bump_generation(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
                _generation: u64,
            ) -> StorageResult<bool> {
                Ok(false)
            }

            async fn release_if_holder(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
            ) -> StorageResult<bool> {
                Ok(false)
            }

            async fn task_state(
                &self,
                idempotency_key: &str,
            ) -> StorageResult<super::LazyRefreshTaskState> {
                self.inner.task_state(idempotency_key).await
            }

            async fn try_claim_fallback(
                &self,
                upstream_id: Uuid,
                holder: &str,
                idempotency_key: &str,
                generation: u64,
                now_unix_secs: u64,
            ) -> StorageResult<Option<super::LazyRefreshFallbackLease>> {
                let lease = self
                    .inner
                    .try_claim_fallback(
                        upstream_id,
                        holder,
                        idempotency_key,
                        generation,
                        now_unix_secs,
                    )
                    .await?;
                if lease.is_some() {
                    // Arm only after acquiring the fallback lease so earlier scheduler wait
                    // polls and the claim acquisition itself proceed normally.
                    self.fail_reads.store(true, Ordering::SeqCst);
                }
                Ok(lease)
            }

            async fn renew_fallback_claim(
                &self,
                upstream_id: Uuid,
                lease: &super::LazyRefreshFallbackLease,
                now_unix_secs: u64,
            ) -> StorageResult<Option<super::LazyRefreshFallbackLease>> {
                self.inner
                    .renew_fallback_claim(upstream_id, lease, now_unix_secs)
                    .await
            }

            fn fallback_lease_meta_key(&self, upstream_id: Uuid) -> Option<String> {
                self.inner.fallback_lease_meta_key(upstream_id)
            }

            async fn finish_fallback_claim(
                &self,
                upstream_id: Uuid,
                lease: &super::LazyRefreshFallbackLease,
                final_generation: u64,
            ) -> StorageResult<()> {
                self.inner
                    .finish_fallback_claim(upstream_id, lease, final_generation)
                    .await
            }
        }

        let (fixture, upstream_id, starting_gen) = setup_killed_oauth_job_fixture().await?;
        let lease_key = super::oauth_refresh_lease_meta_key(upstream_id);
        let fail_reads = Arc::new(AtomicBool::new(false));
        let failing_stores = Arc::new(crate::dynamic_view_builder::Stores {
            upstreams: Arc::new(FailPostClaimGenerationReadStore {
                inner: fixture.storage.clone(),
                fail_reads: Arc::clone(&fail_reads),
            }),
            vertex_state: Arc::new(crate::dynamic_view_builder::VertexState::new(
                fixture.storage.clone(),
            )),
            principals: fixture.storage.clone(),
            plugin_registry: fixture.storage.clone(),
            upstream_rate_limits: fixture.storage.clone(),
            upstream_subscription_quotas: fixture.storage.clone(),
            upstream_subscription_metadata: fixture.storage.clone(),
            organization_metadata: fixture.storage.clone(),
            plan_tiers: fixture.storage.clone(),
            prompt_cache_observations: fixture.storage.clone(),
            anthropic_compatibility_kv: fixture.storage.clone(),
            audit: Some(fixture.storage.clone()),
        });
        let refresher = super::LazyRefresher::new_with_claim_guard_for_tests(
            LazyRefresherDeps {
                google_config: Default::default(),
                stores: failing_stores,
                aead: fixture.aead.clone(),
                oauth_cfg: fixture.oauth_cfg.clone(),
                clock: fixture.clock.clone(),
            },
            Uuid::new_v4(),
            None,
            CancellationToken::new(),
            Arc::new(ArmFailureAfterClaimGuard {
                inner: super::ApalisLazyRefreshClaimGuard::new(
                    fixture.scheduler_backend.clone(),
                    fixture.storage.clone(),
                ),
                fail_reads,
            }),
            LazyRefreshContentionConfig::for_tests(
                Duration::from_secs(5),
                Duration::from_millis(5),
            ),
            fixture.scheduler_backend.clone(),
        );

        let err = refresher
            .refresh_one(upstream_id)
            .await
            .expect_err("post-claim generation read error must propagate to caller");
        assert!(matches!(
            err,
            super::LazyRefreshError::Failed { ref reason }
                if reason.contains("transient post-claim generation read failure")
        ));
        assert_eq!(
            fixture.storage.get_meta_value(&lease_key).await?.as_deref(),
            Some(format!("idle:{starting_gen}").as_str())
        );
        let healthy = super::ApalisLazyRefreshClaimGuard::new(
            fixture.scheduler_backend.clone(),
            fixture.storage.clone(),
        );
        let healthy_lease = healthy
            .try_claim_fallback(
                upstream_id,
                "healthy-worker-post-claim",
                "k-post-claim",
                starting_gen,
                cc_lb_clock::unix_secs(fixture.clock.now()),
            )
            .await?
            .ok_or("healthy worker must immediately acquire released fallback lease")?;
        healthy
            .finish_fallback_claim(upstream_id, &healthy_lease, starting_gen)
            .await?;
        assert_eq!(fixture.refresh_call_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn oauth_refresh_lease_serializes_ambiguous_cleanup_before_commit_visibility()
    -> Result<(), Box<dyn std::error::Error>> {
        type PendingMetaWrite = (String, Option<String>, String);

        struct DeferredCommitVisibilityMetaStore {
            inner: Arc<cc_lb_storage_sqlite::SqliteStorage>,
            pending_commit: std::sync::Mutex<Option<PendingMetaWrite>>,
            defer_next_put: AtomicBool,
            deferred_flushes: AtomicUsize,
        }

        impl DeferredCommitVisibilityMetaStore {
            async fn flush_pending(&self) -> StorageResult<()> {
                let pending = {
                    let mut guard = self
                        .pending_commit
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    guard.take()
                };
                if let Some((key, expected, value)) = pending {
                    self.deferred_flushes.fetch_add(1, Ordering::SeqCst);
                    let _ = self
                        .inner
                        .compare_and_put_meta_value(&key, expected.as_deref(), &value)
                        .await?;
                }
                Ok(())
            }
        }

        #[async_trait::async_trait]
        impl MetaStore for DeferredCommitVisibilityMetaStore {
            async fn initialize(&self) -> StorageResult<()> {
                self.inner.initialize().await
            }

            async fn backend_kind(&self) -> StorageResult<cc_lb_storage_api::BackendKind> {
                self.inner.backend_kind().await
            }

            async fn get_meta_value(&self, key: &str) -> StorageResult<Option<String>> {
                // Intentionally read without flushing `pending_commit` to model an
                // unlocked PostgreSQL SELECT observing pre-commit state while COMMIT finishes.
                self.inner.get_meta_value(key).await
            }

            async fn put_meta_value(&self, key: &str, value: &str) -> StorageResult<()> {
                self.flush_pending().await?;
                self.inner.put_meta_value(key, value).await
            }

            async fn compare_and_put_meta_value(
                &self,
                key: &str,
                expected: Option<&str>,
                value: &str,
            ) -> StorageResult<bool> {
                // Serialized behind the backend per-key transaction lock: any
                // in-flight COMMIT finishes before the next CAS executes.
                self.flush_pending().await?;
                if self.defer_next_put.swap(false, Ordering::SeqCst) {
                    let mut guard = self
                        .pending_commit
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    *guard = Some((
                        key.to_owned(),
                        expected.map(str::to_owned),
                        value.to_owned(),
                    ));
                    return Err(cc_lb_storage_api::StorageError::Transient {
                        retryable: true,
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::ConnectionReset,
                            "simulated connection drop before COMMIT visibility",
                        )),
                    });
                }
                self.inner
                    .compare_and_put_meta_value(key, expected, value)
                    .await
            }

            async fn verify_schema_current(&self) -> StorageResult<()> {
                self.inner.verify_schema_current().await
            }
        }

        // Sub-case A: `try_acquire_oauth_refresh_lease` receives a dropped-connection Err before
        // COMMIT becomes visible; `reconcile_exact_oauth_refresh_lease` issues the direct CAS,
        // serializing behind the pending commit and releasing it without an unlocked pre-read.
        let (fixture_a, upstream_a, gen_a) = setup_killed_oauth_job_fixture().await?;
        let lease_key_a = super::oauth_refresh_lease_meta_key(upstream_a);
        let deferred_a = Arc::new(DeferredCommitVisibilityMetaStore {
            inner: fixture_a.storage.clone(),
            pending_commit: std::sync::Mutex::new(None),
            defer_next_put: AtomicBool::new(true),
            deferred_flushes: AtomicUsize::new(0),
        });
        let now_a = cc_lb_clock::unix_secs(fixture_a.clock.now());
        let err_a =
            super::try_acquire_oauth_refresh_lease(deferred_a.as_ref(), upstream_a, gen_a, now_a)
                .await
                .expect_err("deferred ambiguous commit must return Err");
        assert!(matches!(
            err_a,
            cc_lb_storage_api::StorageError::Transient { .. }
        ));
        assert_eq!(deferred_a.deferred_flushes.load(Ordering::SeqCst), 1);
        assert!(
            deferred_a
                .pending_commit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none()
        );
        assert_eq!(
            fixture_a
                .storage
                .get_meta_value(&lease_key_a)
                .await?
                .as_deref(),
            Some(format!("idle:{gen_a}").as_str())
        );
        let healthy_lease_a = super::try_acquire_oauth_refresh_lease(
            fixture_a.storage.as_ref(),
            upstream_a,
            gen_a,
            now_a,
        )
        .await?
        .ok_or("healthy worker A must immediately acquire serialized cleaned lease")?;
        super::release_oauth_refresh_lease(
            fixture_a.storage.as_ref(),
            upstream_a,
            &healthy_lease_a.token,
            gen_a,
        )
        .await?;

        // Sub-case B: Outer `release_oauth_refresh_lease` serializes ahead of commit visibility
        // even while an unlocked `get_meta_value` still observes `None`.
        let (fixture_b, upstream_b, gen_b) = setup_killed_oauth_job_fixture().await?;
        let lease_key_b = super::oauth_refresh_lease_meta_key(upstream_b);
        let now_b = cc_lb_clock::unix_secs(fixture_b.clock.now());
        let holder_b = Uuid::new_v4().to_string();
        let token_b = super::oauth_refresh_lease_token(gen_b, &holder_b, now_b);
        let deferred_b = Arc::new(DeferredCommitVisibilityMetaStore {
            inner: fixture_b.storage.clone(),
            pending_commit: std::sync::Mutex::new(Some((
                lease_key_b.clone(),
                None,
                token_b.clone(),
            ))),
            defer_next_put: AtomicBool::new(false),
            deferred_flushes: AtomicUsize::new(0),
        });
        assert_eq!(deferred_b.get_meta_value(&lease_key_b).await?, None);
        super::release_oauth_refresh_lease(deferred_b.as_ref(), upstream_b, &token_b, gen_b)
            .await?;
        assert_eq!(deferred_b.deferred_flushes.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture_b
                .storage
                .get_meta_value(&lease_key_b)
                .await?
                .as_deref(),
            Some(format!("idle:{gen_b}").as_str())
        );

        // Sub-case C: Outer cleanup with a different caller token waits behind a foreign pending
        // commit without clobbering that foreign holder's lease.
        let (fixture_c, upstream_c, gen_c) = setup_killed_oauth_job_fixture().await?;
        let lease_key_c = super::oauth_refresh_lease_meta_key(upstream_c);
        let now_c = cc_lb_clock::unix_secs(fixture_c.clock.now());
        let other_token_c =
            super::oauth_refresh_lease_token(gen_c, &Uuid::new_v4().to_string(), now_c);
        let stale_caller_token_c =
            super::oauth_refresh_lease_token(gen_c, &Uuid::new_v4().to_string(), now_c);
        let deferred_c = Arc::new(DeferredCommitVisibilityMetaStore {
            inner: fixture_c.storage.clone(),
            pending_commit: std::sync::Mutex::new(Some((
                lease_key_c.clone(),
                None,
                other_token_c.clone(),
            ))),
            defer_next_put: AtomicBool::new(false),
            deferred_flushes: AtomicUsize::new(0),
        });
        super::release_oauth_refresh_lease(
            deferred_c.as_ref(),
            upstream_c,
            &stale_caller_token_c,
            gen_c,
        )
        .await?;
        assert_eq!(deferred_c.deferred_flushes.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture_c
                .storage
                .get_meta_value(&lease_key_c)
                .await?
                .as_deref(),
            Some(other_token_c.as_str())
        );
        Ok(())
    }

    #[tokio::test]
    async fn oauth_refresh_lease_recovers_after_lost_renewal_ack_while_exchange_in_flight()
    -> Result<(), Box<dyn std::error::Error>> {
        use tokio::sync::watch;

        struct LostRenewalAckMetaStore {
            inner: Arc<cc_lb_storage_sqlite::SqliteStorage>,
            initial_token: String,
            expected_recovered_token: String,
            fail_commit_and_probe_remaining: AtomicUsize,
            lost_ack: watch::Sender<bool>,
            recovered: watch::Sender<bool>,
        }

        #[async_trait::async_trait]
        impl MetaStore for LostRenewalAckMetaStore {
            async fn initialize(&self) -> StorageResult<()> {
                self.inner.initialize().await
            }

            async fn backend_kind(&self) -> StorageResult<cc_lb_storage_api::BackendKind> {
                self.inner.backend_kind().await
            }

            async fn get_meta_value(&self, key: &str) -> StorageResult<Option<String>> {
                self.inner.get_meta_value(key).await
            }

            async fn put_meta_value(&self, key: &str, value: &str) -> StorageResult<()> {
                self.inner.put_meta_value(key, value).await
            }

            async fn compare_and_put_meta_value(
                &self,
                key: &str,
                expected: Option<&str>,
                value: &str,
            ) -> StorageResult<bool> {
                if expected == Some(value)
                    && self.fail_commit_and_probe_remaining.load(Ordering::SeqCst) > 0
                {
                    self.fail_commit_and_probe_remaining
                        .fetch_sub(1, Ordering::SeqCst);
                    return Err(cc_lb_storage_api::StorageError::Transient {
                        retryable: true,
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::ConnectionReset,
                            "simulated lost renewal probe ack",
                        )),
                    });
                }
                let applied = self
                    .inner
                    .compare_and_put_meta_value(key, expected, value)
                    .await?;
                if applied
                    && expected == Some(self.initial_token.as_str())
                    && value != self.initial_token
                    && self.fail_commit_and_probe_remaining.load(Ordering::SeqCst) > 0
                {
                    self.fail_commit_and_probe_remaining
                        .fetch_sub(1, Ordering::SeqCst);
                    self.lost_ack.send_replace(true);
                    return Err(cc_lb_storage_api::StorageError::Transient {
                        retryable: true,
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::ConnectionReset,
                            "simulated lost renewal COMMIT ack",
                        )),
                    });
                }
                if applied && value == self.expected_recovered_token {
                    self.recovered.send_replace(true);
                }
                Ok(applied)
            }

            async fn verify_schema_current(&self) -> StorageResult<()> {
                self.inner.verify_schema_current().await
            }
        }

        // Sub-case 1: Live `refresh_flow` exchange stays in flight while the heartbeat renewal
        // commits to SQLite and loses both its initial ack and immediate probe; subsequent
        // heartbeat reconciles stored holder + expiry instead of cancelling the live exchange.
        let fixture1 = LazyRefreshFixture::new(Duration::ZERO).await;
        fixture1.hold_refresh_response();
        let upstream1 = fixture1.create_oauth_upstream().await;
        let record1 = UpstreamStore::get_by_id(fixture1.storage.as_ref(), upstream1)
            .await?
            .ok_or("missing upstream1")?;
        let gen1 = record1.oauth_token_generation;
        let lease_key1 = super::oauth_refresh_lease_meta_key(upstream1);
        let test_clock1 = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let initial_now1 = cc_lb_clock::unix_secs(test_clock1.now());
        let initial_lease1 = super::try_acquire_oauth_refresh_lease(
            fixture1.storage.as_ref(),
            upstream1,
            gen1,
            initial_now1,
        )
        .await?
        .ok_or("initial lease 1 must succeed")?;
        let holder1 = match super::parse_oauth_refresh_lease(&initial_lease1.token) {
            Some(super::ParsedOAuthRefreshLease::Held { holder, .. }) => holder,
            _ => return Err("expected held initial lease 1".into()),
        };
        let expected_recovered1 =
            super::oauth_refresh_lease_token(gen1, &holder1, initial_now1 + 50);
        let (lost_ack_tx1, mut lost_ack_rx1) = watch::channel(false);
        let (recovered_tx1, mut recovered_rx1) = watch::channel(false);
        let lost_meta1 = Arc::new(LostRenewalAckMetaStore {
            inner: fixture1.storage.clone(),
            initial_token: initial_lease1.token.clone(),
            expected_recovered_token: expected_recovered1.clone(),
            fail_commit_and_probe_remaining: AtomicUsize::new(2),
            lost_ack: lost_ack_tx1,
            recovered: recovered_tx1,
        });
        let parent_cancel1 = CancellationToken::new();
        let mut active1 = super::ActiveOAuthRefreshLease::start_with_interval(
            lost_meta1,
            test_clock1.clone(),
            parent_cancel1.clone(),
            initial_lease1.clone(),
            gen1,
            Duration::from_millis(20),
        );
        let lease_cancel1 = active1.cancel_token();
        let stores1 = fixture1.stores.clone();
        let aead1 = fixture1.aead.clone();
        let oauth_cfg1 = fixture1.oauth_cfg.clone();
        let clock1_for_flow: ClockHandle = test_clock1.clone();
        let flow_cancel1 = lease_cancel1.clone();
        let refresh_handle1 = tokio::spawn(async move {
            let outcome = super::refresh_flow(
                &stores1,
                stores1.audit.as_deref(),
                &aead1,
                &oauth_cfg1,
                &cc_lb_config::Config::default(),
                Uuid::new_v4(),
                &super::TokenHttpClient::new(Duration::from_secs(30)),
                &flow_cancel1,
                record1,
                clock1_for_flow.as_ref(),
                Some(&mut active1),
                Some(gen1),
            )
            .await;
            (outcome, active1)
        });
        fixture1.wait_for_refresh_start().await;
        test_clock1.advance_secs(25);
        tokio::time::timeout(Duration::from_secs(2), lost_ack_rx1.wait_for(|&lost| lost)).await??;
        let expected_first_renew1 =
            super::oauth_refresh_lease_token(gen1, &holder1, initial_now1 + 25);
        assert_eq!(
            fixture1
                .storage
                .get_meta_value(&lease_key1)
                .await?
                .as_deref(),
            Some(expected_first_renew1.as_str())
        );
        assert!(!lease_cancel1.is_cancelled());

        test_clock1.advance_secs(25);
        tokio::time::timeout(Duration::from_secs(2), recovered_rx1.wait_for(|&done| done))
            .await??;
        assert!(!lease_cancel1.is_cancelled());
        assert_eq!(
            fixture1
                .storage
                .get_meta_value(&lease_key1)
                .await?
                .as_deref(),
            Some(expected_recovered1.as_str())
        );

        fixture1.release_refresh_response();
        let (flow_outcome1, active1) =
            tokio::time::timeout(Duration::from_secs(2), refresh_handle1).await??;
        let refreshed1 = flow_outcome1?;
        assert_eq!(refreshed1.generation, gen1 + 1);
        assert!(!active1.is_lost().await);
        active1.release(refreshed1.generation).await?;
        assert_eq!(
            fixture1
                .storage
                .get_meta_value(&lease_key1)
                .await?
                .as_deref(),
            Some(format!("idle:{}", gen1 + 1).as_str())
        );

        // Sub-case 2: Immediate serialized probe confirms an ambiguous renewal even when
        // the write finished after a dropped COMMIT acknowledgement.
        struct DeferredRenewMetaStore {
            inner: Arc<cc_lb_storage_sqlite::SqliteStorage>,
            pending: std::sync::Mutex<Option<(String, Option<String>, String)>>,
            defer_next_put: AtomicBool,
        }

        #[async_trait::async_trait]
        impl MetaStore for DeferredRenewMetaStore {
            async fn initialize(&self) -> StorageResult<()> {
                self.inner.initialize().await
            }

            async fn backend_kind(&self) -> StorageResult<cc_lb_storage_api::BackendKind> {
                self.inner.backend_kind().await
            }

            async fn get_meta_value(&self, key: &str) -> StorageResult<Option<String>> {
                self.inner.get_meta_value(key).await
            }

            async fn put_meta_value(&self, key: &str, value: &str) -> StorageResult<()> {
                self.inner.put_meta_value(key, value).await
            }

            async fn compare_and_put_meta_value(
                &self,
                key: &str,
                expected: Option<&str>,
                value: &str,
            ) -> StorageResult<bool> {
                let pending = {
                    let mut guard = self
                        .pending
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    guard.take()
                };
                if let Some((p_key, p_expected, p_value)) = pending {
                    let _ = self
                        .inner
                        .compare_and_put_meta_value(&p_key, p_expected.as_deref(), &p_value)
                        .await?;
                }
                if self.defer_next_put.swap(false, Ordering::SeqCst) {
                    let mut guard = self
                        .pending
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    *guard = Some((
                        key.to_owned(),
                        expected.map(str::to_owned),
                        value.to_owned(),
                    ));
                    return Err(cc_lb_storage_api::StorageError::Transient {
                        retryable: true,
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::ConnectionReset,
                            "simulated lost renewal ack before visibility",
                        )),
                    });
                }
                self.inner
                    .compare_and_put_meta_value(key, expected, value)
                    .await
            }

            async fn verify_schema_current(&self) -> StorageResult<()> {
                self.inner.verify_schema_current().await
            }
        }

        let (fixture2, upstream2, gen2) = setup_killed_oauth_job_fixture().await?;
        let lease_key2 = super::oauth_refresh_lease_meta_key(upstream2);
        let now2 = cc_lb_clock::unix_secs(fixture2.clock.now());
        let lease2 = super::try_acquire_oauth_refresh_lease(
            fixture2.storage.as_ref(),
            upstream2,
            gen2,
            now2,
        )
        .await?
        .ok_or("lease 2 must succeed")?;
        let holder2 = match super::parse_oauth_refresh_lease(&lease2.token) {
            Some(super::ParsedOAuthRefreshLease::Held { holder, .. }) => holder,
            _ => return Err("expected held lease 2".into()),
        };
        let deferred2 = Arc::new(DeferredRenewMetaStore {
            inner: fixture2.storage.clone(),
            pending: std::sync::Mutex::new(None),
            defer_next_put: AtomicBool::new(true),
        });
        let renewed2 = super::renew_oauth_refresh_lease(
            deferred2.as_ref(),
            upstream2,
            &lease2.token,
            now2 + 25,
        )
        .await?
        .ok_or("serialized probe must confirm deferred renewal commit")?;
        let expected_renew2 = super::oauth_refresh_lease_token(gen2, &holder2, now2 + 25);
        assert_eq!(renewed2, expected_renew2);
        assert_eq!(
            fixture2
                .storage
                .get_meta_value(&lease_key2)
                .await?
                .as_deref(),
            Some(expected_renew2.as_str())
        );

        // Sub-case 3: Direct mismatch with stale `lease2.token` against SQLite's `expected_renew2`
        // reconciles to `now2 + 50`; foreign holder and expired own lease fail closed (`None`).
        let reconciled3 = super::renew_oauth_refresh_lease(
            fixture2.storage.as_ref(),
            upstream2,
            &lease2.token,
            now2 + 50,
        )
        .await?
        .ok_or("same-holder mismatch must reconcile and extend")?;
        let expected_reconciled3 = super::oauth_refresh_lease_token(gen2, &holder2, now2 + 50);
        assert_eq!(reconciled3, expected_reconciled3);

        let foreign_token = super::oauth_refresh_lease_token(gen2, "foreign-holder", now2 + 60);
        fixture2
            .storage
            .put_meta_value(&lease_key2, &foreign_token)
            .await?;
        assert_eq!(
            super::renew_oauth_refresh_lease(
                fixture2.storage.as_ref(),
                upstream2,
                &lease2.token,
                now2 + 60,
            )
            .await?,
            None
        );

        fixture2
            .storage
            .put_meta_value(&lease_key2, &expected_renew2)
            .await?;
        assert_eq!(
            super::renew_oauth_refresh_lease(
                fixture2.storage.as_ref(),
                upstream2,
                &lease2.token,
                now2 + 120,
            )
            .await?,
            None
        );
        Ok(())
    }

    #[tokio::test]
    async fn oauth_refresh_lease_releases_committed_pending_renewal_when_expiry_cancels_heartbeat()
    -> Result<(), Box<dyn std::error::Error>> {
        use tokio::sync::watch;

        type PendingMetaWrite = (String, Option<String>, String);

        struct PendingRenewCommitMetaStore {
            inner: Arc<cc_lb_storage_sqlite::SqliteStorage>,
            defer_visibility: bool,
            pending: std::sync::Mutex<Option<PendingMetaWrite>>,
            deferred_flushes: AtomicUsize,
            hang_renew_calls: AtomicBool,
            renew_committed: watch::Sender<bool>,
            released: watch::Sender<bool>,
        }

        impl PendingRenewCommitMetaStore {
            async fn flush_pending(&self) -> StorageResult<()> {
                let pending = {
                    let mut guard = self
                        .pending
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    guard.take()
                };
                if let Some((key, expected, value)) = pending {
                    self.deferred_flushes.fetch_add(1, Ordering::SeqCst);
                    let _ = self
                        .inner
                        .compare_and_put_meta_value(&key, expected.as_deref(), &value)
                        .await?;
                }
                Ok(())
            }
        }

        #[async_trait::async_trait]
        impl MetaStore for PendingRenewCommitMetaStore {
            async fn initialize(&self) -> StorageResult<()> {
                self.inner.initialize().await
            }

            async fn backend_kind(&self) -> StorageResult<cc_lb_storage_api::BackendKind> {
                self.inner.backend_kind().await
            }

            async fn get_meta_value(&self, key: &str) -> StorageResult<Option<String>> {
                self.inner.get_meta_value(key).await
            }

            async fn put_meta_value(&self, key: &str, value: &str) -> StorageResult<()> {
                self.flush_pending().await?;
                self.inner.put_meta_value(key, value).await
            }

            async fn compare_and_put_meta_value(
                &self,
                key: &str,
                expected: Option<&str>,
                value: &str,
            ) -> StorageResult<bool> {
                self.flush_pending().await?;
                if self.hang_renew_calls.load(Ordering::SeqCst) && value.starts_with("held:") {
                    if self.defer_visibility {
                        let mut guard = self
                            .pending
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        *guard = Some((
                            key.to_owned(),
                            expected.map(str::to_owned),
                            value.to_owned(),
                        ));
                    } else {
                        let applied = self
                            .inner
                            .compare_and_put_meta_value(key, expected, value)
                            .await?;
                        assert!(applied, "pending renewal CAS commit must succeed");
                    }
                    self.renew_committed.send_replace(true);
                    std::future::pending::<()>().await;
                    return Ok(true);
                }
                let applied = self
                    .inner
                    .compare_and_put_meta_value(key, expected, value)
                    .await?;
                if applied && value.starts_with("idle:") {
                    self.released.send_replace(true);
                }
                Ok(applied)
            }

            async fn verify_schema_current(&self) -> StorageResult<()> {
                self.inner.verify_schema_current().await
            }
        }

        async fn start_pending_renewal_case(
            defer_visibility: bool,
        ) -> Result<
            (
                LazyRefreshFixture,
                Uuid,
                u64,
                String,
                super::OAuthRefreshLease,
                String,
                Arc<PendingRenewCommitMetaStore>,
                watch::Receiver<bool>,
                watch::Receiver<bool>,
                super::ActiveOAuthRefreshLease,
                CancellationToken,
            ),
            Box<dyn std::error::Error>,
        > {
            let (fixture, upstream_id, starting_gen) = setup_killed_oauth_job_fixture().await?;
            let lease_key = super::oauth_refresh_lease_meta_key(upstream_id);
            let test_clock = Arc::new(TestClock::new_at_secs(1_700_000_000));
            let initial_now = cc_lb_clock::unix_secs(test_clock.now());
            let initial_lease = super::try_acquire_oauth_refresh_lease(
                fixture.storage.as_ref(),
                upstream_id,
                starting_gen,
                initial_now,
            )
            .await?
            .ok_or("initial lease must succeed")?;
            let holder = match super::parse_oauth_refresh_lease(&initial_lease.token) {
                Some(super::ParsedOAuthRefreshLease::Held { holder, .. }) => holder,
                _ => return Err("expected held initial lease".into()),
            };
            test_clock.advance_secs(25);
            let renewed_token =
                super::oauth_refresh_lease_token(starting_gen, &holder, initial_now + 25);
            let (renew_committed_tx, renew_committed_rx) = watch::channel(false);
            let (released_tx, released_rx) = watch::channel(false);
            let meta = Arc::new(PendingRenewCommitMetaStore {
                inner: fixture.storage.clone(),
                defer_visibility,
                pending: std::sync::Mutex::new(None),
                deferred_flushes: AtomicUsize::new(0),
                hang_renew_calls: AtomicBool::new(true),
                renew_committed: renew_committed_tx,
                released: released_tx,
            });
            let parent_cancel = CancellationToken::new();
            let active = super::ActiveOAuthRefreshLease::start_with_controller_for_tests(
                Arc::new(super::MetaStoreLeaseController(meta.clone())),
                test_clock,
                parent_cancel,
                upstream_id,
                initial_lease.token.clone(),
                starting_gen,
                Duration::from_millis(10),
                Duration::from_millis(300),
                Duration::ZERO,
            );
            let lease_cancel = active.cancel_token();
            Ok((
                fixture,
                upstream_id,
                starting_gen,
                lease_key,
                initial_lease,
                renewed_token,
                meta,
                renew_committed_rx,
                released_rx,
                active,
                lease_cancel,
            ))
        }

        // Sub-case A: Pending renewal commits newer expiry to SQLite and remains unacknowledged
        // until `expiry_sleep` marks `Lost`. `release()` retains the holder token and clears
        // the renewed lease back to `idle:{gen}` immediately.
        let (
            fixture_a,
            upstream_a,
            gen_a,
            lease_key_a,
            _init_a,
            renewed_token_a,
            meta_a,
            mut renew_committed_rx_a,
            _released_rx_a,
            active_a,
            lease_cancel_a,
        ) = start_pending_renewal_case(false).await?;
        tokio::time::timeout(
            Duration::from_secs(2),
            renew_committed_rx_a.wait_for(|&committed| committed),
        )
        .await??;
        assert_eq!(
            fixture_a
                .storage
                .get_meta_value(&lease_key_a)
                .await?
                .as_deref(),
            Some(renewed_token_a.as_str())
        );
        tokio::time::timeout(Duration::from_secs(2), lease_cancel_a.cancelled())
            .await
            .map_err(|_| "expiry deadline must cancel active lease")?;
        assert!(active_a.is_lost().await);
        assert!(active_a.fence().await.is_err());
        meta_a.hang_renew_calls.store(false, Ordering::SeqCst);
        active_a.release(gen_a).await?;
        assert_eq!(
            fixture_a
                .storage
                .get_meta_value(&lease_key_a)
                .await?
                .as_deref(),
            Some(format!("idle:{gen_a}").as_str())
        );
        let healthy_lease_a = super::try_acquire_oauth_refresh_lease(
            fixture_a.storage.as_ref(),
            upstream_a,
            gen_a,
            1_700_000_050,
        )
        .await?
        .ok_or("healthy worker A must immediately acquire cleared lease")?;
        super::release_oauth_refresh_lease(
            fixture_a.storage.as_ref(),
            upstream_a,
            &healthy_lease_a.token,
            gen_a,
        )
        .await?;

        // Sub-case B: Releasing a Lost lease with retained holder identity still refuses to
        // overwrite a competitor's active claim.
        let (
            fixture_b,
            _upstream_b,
            gen_b,
            lease_key_b,
            _init_b,
            _renewed_token_b,
            meta_b,
            mut renew_committed_rx_b,
            _released_rx_b,
            active_b,
            lease_cancel_b,
        ) = start_pending_renewal_case(false).await?;
        tokio::time::timeout(
            Duration::from_secs(2),
            renew_committed_rx_b.wait_for(|&committed| committed),
        )
        .await??;
        tokio::time::timeout(Duration::from_secs(2), lease_cancel_b.cancelled()).await?;
        let competitor_b = super::oauth_refresh_lease_token(gen_b, "competitor", 1_700_000_060);
        fixture_b
            .storage
            .put_meta_value(&lease_key_b, &competitor_b)
            .await?;
        meta_b.hang_renew_calls.store(false, Ordering::SeqCst);
        active_b.release(gen_b).await?;
        assert_eq!(
            fixture_b
                .storage
                .get_meta_value(&lease_key_b)
                .await?
                .as_deref(),
            Some(competitor_b.as_str())
        );

        // Sub-case C: Deferred commit visibility on the pending renewal (`get_meta_value` still
        // sees `initial_token`) serializes cleanly via exact-token `compare_and_put_meta_value`.
        let (
            fixture_c,
            _upstream_c,
            gen_c,
            lease_key_c,
            init_c,
            _renewed_token_c,
            meta_c,
            mut renew_committed_rx_c,
            _released_rx_c,
            active_c,
            lease_cancel_c,
        ) = start_pending_renewal_case(true).await?;
        tokio::time::timeout(
            Duration::from_secs(2),
            renew_committed_rx_c.wait_for(|&committed| committed),
        )
        .await??;
        assert_eq!(
            fixture_c
                .storage
                .get_meta_value(&lease_key_c)
                .await?
                .as_deref(),
            Some(init_c.token.as_str())
        );
        tokio::time::timeout(Duration::from_secs(2), lease_cancel_c.cancelled()).await?;
        meta_c.hang_renew_calls.store(false, Ordering::SeqCst);
        active_c.release(gen_c).await?;
        assert_eq!(meta_c.deferred_flushes.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture_c
                .storage
                .get_meta_value(&lease_key_c)
                .await?
                .as_deref(),
            Some(format!("idle:{gen_c}").as_str())
        );

        // Sub-case D: Dropping `ActiveOAuthRefreshLease` (without `.release()`) spawns background
        // release of any held or pending-renewed lease so worker timeouts do not leak leases.
        let (
            fixture_d,
            upstream_d,
            gen_d,
            lease_key_d,
            _init_d,
            _renewed_token_d,
            meta_d,
            mut renew_committed_rx_d,
            mut released_rx_d,
            active_d,
            _lease_cancel_d,
        ) = start_pending_renewal_case(false).await?;
        tokio::time::timeout(
            Duration::from_secs(2),
            renew_committed_rx_d.wait_for(|&committed| committed),
        )
        .await??;
        meta_d.hang_renew_calls.store(false, Ordering::SeqCst);
        drop(active_d);
        tokio::time::timeout(
            Duration::from_secs(2),
            released_rx_d.wait_for(|&released| released),
        )
        .await??;
        assert_eq!(
            fixture_d
                .storage
                .get_meta_value(&lease_key_d)
                .await?
                .as_deref(),
            Some(format!("idle:{gen_d}").as_str())
        );
        let healthy_lease_d = super::try_acquire_oauth_refresh_lease(
            fixture_d.storage.as_ref(),
            upstream_d,
            gen_d,
            1_700_000_050,
        )
        .await?
        .ok_or("healthy worker D must immediately acquire dropped lease")?;
        super::release_oauth_refresh_lease(
            fixture_d.storage.as_ref(),
            upstream_d,
            &healthy_lease_d.token,
            gen_d,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn scheduled_oauth_refresh_lease_acquire_releases_committed_claim_on_worker_timeout()
    -> Result<(), Box<dyn std::error::Error>> {
        use tokio::sync::watch;

        struct DelayedScheduledAcquireMetaStore {
            inner: Arc<cc_lb_storage_sqlite::SqliteStorage>,
            delay_held: AtomicBool,
            committed: watch::Sender<bool>,
            allow_return: watch::Receiver<bool>,
            released: watch::Sender<bool>,
        }

        #[async_trait::async_trait]
        impl MetaStore for DelayedScheduledAcquireMetaStore {
            async fn initialize(&self) -> StorageResult<()> {
                self.inner.initialize().await
            }

            async fn backend_kind(&self) -> StorageResult<cc_lb_storage_api::BackendKind> {
                self.inner.backend_kind().await
            }

            async fn get_meta_value(&self, key: &str) -> StorageResult<Option<String>> {
                self.inner.get_meta_value(key).await
            }

            async fn put_meta_value(&self, key: &str, value: &str) -> StorageResult<()> {
                self.inner.put_meta_value(key, value).await
            }

            async fn compare_and_put_meta_value(
                &self,
                key: &str,
                expected: Option<&str>,
                value: &str,
            ) -> StorageResult<bool> {
                let applied = self
                    .inner
                    .compare_and_put_meta_value(key, expected, value)
                    .await?;
                if applied && value.starts_with("idle:") {
                    self.released.send_replace(true);
                }
                if applied && value.starts_with("held:") && self.delay_held.load(Ordering::SeqCst) {
                    self.committed.send_replace(true);
                    let mut allow_return = self.allow_return.clone();
                    let _ = allow_return.wait_for(|&go| go).await;
                }
                Ok(applied)
            }

            async fn verify_schema_current(&self) -> StorageResult<()> {
                self.inner.verify_schema_current().await
            }
        }

        // Case 1: Worker timeout cancels `try_acquire_oauth_refresh_lease_cancellation_safe`
        // after SQLite commits `held:...` and before the lease is returned to the caller.
        let (fixture1, upstream1, gen1) = setup_killed_oauth_job_fixture().await?;
        let lease_key1 = super::oauth_refresh_lease_meta_key(upstream1);
        let now1 = cc_lb_clock::unix_secs(fixture1.clock.now());
        let (committed_tx1, mut committed_rx1) = watch::channel(false);
        let (allow_tx1, allow_rx1) = watch::channel(false);
        let (released_tx1, mut released_rx1) = watch::channel(false);
        let delayed_meta1 = Arc::new(DelayedScheduledAcquireMetaStore {
            inner: fixture1.storage.clone(),
            delay_held: AtomicBool::new(true),
            committed: committed_tx1,
            allow_return: allow_rx1,
            released: released_tx1,
        });
        tokio::select! {
            biased;
            res = super::try_acquire_oauth_refresh_lease_cancellation_safe(
                delayed_meta1.clone(),
                fixture1.storage.clone(),
                upstream1,
                gen1,
                now1,
            ) => {
                panic!("paused scheduled lease acquisition should not complete early: {res:?}");
            }
            wait_res = tokio::time::timeout(
                Duration::from_secs(2),
                committed_rx1.wait_for(|&committed| committed),
            ) => {
                wait_res??;
            }
        }
        let held1 = fixture1
            .storage
            .get_meta_value(&lease_key1)
            .await?
            .ok_or("missing committed scheduled lease 1")?;
        assert!(held1.starts_with(&format!("held:{gen1}:")));
        allow_tx1.send_replace(true);
        tokio::time::timeout(
            Duration::from_secs(2),
            released_rx1.wait_for(|&released| released),
        )
        .await??;
        assert_eq!(
            fixture1
                .storage
                .get_meta_value(&lease_key1)
                .await?
                .as_deref(),
            Some(format!("idle:{gen1}").as_str())
        );
        delayed_meta1.delay_held.store(false, Ordering::SeqCst);
        let healthy_lease1 = super::try_acquire_oauth_refresh_lease_cancellation_safe(
            delayed_meta1,
            fixture1.storage.clone(),
            upstream1,
            gen1,
            now1,
        )
        .await?
        .ok_or("healthy worker 1 must immediately acquire released scheduled lease")?;
        super::release_oauth_refresh_lease(
            fixture1.storage.as_ref(),
            upstream1,
            &healthy_lease1.token,
            gen1,
        )
        .await?;

        // Case 2: Future dropped after `Delivered(lease)` + oneshot readiness before waiter polls.
        struct NotifyWake(Arc<Notify>);
        impl std::task::Wake for NotifyWake {
            fn wake(self: Arc<Self>) {
                self.0.notify_one();
            }
        }

        let (fixture2, upstream2, gen2) = setup_killed_oauth_job_fixture().await?;
        let lease_key2 = super::oauth_refresh_lease_meta_key(upstream2);
        let now2 = cc_lb_clock::unix_secs(fixture2.clock.now());
        let (committed_tx2, mut committed_rx2) = watch::channel(false);
        let (allow_tx2, allow_rx2) = watch::channel(false);
        let (released_tx2, mut released_rx2) = watch::channel(false);
        let delayed_meta2 = Arc::new(DelayedScheduledAcquireMetaStore {
            inner: fixture2.storage.clone(),
            delay_held: AtomicBool::new(true),
            committed: committed_tx2,
            allow_return: allow_rx2,
            released: released_tx2,
        });
        let notify2 = Arc::new(Notify::new());
        let waker2 = std::task::Waker::from(Arc::new(NotifyWake(Arc::clone(&notify2))));
        let mut cx2 = std::task::Context::from_waker(&waker2);
        let mut acquire_fut2 = Box::pin(super::try_acquire_oauth_refresh_lease_cancellation_safe(
            delayed_meta2.clone(),
            fixture2.storage.clone(),
            upstream2,
            gen2,
            now2,
        ));
        assert!(acquire_fut2.as_mut().poll(&mut cx2).is_pending());
        tokio::time::timeout(
            Duration::from_secs(2),
            committed_rx2.wait_for(|&committed| committed),
        )
        .await??;
        allow_tx2.send_replace(true);
        tokio::time::timeout(Duration::from_secs(2), notify2.notified()).await?;
        drop(acquire_fut2);
        tokio::time::timeout(
            Duration::from_secs(2),
            released_rx2.wait_for(|&released| released),
        )
        .await??;
        assert_eq!(
            fixture2
                .storage
                .get_meta_value(&lease_key2)
                .await?
                .as_deref(),
            Some(format!("idle:{gen2}").as_str())
        );
        delayed_meta2.delay_held.store(false, Ordering::SeqCst);
        let healthy_lease2 = super::try_acquire_oauth_refresh_lease_cancellation_safe(
            delayed_meta2,
            fixture2.storage.clone(),
            upstream2,
            gen2,
            now2,
        )
        .await?
        .ok_or("healthy worker 2 must immediately acquire released scheduled lease")?;
        super::release_oauth_refresh_lease(
            fixture2.storage.as_ref(),
            upstream2,
            &healthy_lease2.token,
            gen2,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn active_oauth_refresh_lease_release_survives_caller_cancellation()
    -> Result<(), Box<dyn std::error::Error>> {
        use tokio::sync::watch;

        struct DelayedReleaseMetaStore {
            inner: Arc<cc_lb_storage_sqlite::SqliteStorage>,
            release_on_idle: AtomicBool,
            release_entered: watch::Sender<bool>,
            allow_release: watch::Receiver<bool>,
            release_finished: watch::Sender<bool>,
        }

        #[async_trait::async_trait]
        impl MetaStore for DelayedReleaseMetaStore {
            async fn initialize(&self) -> StorageResult<()> {
                self.inner.initialize().await
            }

            async fn backend_kind(&self) -> StorageResult<cc_lb_storage_api::BackendKind> {
                self.inner.backend_kind().await
            }

            async fn get_meta_value(&self, key: &str) -> StorageResult<Option<String>> {
                self.inner.get_meta_value(key).await
            }

            async fn put_meta_value(&self, key: &str, value: &str) -> StorageResult<()> {
                self.inner.put_meta_value(key, value).await
            }

            async fn compare_and_put_meta_value(
                &self,
                key: &str,
                expected: Option<&str>,
                value: &str,
            ) -> StorageResult<bool> {
                if self.release_on_idle.load(Ordering::SeqCst) && value.starts_with("idle:") {
                    self.release_entered.send_replace(true);
                    let mut allow = self.allow_release.clone();
                    let _ = allow.wait_for(|&go| go).await;
                    let applied = self
                        .inner
                        .compare_and_put_meta_value(key, expected, value)
                        .await?;
                    self.release_finished.send_replace(true);
                    return Ok(applied);
                }
                self.inner
                    .compare_and_put_meta_value(key, expected, value)
                    .await
            }

            async fn verify_schema_current(&self) -> StorageResult<()> {
                self.inner.verify_schema_current().await
            }
        }

        // Case 1: Caller cancels while explicit `release()` awaits conditional CAS write.
        let (fixture1, upstream1, gen1) = setup_killed_oauth_job_fixture().await?;
        let lease_key1 = super::oauth_refresh_lease_meta_key(upstream1);
        let now1 = cc_lb_clock::unix_secs(fixture1.clock.now());
        let lease1 = super::try_acquire_oauth_refresh_lease(
            fixture1.storage.as_ref(),
            upstream1,
            gen1,
            now1,
        )
        .await?
        .ok_or("lease 1 must succeed")?;
        let (entered_tx1, mut entered_rx1) = watch::channel(false);
        let (allow_tx1, allow_rx1) = watch::channel(false);
        let (finished_tx1, mut finished_rx1) = watch::channel(false);
        let delayed1 = Arc::new(DelayedReleaseMetaStore {
            inner: fixture1.storage.clone(),
            release_on_idle: AtomicBool::new(true),
            release_entered: entered_tx1,
            allow_release: allow_rx1,
            release_finished: finished_tx1,
        });
        let active1 = super::ActiveOAuthRefreshLease::start_with_interval(
            delayed1,
            fixture1.clock.clone(),
            CancellationToken::new(),
            lease1.clone(),
            gen1,
            Duration::from_secs(60),
        );
        tokio::select! {
            biased;
            res = active1.release(gen1) => {
                panic!("paused explicit release should not finish before allow_release: {res:?}");
            }
            wait_res = tokio::time::timeout(
                Duration::from_secs(2),
                entered_rx1.wait_for(|&entered| entered),
            ) => {
                wait_res??;
            }
        }
        assert_eq!(
            fixture1
                .storage
                .get_meta_value(&lease_key1)
                .await?
                .as_deref(),
            Some(lease1.token.as_str())
        );
        allow_tx1.send_replace(true);
        tokio::time::timeout(Duration::from_secs(2), finished_rx1.wait_for(|&done| done)).await??;
        assert_eq!(
            fixture1
                .storage
                .get_meta_value(&lease_key1)
                .await?
                .as_deref(),
            Some(format!("idle:{gen1}").as_str())
        );
        let healthy1 = super::try_acquire_oauth_refresh_lease(
            fixture1.storage.as_ref(),
            upstream1,
            gen1,
            now1,
        )
        .await?
        .ok_or("healthy worker 1 must immediately acquire released lease")?;
        super::release_oauth_refresh_lease(
            fixture1.storage.as_ref(),
            upstream1,
            &healthy1.token,
            gen1,
        )
        .await?;

        // Case 2: Explicit `release()` future is dropped right after its first poll while
        // joining heartbeat / before `release_finished`; detached task still finishes cleanup.
        struct NotifyWake(Arc<Notify>);
        impl std::task::Wake for NotifyWake {
            fn wake(self: Arc<Self>) {
                self.0.notify_one();
            }
        }

        let (fixture2, upstream2, gen2) = setup_killed_oauth_job_fixture().await?;
        let lease_key2 = super::oauth_refresh_lease_meta_key(upstream2);
        let now2 = cc_lb_clock::unix_secs(fixture2.clock.now());
        let lease2 = super::try_acquire_oauth_refresh_lease(
            fixture2.storage.as_ref(),
            upstream2,
            gen2,
            now2,
        )
        .await?
        .ok_or("lease 2 must succeed")?;
        let (entered_tx2, mut entered_rx2) = watch::channel(false);
        let (allow_tx2, allow_rx2) = watch::channel(false);
        let (finished_tx2, mut finished_rx2) = watch::channel(false);
        let delayed2 = Arc::new(DelayedReleaseMetaStore {
            inner: fixture2.storage.clone(),
            release_on_idle: AtomicBool::new(true),
            release_entered: entered_tx2,
            allow_release: allow_rx2,
            release_finished: finished_tx2,
        });
        let active2 = super::ActiveOAuthRefreshLease::start_with_interval(
            delayed2,
            fixture2.clock.clone(),
            CancellationToken::new(),
            lease2.clone(),
            gen2,
            Duration::from_secs(60),
        );
        let notify2 = Arc::new(Notify::new());
        let waker2 = std::task::Waker::from(Arc::new(NotifyWake(Arc::clone(&notify2))));
        let mut cx2 = std::task::Context::from_waker(&waker2);
        let mut rel_fut2 = Box::pin(active2.release(gen2));
        assert!(rel_fut2.as_mut().poll(&mut cx2).is_pending());
        tokio::time::timeout(
            Duration::from_secs(2),
            entered_rx2.wait_for(|&entered| entered),
        )
        .await??;
        drop(rel_fut2);
        assert_eq!(
            fixture2
                .storage
                .get_meta_value(&lease_key2)
                .await?
                .as_deref(),
            Some(lease2.token.as_str())
        );
        allow_tx2.send_replace(true);
        tokio::time::timeout(Duration::from_secs(2), finished_rx2.wait_for(|&done| done)).await??;
        assert_eq!(
            fixture2
                .storage
                .get_meta_value(&lease_key2)
                .await?
                .as_deref(),
            Some(format!("idle:{gen2}").as_str())
        );
        let healthy2 = super::try_acquire_oauth_refresh_lease(
            fixture2.storage.as_ref(),
            upstream2,
            gen2,
            now2,
        )
        .await?
        .ok_or("healthy worker 2 must immediately acquire released lease")?;
        super::release_oauth_refresh_lease(
            fixture2.storage.as_ref(),
            upstream2,
            &healthy2.token,
            gen2,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn active_oauth_refresh_lease_drop_respects_provider_exchange_started()
    -> Result<(), Box<dyn std::error::Error>> {
        use tokio::sync::watch;

        struct DropObserveMetaStore {
            inner: Arc<cc_lb_storage_sqlite::SqliteStorage>,
            idle_writes: AtomicUsize,
            released: watch::Sender<bool>,
        }

        #[async_trait::async_trait]
        impl MetaStore for DropObserveMetaStore {
            async fn initialize(&self) -> StorageResult<()> {
                self.inner.initialize().await
            }

            async fn backend_kind(&self) -> StorageResult<cc_lb_storage_api::BackendKind> {
                self.inner.backend_kind().await
            }

            async fn get_meta_value(&self, key: &str) -> StorageResult<Option<String>> {
                self.inner.get_meta_value(key).await
            }

            async fn put_meta_value(&self, key: &str, value: &str) -> StorageResult<()> {
                self.inner.put_meta_value(key, value).await
            }

            async fn compare_and_put_meta_value(
                &self,
                key: &str,
                expected: Option<&str>,
                value: &str,
            ) -> StorageResult<bool> {
                let applied = self
                    .inner
                    .compare_and_put_meta_value(key, expected, value)
                    .await?;
                if applied && value.starts_with("idle:") {
                    self.idle_writes.fetch_add(1, Ordering::SeqCst);
                    self.released.send_replace(true);
                }
                Ok(applied)
            }

            async fn verify_schema_current(&self) -> StorageResult<()> {
                self.inner.verify_schema_current().await
            }
        }

        // Case 1: Dropped before provider exchange starts -> releases promptly to `idle:{gen}`.
        let (fixture1, upstream1, gen1) = setup_killed_oauth_job_fixture().await?;
        let lease_key1 = super::oauth_refresh_lease_meta_key(upstream1);
        let now1 = cc_lb_clock::unix_secs(fixture1.clock.now());
        let lease1 = super::try_acquire_oauth_refresh_lease(
            fixture1.storage.as_ref(),
            upstream1,
            gen1,
            now1,
        )
        .await?
        .ok_or("lease 1 must succeed")?;
        let (released_tx1, mut released_rx1) = watch::channel(false);
        let obs1 = Arc::new(DropObserveMetaStore {
            inner: fixture1.storage.clone(),
            idle_writes: AtomicUsize::new(0),
            released: released_tx1,
        });
        let active1 = super::ActiveOAuthRefreshLease::start_with_interval(
            obs1.clone(),
            fixture1.clock.clone(),
            CancellationToken::new(),
            lease1,
            gen1,
            Duration::from_secs(60),
        );
        drop(active1);
        tokio::time::timeout(
            Duration::from_secs(2),
            released_rx1.wait_for(|&released| released),
        )
        .await??;
        assert_eq!(obs1.idle_writes.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture1
                .storage
                .get_meta_value(&lease_key1)
                .await?
                .as_deref(),
            Some(format!("idle:{gen1}").as_str())
        );
        let healthy1 = super::try_acquire_oauth_refresh_lease(
            fixture1.storage.as_ref(),
            upstream1,
            gen1,
            now1,
        )
        .await?
        .ok_or("peer 1 must acquire lease dropped before exchange started")?;
        super::release_oauth_refresh_lease(
            fixture1.storage.as_ref(),
            upstream1,
            &healthy1.token,
            gen1,
        )
        .await?;

        // Case 2: Dropped after provider exchange starts -> retains exclusive lease protection
        // rather than letting a peer resubmit the potentially rotated refresh token.
        let (fixture2, upstream2, gen2) = setup_killed_oauth_job_fixture().await?;
        let lease_key2 = super::oauth_refresh_lease_meta_key(upstream2);
        let now2 = cc_lb_clock::unix_secs(fixture2.clock.now());
        let lease2 = super::try_acquire_oauth_refresh_lease(
            fixture2.storage.as_ref(),
            upstream2,
            gen2,
            now2,
        )
        .await?
        .ok_or("lease 2 must succeed")?;
        let (released_tx2, _released_rx2) = watch::channel(false);
        let obs2 = Arc::new(DropObserveMetaStore {
            inner: fixture2.storage.clone(),
            idle_writes: AtomicUsize::new(0),
            released: released_tx2,
        });
        let mut active2 = super::ActiveOAuthRefreshLease::start_with_interval(
            obs2.clone(),
            fixture2.clock.clone(),
            CancellationToken::new(),
            lease2.clone(),
            gen2,
            Duration::from_secs(60),
        );
        let lease_cancel2 = active2.cancel_token();
        active2.mark_provider_exchange_started();
        drop(active2);
        tokio::time::timeout(Duration::from_secs(2), lease_cancel2.cancelled()).await?;
        assert_eq!(obs2.idle_writes.load(Ordering::SeqCst), 0);
        assert_eq!(
            fixture2
                .storage
                .get_meta_value(&lease_key2)
                .await?
                .as_deref(),
            Some(lease2.token.as_str())
        );
        assert_eq!(
            super::try_acquire_oauth_refresh_lease(
                fixture2.storage.as_ref(),
                upstream2,
                gen2,
                now2,
            )
            .await?,
            None
        );
        super::release_oauth_refresh_lease(
            fixture2.storage.as_ref(),
            upstream2,
            &lease2.token,
            gen2,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn lazy_refresher_retains_fallback_lease_when_shutdown_cancels_after_provider_accepts()
    -> Result<(), Box<dyn std::error::Error>> {
        let (fixture, upstream_id, starting_generation) = setup_killed_oauth_job_fixture().await?;
        fixture.hold_refresh_response();
        let lease_key = super::oauth_refresh_lease_meta_key(upstream_id);
        let claims = Arc::new(super::ApalisLazyRefreshClaimGuard::new(
            fixture.scheduler_backend.clone(),
            fixture.storage.clone(),
        ));
        let build_refresher = |cancel, wait| {
            LazyRefresher::new_with_claim_guard_for_tests(
                LazyRefresherDeps {
                    google_config: Default::default(),
                    stores: fixture.stores.clone(),
                    aead: fixture.aead.clone(),
                    oauth_cfg: fixture.oauth_cfg.clone(),
                    clock: fixture.clock.clone(),
                },
                Uuid::new_v4(),
                None,
                cancel,
                claims.clone(),
                LazyRefreshContentionConfig::for_tests(wait, Duration::from_millis(5)),
                fixture.scheduler_backend.clone(),
            )
        };
        let shutdown_cancel = CancellationToken::new();
        let refresher = build_refresher(shutdown_cancel.clone(), Duration::from_secs(5));

        let caller_task = tokio::spawn(async move { refresher.refresh_one(upstream_id).await });
        tokio::time::timeout(Duration::from_secs(2), fixture.wait_for_refresh_start())
            .await
            .expect("provider request starts within 2s");

        let held_before = fixture
            .storage
            .get_meta_value(&lease_key)
            .await?
            .ok_or("expected held lease while provider request is in flight")?;
        assert!(held_before.starts_with(&format!("held:{starting_generation}:")));

        shutdown_cancel.cancel();

        let first_outcome = tokio::time::timeout(Duration::from_secs(2), caller_task)
            .await
            .expect("cancelled fallback finishes within 2s")
            .expect("fallback task joins");
        assert!(first_outcome.is_err());

        let retained = fixture
            .storage
            .get_meta_value(&lease_key)
            .await?
            .ok_or("expected held lease to remain after post-send shutdown cancellation")?;
        assert_eq!(retained, held_before);

        // A second claimant on another replica observes the retained lease and never re-sends.
        let peer = build_refresher(CancellationToken::new(), Duration::from_millis(40));
        let peer_outcome =
            tokio::time::timeout(Duration::from_secs(2), peer.refresh_one(upstream_id))
                .await
                .expect("peer contention check finishes within 2s");
        assert!(peer_outcome.is_err());
        assert_eq!(fixture.refresh_call_count(), 1);

        let direct_claim = super::try_acquire_oauth_refresh_lease_cancellation_safe(
            fixture.storage.clone(),
            fixture.storage.clone(),
            upstream_id,
            starting_generation,
            cc_lb_clock::unix_secs(fixture.clock.now()),
        )
        .await?;
        assert!(direct_claim.is_none());

        fixture.release_refresh_response();
        Ok(())
    }

    #[tokio::test]
    async fn lazy_refresher_retains_fallback_lease_on_gateway_timeout_and_releases_on_client_rejection()
    -> Result<(), Box<dyn std::error::Error>> {
        use super::StatusCode;

        async fn spawn_status_token_server(
            status: StatusCode,
            body: &'static str,
        ) -> (Url, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
            let calls = Arc::new(AtomicUsize::new(0));
            let recorded = Arc::clone(&calls);
            let app = axum::Router::new().route(
                "/oauth/token",
                axum::routing::post(move || {
                    let recorded = Arc::clone(&recorded);
                    async move {
                        recorded.fetch_add(1, Ordering::SeqCst);
                        (status, [("content-type", "application/json")], body)
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind status token server");
            let addr = listener.local_addr().expect("status token server addr");
            let url = Url::parse(&format!("http://{addr}/oauth/token")).expect("token url");
            let task = tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            (url, calls, task)
        }

        async fn run_case(
            status: StatusCode,
            body: &'static str,
        ) -> Result<
            (LazyRefreshFixture, Uuid, u64, String, Arc<AtomicUsize>),
            Box<dyn std::error::Error>,
        > {
            let (fixture, upstream_id, starting_generation) =
                setup_killed_oauth_job_fixture().await?;
            let (endpoint, calls, server) = spawn_status_token_server(status, body).await;
            let lease_key = super::oauth_refresh_lease_meta_key(upstream_id);
            let claims = Arc::new(super::ApalisLazyRefreshClaimGuard::new(
                fixture.scheduler_backend.clone(),
                fixture.storage.clone(),
            ));
            let mut oauth_cfg = (*fixture.oauth_cfg).clone();
            oauth_cfg.token_url = endpoint;
            let refresher = LazyRefresher::new_with_claim_guard_for_tests(
                LazyRefresherDeps {
                    google_config: Default::default(),
                    stores: fixture.stores.clone(),
                    aead: fixture.aead.clone(),
                    oauth_cfg: Arc::new(oauth_cfg),
                    clock: fixture.clock.clone(),
                },
                Uuid::new_v4(),
                None,
                CancellationToken::new(),
                claims,
                LazyRefreshContentionConfig::for_tests(
                    Duration::from_secs(5),
                    Duration::from_millis(5),
                ),
                fixture.scheduler_backend.clone(),
            );
            let outcome =
                tokio::time::timeout(Duration::from_secs(2), refresher.refresh_one(upstream_id))
                    .await
                    .expect("fallback refresh settles within 2s");
            assert!(outcome.is_err());
            server.abort();
            Ok((fixture, upstream_id, starting_generation, lease_key, calls))
        }

        // 504 Gateway Timeout keeps the held lease until TTL expiry so concurrent callers cannot replay.
        let (fixture_504, upstream_504, gen_504, lease_key_504, calls_504) = run_case(
            StatusCode::GATEWAY_TIMEOUT,
            r#"{"error":"gateway_timeout"}"#,
        )
        .await?;
        assert_eq!(calls_504.load(Ordering::SeqCst), 1);
        let retained_504 = fixture_504
            .storage
            .get_meta_value(&lease_key_504)
            .await?
            .ok_or("expected held lease after 504 gateway timeout")?;
        assert!(retained_504.starts_with(&format!("held:{gen_504}:")));
        assert!(
            super::try_acquire_oauth_refresh_lease_cancellation_safe(
                fixture_504.storage.clone(),
                fixture_504.storage.clone(),
                upstream_504,
                gen_504,
                cc_lb_clock::unix_secs(fixture_504.clock.now()),
            )
            .await?
            .is_none()
        );

        // Conclusive OAuth 4xx rejections immediately release back to `idle:{gen}`.
        for (status, body) in [
            (StatusCode::TOO_MANY_REQUESTS, r#"{"error":"rate_limited"}"#),
            (StatusCode::UNAUTHORIZED, r#"{"error":"invalid_grant"}"#),
        ] {
            let (fixture, upstream_id, starting_generation, lease_key, calls) =
                run_case(status, body).await?;
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                fixture.storage.get_meta_value(&lease_key).await?.as_deref(),
                Some(format!("idle:{starting_generation}").as_str())
            );
            assert!(
                super::try_acquire_oauth_refresh_lease_cancellation_safe(
                    fixture.storage.clone(),
                    fixture.storage.clone(),
                    upstream_id,
                    starting_generation,
                    cc_lb_clock::unix_secs(fixture.clock.now()),
                )
                .await?
                .is_some()
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn default_complete_refresh_fenced_fails_closed() {
        struct UnimplementedFencedStore;

        #[async_trait::async_trait]
        impl UpstreamStore for UnimplementedFencedStore {
            async fn create(
                &self,
                _create: UpstreamCreate,
            ) -> StorageResult<cc_lb_storage_api::UpstreamRecord> {
                unimplemented!()
            }
            async fn get_by_name(
                &self,
                _name: &str,
            ) -> StorageResult<Option<cc_lb_storage_api::UpstreamRecord>> {
                unimplemented!()
            }
            async fn get_by_id(
                &self,
                _id: Uuid,
            ) -> StorageResult<Option<cc_lb_storage_api::UpstreamRecord>> {
                unimplemented!()
            }
            async fn list(
                &self,
                _after: Option<Uuid>,
                _limit: usize,
            ) -> StorageResult<Vec<cc_lb_storage_api::UpstreamRecord>> {
                unimplemented!()
            }
            async fn update(
                &self,
                _id: Uuid,
                _expected_revision: u64,
                _update: cc_lb_storage_api::UpstreamUpdate,
            ) -> StorageResult<cc_lb_storage_api::UpstreamRecord> {
                unimplemented!()
            }
            async fn set_status(
                &self,
                _id: Uuid,
                _status: cc_lb_storage_api::UpstreamStatusUpdate,
            ) -> StorageResult<()> {
                unimplemented!()
            }
            async fn store_oauth_tokens(
                &self,
                _id: Uuid,
                _expected_revision: u64,
                _tokens: EncryptedOAuthTokens,
                _never_refresh: bool,
            ) -> StorageResult<cc_lb_storage_api::UpstreamRecord> {
                unimplemented!()
            }
            async fn complete_refresh(
                &self,
                _id: Uuid,
                _holder: Uuid,
                _tokens: EncryptedOAuthTokens,
            ) -> StorageResult<cc_lb_storage_api::UpstreamRecord> {
                panic!(
                    "default complete_refresh_fenced must never delegate to unfenced complete_refresh"
                );
            }
            async fn set_last_apply_error(
                &self,
                _id: Uuid,
                _error: Option<String>,
            ) -> StorageResult<()> {
                unimplemented!()
            }
            async fn soft_delete(&self, _id: Uuid, _expected_revision: u64) -> StorageResult<()> {
                unimplemented!()
            }
            async fn hard_delete(&self, _id: Uuid) -> StorageResult<()> {
                unimplemented!()
            }
            async fn clear_warmup_dialect_plugin(
                &self,
                _id: Uuid,
                _expected_revision: u64,
            ) -> StorageResult<Option<cc_lb_storage_api::UpstreamRecord>> {
                unimplemented!()
            }
        }

        let upstream_id = Uuid::new_v4();
        let err = UnimplementedFencedStore
            .complete_refresh_fenced(
                upstream_id,
                Uuid::new_v4(),
                EncryptedOAuthTokens::from_ciphertext(vec![1, 2, 3]),
                &cc_lb_storage_api::OAuthRefreshFence {
                    expected_generation: 1,
                    lease_meta_key: "oauth_refresh_lease:v1:test".to_owned(),
                    expected_lease_prefix: "held:1:test:".to_owned(),
                },
            )
            .await
            .expect_err("default complete_refresh_fenced must fail closed");
        assert!(matches!(err, cc_lb_storage_api::StorageError::Fatal { .. }));
    }

    /// `LazyRefresher::refresh_one` is the single chokepoint every lazy and
    /// usage-poll refresh caller funnels through. The per-caller early returns
    /// upstream of it are defence in depth; this guard is what makes a refresh
    /// of a 365-day credential impossible for any present or future caller,
    /// so it needs coverage of its own.
    #[tokio::test]
    async fn lazy_refresher_refuses_to_refresh_a_long_lived_credential() {
        let fixture = LazyRefreshFixture::new(Duration::from_millis(0)).await;
        let upstream_id = fixture.create_oauth_upstream_with_mode(true).await;
        let claims = Arc::new(TestOAuthRefreshClaims::single_winner());
        let refresher = fixture.lazy_refresher(
            claims,
            LazyRefreshContentionConfig::for_tests(
                Duration::from_millis(20),
                Duration::from_millis(5),
            ),
        );

        let error = refresher
            .refresh_one(upstream_id)
            .await
            .expect_err("a long-lived credential must never be refreshed");

        assert!(
            error.to_string().contains("reauthorization required"),
            "operator-facing error must ask for reauthorization, got: {error}"
        );
        // The Anthropic token endpoint was never contacted: rotating the grant
        // is what destroys the 365-day token.
        assert_eq!(fixture.refresh_call_count(), 0);

        let stored = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await
            .expect("upstream readable")
            .expect("upstream present");
        let bundle = stored
            .oauth_credentials
            .expect("credentials present")
            .decrypt(fixture.aead.as_ref(), upstream_id.as_bytes())
            .expect("credentials decrypt");
        assert_eq!(bundle.access_token, "sk-ant-oat01-old");
        assert!(bundle.never_refresh);
    }

    struct ForbiddenRefreshClaims {
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl LazyRefreshClaimGuard for ForbiddenRefreshClaims {
        async fn begin_refresh(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
            _expires_at_unix_secs: u64,
            _now_unix_secs: u64,
        ) -> StorageResult<LazyRefreshClaim> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(cc_lb_storage_api::StorageError::Fatal {
                message: "access-only refresh must stop before claim acquisition".to_owned(),
            })
        }

        async fn complete_and_bump_generation(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
            _generation: u64,
        ) -> StorageResult<bool> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(false)
        }

        async fn release_if_holder(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
        ) -> StorageResult<bool> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(false)
        }

        async fn task_state(&self, _idempotency_key: &str) -> StorageResult<LazyRefreshTaskState> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(LazyRefreshTaskState::Done)
        }
    }

    #[tokio::test]
    async fn access_only_oauth_lazy_refresh_stops_before_claim_or_generation_write()
    -> Result<(), Box<dyn std::error::Error>> {
        for kind in [UpstreamKind::OpenaiOauth, UpstreamKind::GeminiOauth] {
            let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
            let id = Uuid::new_v4();
            let bundle = OAuthTokenBundle {
                oauth_profile: Default::default(),
                access_token: "access-only-test-token".to_owned(),
                refresh_token: String::new(),
                expires_at_unix_secs: 1,
                refresh_token_expires_at_unix_secs: None,
                account_id: None,
                project_id: (kind == UpstreamKind::GeminiOauth).then(|| "test-project".to_owned()),
                scopes: Vec::new(),
                never_refresh: true,
            };
            let encrypted =
                EncryptedOAuthTokens::encrypt(fixture.aead.as_ref(), &bundle, id.as_bytes())?;
            let original = UpstreamStore::create(
                fixture.storage.as_ref(),
                UpstreamCreate {
                    vertex: None,
                    vertex_credential_type: None,
                    vertex_quota_project_id: None,
                    id,
                    name: format!("{}-access-only-guard", kind.as_str()),
                    kind,
                    base_url: None,
                    api_key_ciphertext: None,
                    oauth_tokens: Some(cc_lb_storage_api::OAuthTokensCreate {
                        tokens: encrypted,
                        never_refresh: true,
                    }),
                    warmup_enabled: false,
                    warmup_dialect_plugin: None,
                    workspace_id: None,
                    api_key_access: None,
                },
            )
            .await?;
            let claims = Arc::new(ForbiddenRefreshClaims {
                calls: AtomicUsize::new(0),
            });
            let refresher = fixture.lazy_refresher(
                claims.clone(),
                LazyRefreshContentionConfig::for_tests(
                    Duration::from_secs(1),
                    Duration::from_millis(1),
                ),
            );
            let result = refresher.refresh_one(id).await;
            assert!(
                matches!(result, Err(cc_lb_signer_anthropic_oauth::LazyRefreshError::Failed { ref reason })
                    if reason == &super::RefreshError::NotRefreshable.to_string()),
                "access-only credentials must be rejected before any claim: {result:?}"
            );
            refresher.enqueue_only(id).await?;
            assert_eq!(claims.calls.load(Ordering::SeqCst), 0);
            assert_eq!(fixture.refresh_call_count(), 0);
            let current = UpstreamStore::get_by_id(fixture.storage.as_ref(), id)
                .await?
                .ok_or("missing access-only upstream")?;
            assert_eq!(
                current.oauth_token_generation,
                original.oauth_token_generation
            );
            assert_eq!(current.revision, original.revision);
            assert_eq!(current.last_apply_error, original.last_apply_error);
            assert_eq!(
                current
                    .oauth_credentials
                    .as_ref()
                    .map(EncryptedOAuthTokens::ciphertext),
                original
                    .oauth_credentials
                    .as_ref()
                    .map(EncryptedOAuthTokens::ciphertext)
            );

            let flow_gate = super::check_refresh_allowed(
                fixture.stores.as_ref(),
                &current,
                &bundle,
                fixture.clock.as_ref(),
            )
            .await;
            assert!(matches!(
                flow_gate,
                Err(super::RefreshError::NotRefreshable)
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn wait_for_token_generation_returns_ok_when_done_state_races_with_generation_advance() {
        // Race between wait_for_token_generation's two consecutive reads:
        //   1. Lazy reads upstream snapshot → generation still equals the starting value
        //      (the worker has not yet committed complete_refresh).
        //   2. The proactive worker commits complete_refresh (generation += 1) and apalis
        //      marks the task as Done.
        //   3. Lazy reads task_state → Done.
        //
        // Without the fix the lazy refresher concludes "completed without advancing
        // generation" and falls back to in_process_refresh, firing a redundant HTTP
        // refresh against the OAuth provider. With the fix the lazy refresher re-reads
        // the upstream after observing the terminal task state, sees the advanced
        // generation, and returns Ok without triggering the fallback.
        let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
        let upstream_id = fixture.create_oauth_upstream().await;
        let claims: Arc<dyn LazyRefreshClaimGuard> = Arc::new(RaceDoneClaims::new(
            fixture.storage.clone(),
            fixture.aead.clone(),
            upstream_id,
        ));
        let config = LazyRefreshContentionConfig::for_tests(
            Duration::from_secs(5),
            Duration::from_millis(10),
        );
        let refresher = fixture.lazy_refresher(claims, config);

        refresher
            .refresh_one(upstream_id)
            .await
            .expect("lazy refresh observes the racing winner's generation advance");

        assert_eq!(
            fixture.refresh_call_count(),
            0,
            "no in-process HTTP fallback should fire when the worker advanced the generation",
        );
    }

    struct LazyRefreshFixture {
        _dir: tempfile::TempDir,
        storage: Arc<cc_lb_storage_sqlite::SqliteStorage>,
        stores: Arc<crate::dynamic_view_builder::Stores>,
        aead: Arc<AeadService>,
        oauth_cfg: Arc<AnthropicOAuthConfig>,
        refresh_calls: Arc<AtomicUsize>,
        refresh_started: watch::Sender<bool>,
        hold_refresh_response: Arc<AtomicBool>,
        refresh_release: Arc<Notify>,
        scheduler_pool: sqlx::SqlitePool,
        scheduler_backend: SchedulerBackend,
        clock: ClockHandle,
    }

    impl LazyRefreshFixture {
        async fn new(refresh_delay: Duration) -> Self {
            let refresh_calls = Arc::new(AtomicUsize::new(0));
            let (refresh_started, _) = watch::channel(false);
            let hold_refresh_response = Arc::new(AtomicBool::new(false));
            let refresh_release = Arc::new(Notify::new());
            let token_url = spawn_lazy_refresh_token_server(
                refresh_calls.clone(),
                refresh_delay,
                refresh_started.clone(),
                hold_refresh_response.clone(),
                refresh_release.clone(),
            )
            .await;
            let dir = tempfile::tempdir().expect("tempdir");
            let database_url = format!(
                "sqlite://{}",
                dir.path().join("lazy-refresh.sqlite").display()
            );
            let storage = Arc::new(
                cc_lb_storage_sqlite::open_sqlite(
                    &database_url,
                    Arc::new(cc_lb_clock::SystemClock),
                )
                .await
                .expect("storage opens"),
            );
            storage.initialize().await.expect("storage initializes");
            let scheduler_db_url =
                format!("sqlite://{}", dir.path().join("scheduler.sqlite").display());
            use std::str::FromStr as _;
            let scheduler_options = sqlx::sqlite::SqliteConnectOptions::from_str(&scheduler_db_url)
                .expect("parse url")
                .create_if_missing(true);
            let scheduler_pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(5)
                .connect_with(scheduler_options)
                .await
                .expect("scheduler storage opens");
            apalis_sqlite::SqliteStorage::setup(&scheduler_pool)
                .await
                .expect("scheduler storage initializes");
            let scheduler_backend =
                SchedulerBackend::Sqlite(cc_lb_scheduler::worker::SqliteSchedulerBackend::new(
                    scheduler_pool.clone(),
                    Arc::new(cc_lb_clock::SystemClock),
                ));
            let stores = Arc::new(crate::dynamic_view_builder::Stores {
                upstreams: storage.clone(),
                vertex_state: Arc::new(crate::dynamic_view_builder::VertexState::new(
                    storage.clone(),
                )),
                principals: storage.clone(),
                plugin_registry: storage.clone(),
                upstream_rate_limits: storage.clone(),
                upstream_subscription_quotas: storage.clone(),
                upstream_subscription_metadata: storage.clone(),
                organization_metadata: storage.clone(),
                plan_tiers: storage.clone(),
                prompt_cache_observations: storage.clone(),
                anthropic_compatibility_kv: storage.clone(),
                audit: Some(storage.clone()),
            });
            let aead = Arc::new(AeadService::from_master_key([42; 32]));
            let oauth_cfg = Arc::new(AnthropicOAuthConfig {
                client_id: "lazy-client".to_owned(),
                auth_url: Url::parse("http://127.0.0.1/oauth/authorize").expect("auth url"),
                token_url,
                redirect_uri: Url::parse("http://127.0.0.1/callback").expect("redirect url"),
                scopes: vec!["messages".to_owned()],
            });
            let clock = Arc::new(TestClock::new_at_secs(1_700_000_000)) as ClockHandle;
            Self {
                _dir: dir,
                storage,
                stores,
                aead,
                oauth_cfg,
                refresh_calls,
                refresh_started,
                hold_refresh_response,
                refresh_release,
                scheduler_pool,
                scheduler_backend,
                clock,
            }
        }

        async fn create_oauth_upstream(&self) -> Uuid {
            self.create_oauth_upstream_with_mode(false).await
        }

        async fn create_oauth_upstream_with_mode(&self, never_refresh: bool) -> Uuid {
            let record = self
                .storage
                .create(UpstreamCreate {
                    vertex: None,
                    vertex_credential_type: None,
                    vertex_quota_project_id: None,
                    id: Uuid::new_v4(),
                    name: "lazy-guard".to_owned(),
                    kind: UpstreamKind::AnthropicOauth,
                    base_url: None,
                    api_key_ciphertext: None,
                    oauth_tokens: None,
                    warmup_enabled: false,
                    warmup_dialect_plugin: None,
                    workspace_id: None,
                    api_key_access: None,
                })
                .await
                .expect("upstream created");
            let encrypted = EncryptedOAuthTokens::encrypt(
                self.aead.as_ref(),
                &OAuthTokenBundle {
                    oauth_profile: Default::default(),
                    access_token: "sk-ant-oat01-old".to_owned(),
                    refresh_token: "sk-ant-ort01-old".to_owned(),
                    expires_at_unix_secs: 1,
                    refresh_token_expires_at_unix_secs: None,
                    account_id: None,
                    project_id: None,
                    scopes: vec!["messages".to_owned()],
                    never_refresh,
                },
                record.id.as_bytes(),
            )
            .expect("tokens encrypt");
            self.storage
                .store_oauth_tokens(record.id, record.revision, encrypted, never_refresh)
                .await
                .expect("tokens stored");
            record.id
        }

        fn lazy_refresher(
            &self,
            claims: Arc<dyn LazyRefreshClaimGuard>,
            config: LazyRefreshContentionConfig,
        ) -> LazyRefresher {
            LazyRefresher::new_with_claim_guard_for_tests(
                LazyRefresherDeps {
                    google_config: Default::default(),
                    stores: self.stores.clone(),
                    aead: self.aead.clone(),
                    oauth_cfg: self.oauth_cfg.clone(),
                    clock: self.clock.clone(),
                },
                Uuid::new_v4(),
                None,
                CancellationToken::new(),
                claims,
                config,
                self.scheduler_backend.clone(),
            )
        }

        fn refresh_call_count(&self) -> usize {
            self.refresh_calls.load(Ordering::SeqCst)
        }

        fn hold_refresh_response(&self) {
            self.hold_refresh_response.store(true, Ordering::SeqCst);
        }

        async fn wait_for_refresh_start(&self) {
            let mut refresh_started = self.refresh_started.subscribe();
            if *refresh_started.borrow() {
                return;
            }
            refresh_started
                .changed()
                .await
                .expect("test token server start sender remains alive");
        }

        fn release_refresh_response(&self) {
            self.hold_refresh_response.store(false, Ordering::SeqCst);
            self.refresh_release.notify_one();
        }
    }

    async fn spawn_lazy_refresh_token_server(
        refresh_calls: Arc<AtomicUsize>,
        refresh_delay: Duration,
        refresh_started: watch::Sender<bool>,
        hold_refresh_response: Arc<AtomicBool>,
        refresh_release: Arc<Notify>,
    ) -> Url {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("token listener binds");
        let addr = listener.local_addr().expect("token listener addr");
        let app = Router::new()
            .route("/oauth/token", post(lazy_refresh_token_response))
            .with_state(TokenServerState {
                refresh_calls,
                refresh_delay,
                refresh_started,
                hold_refresh_response,
                refresh_release,
            });
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("token server runs");
        });
        Url::parse(&format!("http://{addr}/oauth/token")).expect("token url")
    }

    #[derive(Clone)]
    struct TokenServerState {
        refresh_calls: Arc<AtomicUsize>,
        refresh_delay: Duration,
        refresh_started: watch::Sender<bool>,
        hold_refresh_response: Arc<AtomicBool>,
        refresh_release: Arc<Notify>,
    }

    async fn lazy_refresh_token_response(
        State(state): State<TokenServerState>,
    ) -> Json<serde_json::Value> {
        state.refresh_calls.fetch_add(1, Ordering::SeqCst);
        state.refresh_started.send_replace(true);
        if state.hold_refresh_response.load(Ordering::SeqCst) {
            state.refresh_release.notified().await;
        }
        if !state.refresh_delay.is_zero() {
            tokio::time::sleep(state.refresh_delay).await;
        }
        Json(serde_json::json!({
            "access_token": "sk-ant-oat01-new",
            "refresh_token": "sk-ant-ort01-new",
            "expires_in": 3600,
            "scope": "messages"
        }))
    }

    #[derive(Debug)]
    struct TestOAuthRefreshClaims {
        first_acquire_wins: bool,
        allow_timeout_fallback: bool,
        acquired: AtomicBool,
        completed_generation: Mutex<Option<u64>>,
        completion: watch::Sender<Option<u64>>,
        contention_observed: watch::Sender<bool>,
    }

    impl TestOAuthRefreshClaims {
        fn single_winner() -> Self {
            let (completion, _) = watch::channel(None);
            let (contention_observed, _) = watch::channel(false);
            Self {
                first_acquire_wins: true,
                allow_timeout_fallback: false,
                acquired: AtomicBool::new(false),
                completed_generation: Mutex::new(None),
                completion,
                contention_observed,
            }
        }

        fn always_contended() -> Self {
            let (completion, _) = watch::channel(None);
            let (contention_observed, _) = watch::channel(false);
            Self {
                first_acquire_wins: false,
                allow_timeout_fallback: true,
                acquired: AtomicBool::new(true),
                completed_generation: Mutex::new(None),
                completion,
                contention_observed,
            }
        }

        fn active_worker_in_progress() -> Self {
            let (completion, _) = watch::channel(None);
            let (contention_observed, _) = watch::channel(false);
            Self {
                first_acquire_wins: false,
                allow_timeout_fallback: false,
                acquired: AtomicBool::new(true),
                completed_generation: Mutex::new(None),
                completion,
                contention_observed,
            }
        }

        fn completed_generation(&self) -> Option<u64> {
            *self.completed_generation.lock().expect("completed lock")
        }

        async fn wait_for_contention(&self) {
            let mut contention_observed = self.contention_observed.subscribe();
            if *contention_observed.borrow() {
                return;
            }
            contention_observed
                .changed()
                .await
                .expect("test contention sender remains alive");
        }
    }

    #[async_trait::async_trait]
    impl LazyRefreshClaimGuard for TestOAuthRefreshClaims {
        async fn begin_refresh(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
            _expires_at_unix_secs: u64,
            _now_unix_secs: u64,
        ) -> StorageResult<LazyRefreshClaim> {
            if self.first_acquire_wins && !self.acquired.swap(true, Ordering::SeqCst) {
                Ok(LazyRefreshClaim::Acquired)
            } else {
                Ok(LazyRefreshClaim::Enqueued {
                    idempotency_key: "test:oauth-refresh".to_owned(),
                })
            }
        }

        async fn complete_and_bump_generation(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
            generation: u64,
        ) -> StorageResult<bool> {
            *self.completed_generation.lock().expect("completed lock") = Some(generation);
            self.completion.send_replace(Some(generation));
            Ok(true)
        }

        async fn release_if_holder(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
        ) -> StorageResult<bool> {
            Ok(true)
        }

        async fn task_state(&self, _idempotency_key: &str) -> StorageResult<LazyRefreshTaskState> {
            if self.completion.borrow().is_some() {
                Ok(LazyRefreshTaskState::Done)
            } else {
                self.contention_observed.send_replace(true);
                Ok(LazyRefreshTaskState::Active)
            }
        }

        async fn try_claim_fallback(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
            _idempotency_key: &str,
            _generation: u64,
            _now_unix_secs: u64,
        ) -> StorageResult<Option<super::LazyRefreshFallbackLease>> {
            Ok(self
                .allow_timeout_fallback
                .then(|| super::LazyRefreshFallbackLease::from_token("test-claim".to_owned())))
        }
    }

    struct RaceDoneClaims {
        storage: Arc<cc_lb_storage_sqlite::SqliteStorage>,
        aead: Arc<AeadService>,
        upstream_id: Uuid,
        invoked: AtomicBool,
    }

    impl RaceDoneClaims {
        fn new(
            storage: Arc<cc_lb_storage_sqlite::SqliteStorage>,
            aead: Arc<AeadService>,
            upstream_id: Uuid,
        ) -> Self {
            Self {
                storage,
                aead,
                upstream_id,
                invoked: AtomicBool::new(false),
            }
        }
    }

    #[async_trait::async_trait]
    impl LazyRefreshClaimGuard for RaceDoneClaims {
        async fn begin_refresh(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
            _expires_at_unix_secs: u64,
            _now_unix_secs: u64,
        ) -> StorageResult<LazyRefreshClaim> {
            Ok(LazyRefreshClaim::Enqueued {
                idempotency_key: "race:done".to_owned(),
            })
        }

        async fn complete_and_bump_generation(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
            _generation: u64,
        ) -> StorageResult<bool> {
            Ok(true)
        }

        async fn release_if_holder(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
        ) -> StorageResult<bool> {
            Ok(true)
        }

        async fn task_state(&self, _idempotency_key: &str) -> StorageResult<LazyRefreshTaskState> {
            if !self.invoked.swap(true, Ordering::SeqCst) {
                let rotated = EncryptedOAuthTokens::encrypt(
                    self.aead.as_ref(),
                    &OAuthTokenBundle {
                        oauth_profile: Default::default(),
                        access_token: "sk-ant-oat01-rotated".to_owned(),
                        refresh_token: "sk-ant-ort01-rotated".to_owned(),
                        expires_at_unix_secs: 9_999_999_999,
                        refresh_token_expires_at_unix_secs: None,
                        account_id: None,
                        project_id: None,
                        scopes: vec!["messages".to_owned()],
                        never_refresh: false,
                    },
                    self.upstream_id.as_bytes(),
                )
                .expect("rotated tokens encrypt");
                UpstreamStore::complete_refresh(
                    self.storage.as_ref(),
                    self.upstream_id,
                    Uuid::new_v4(),
                    rotated,
                )
                .await?;
            }
            Ok(LazyRefreshTaskState::Done)
        }
    }
}
