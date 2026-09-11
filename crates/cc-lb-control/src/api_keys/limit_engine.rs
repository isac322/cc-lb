use std::collections::{HashMap, VecDeque};
use std::sync::{
    Arc, Weak,
    atomic::{AtomicU32, Ordering},
};
use std::time::Duration;
use tokio::time::Instant;

use cc_lb_storage_api::types::{
    KeyStatus as StoredKeyStatus, PrincipalLimitIdentityKind, PrincipalLimitKind,
    PrincipalLimitState, StoredApiKeyRecord,
};
use cc_lb_storage_api::{
    ApiKeyUsage, ApiKeyUsageBucketDelta, ApiKeyUsageBucketKey, ApiKeyUsageBucketQuery,
    ApiKeyUsageFlush, ApiKeyUsageFlushResult, Storage, StorageError,
};
use parking_lot::{Mutex, RwLock};
use serde::Serialize;
use uuid::Uuid;

use crate::api_keys::concurrent_guard::{KeyConcurrencyGuard, KeyConcurrencyManager};
use crate::api_keys::principal_view::{PrincipalStatus, PrincipalView};
use crate::api_keys::types::{Limit, LimitKind};
use cc_lb_clock::{ClockHandle, unix_secs};

/// Stable identifier for a live [`Reservation`].
///
/// Post-RFC-0002 Phase 7 the engine stores reservation state by this ID so
/// out-of-band consumers (Phase 8 `LimitReconcileSubscriber`, TTL sweeper)
/// can reconcile or refund without holding the `Reservation` handle.
pub type ReservationId = String;

type RollingKey = (String, LimitKind, u64);
const API_KEY_USAGE_WRITER_LEASE_SECS: u64 = 30;
const API_KEY_USAGE_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const API_KEY_USAGE_REFRESH_BATCH_SIZE: usize = 100;
const API_KEY_USAGE_COLD_ALLOWANCE_DIVISOR: i64 = 10;
const API_KEY_USAGE_OUTAGE_CEILING_SECS: u64 = 300;
const API_KEY_USAGE_RETENTION_MARGIN_SECS: u64 = 3_600;
const API_KEY_USAGE_PENDING_MAX_ENTRIES: usize = 100_000;
const API_KEY_USAGE_PENDING_TARGET_ENTRIES: usize = 90_000;
const API_KEY_USAGE_WRITER_INACTIVE_AFTER_SECS: u64 = 60;
const API_KEY_USAGE_COMPACTION_BATCH_SIZE: usize = 1_000;
const API_KEY_USAGE_FINAL_FLUSH_MAX_ATTEMPTS: usize = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityFilter {
    Principal,
    ApiKey,
    All,
}

#[derive(Clone, Debug, Serialize)]
pub struct PrincipalLimitsSnapshot {
    pub principal_id: String,
    pub observed: bool,
    pub identities: Vec<PrincipalLimitIdentitySnapshot>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PrincipalLimitIdentitySnapshot {
    pub identity_kind: &'static str,
    pub identity_value: Option<String>,
    pub account_observed: bool,
    pub windows: Vec<PrincipalLimitWindowSnapshot>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PrincipalLimitWindowSnapshot {
    pub window: String,
    pub snapshots: Vec<PrincipalLimitSnapshot>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PrincipalLimitSnapshot {
    pub kind: &'static str,
    pub limit: Option<u64>,
    pub remaining: Option<u64>,
    pub reset: Option<String>,
    pub observed_at_unix_secs: u64,
    pub stored_at_unix_secs: u64,
    pub observed: bool,
}

pub struct LimitEngine {
    inner: Arc<LimitEngineInner>,
}

struct LimitEngineInner {
    rolling: RwLock<HashMap<RollingKey, RingCounter>>,
    remote_rolling: RwLock<HashMap<RollingKey, RingCounter>>,
    effective_limits: RwLock<HashMap<(String, String), Vec<Limit>>>,
    reservations: Mutex<HashMap<ReservationId, ReservationRecord>>,
    concurrent_mgr: Arc<KeyConcurrencyManager>,
    clock: ClockHandle,
    durable_usage: RwLock<Option<Arc<DurableApiKeyUsage>>>,
}

struct DurableApiKeyUsage {
    storage: Arc<dyn Storage>,
    writer_epoch: RwLock<Uuid>,
    pending: Mutex<HashMap<ApiKeyUsageBucketKey, ApiKeyUsage>>,
    in_flight: Mutex<Option<ApiKeyUsageFlush>>,
    key_limits: RwLock<KeyLimitRegistry>,
    refreshed_at: RwLock<HashMap<String, u64>>,
    refresh_cursor: AtomicU32,
}

#[derive(Default)]
struct KeyLimitRegistry {
    by_key: HashMap<String, Vec<Limit>>,
    sorted_key_ids: Vec<String>,
}

impl KeyLimitRegistry {
    fn record(&mut self, key_id: String, limits: Vec<Limit>) {
        if !self.by_key.contains_key(&key_id) {
            let index = self
                .sorted_key_ids
                .binary_search(&key_id)
                .unwrap_or_else(|index| index);
            self.sorted_key_ids.insert(index, key_id.clone());
        }
        self.by_key.insert(key_id, limits);
    }
}

pub struct ApiKeyUsageHandle {
    cancel: tokio_util::sync::CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl ApiKeyUsageHandle {
    pub async fn shutdown(self) {
        self.cancel.cancel();
        let _ = self.task.await;
    }
}

impl DurableApiKeyUsage {
    fn record(&self, key_id: &str, bucket_widths: &[u64], usage: ApiKeyUsage, now_sec: u64) {
        let mut pending = self.pending.lock();
        for bucket_width_secs in bucket_widths {
            let bucket_start_unix_secs = now_sec - now_sec % bucket_width_secs;
            let key = ApiKeyUsageBucketKey {
                key_id: key_id.to_owned(),
                bucket_width_secs: *bucket_width_secs,
                bucket_start_unix_secs,
            };
            add_usage(pending.entry(key).or_default(), usage);
        }
        bound_pending_usage(&mut pending, now_sec);
    }

    fn record_key_limits(&self, key_id: String, limits: Vec<Limit>) {
        self.key_limits.write().record(key_id, limits);
    }
}

pub struct Reservation {
    engine: Weak<LimitEngineInner>,
    pub(crate) id: ReservationId,
    forgotten: bool,
}

impl Reservation {
    /// Stable engine-side identifier used by
    /// [`LimitEngine::reconcile_by_id`] and [`LimitEngine::refund_by_id`].
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Suppress the RAII refund on drop.
    ///
    /// After Phase 8 the handler hands `id` off to the reconcile subscriber
    /// via `LifecycleEvent::LimitDecision::Reserved` and no longer holds the
    /// reservation to completion. Without this suppressor, dropping the
    /// handle would race the subscriber and full-refund. The TTL sweeper is
    /// the safety net when neither the subscriber nor `forget` ever runs.
    pub fn forget(mut self) {
        self.forgotten = true;
    }
}

/// Engine-side reservation state. Stored inside
/// [`LimitEngineInner::reservations`] and looked up by [`ReservationId`].
///
/// Moved out of `Reservation` in Phase 7 so out-of-band consumers can
/// reconcile without owning the handle. `Reservation` is now just an
/// RAII refund guard keyed by `id`.
#[allow(dead_code)]
struct ReservationRecord {
    key_id: String,
    reserved: Vec<ReservedAmount>,
    durable_bucket_widths: Vec<u64>,
    /// Held here to keep the concurrent-request slot occupied until refund
    /// or reconcile removes the record.
    concurrent_guards: Vec<KeyConcurrencyGuard>,
    created_at: Instant,
}

#[derive(Clone)]
struct ReservedAmount {
    kind: LimitKind,
    window_sec: u64,
    amount: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RejectReason {
    PrincipalMissing,
    PrincipalDisabled,
    KeyDisabled,
    KeyRevoked,
    Expired,
    ModelNotAllowed,
    RequestsRateLimit,
    TokenRateLimit { kind: LimitKind },
    CostRateLimit,
    ConcurrentRateLimit,
    CostUnavailable,
    UnsupportedLimitWindow { window_secs: u64 },
    OutputCapExceeded { cap: i64, requested: i64 },
}

pub struct RingCounter {
    buckets: VecDeque<(u64, i64)>,
    window_sec: u64,
}

impl RingCounter {
    pub fn new(window_sec: u64) -> Self {
        Self {
            buckets: VecDeque::new(),
            window_sec,
        }
    }

    pub fn current_total(&mut self, now_sec: u64) -> i64 {
        self.evict_old(now_sec);
        self.buckets.iter().map(|(_, delta)| *delta).sum()
    }

    fn total_at(&self, now_sec: u64) -> i64 {
        self.buckets
            .iter()
            .filter(|(bucket_sec, _)| bucket_sec.saturating_add(self.window_sec) > now_sec)
            .map(|(_, delta)| *delta)
            .sum()
    }

    pub fn record(&mut self, now_sec: u64, delta: i64) {
        let bucket_width = durable_bucket_width(self.window_sec).unwrap_or(self.window_sec.max(1));
        let bucket_start = now_sec - now_sec % bucket_width;
        match self.buckets.back().copied() {
            None => self.buckets.push_back((bucket_start, delta)),
            Some((last_start, _)) if last_start == bucket_start => {
                let (_, last_delta) = self.buckets.back_mut().expect("last bucket exists");
                *last_delta = last_delta.saturating_add(delta);
            }
            Some((last_start, _)) if last_start < bucket_start => {
                self.buckets.push_back((bucket_start, delta));
            }
            Some(_) => {
                let index = self
                    .buckets
                    .iter()
                    .position(|(existing_start, _)| *existing_start >= bucket_start)
                    .expect("out-of-order bucket has an insertion point");
                if self.buckets[index].0 == bucket_start {
                    self.buckets[index].1 = self.buckets[index].1.saturating_add(delta);
                } else {
                    self.buckets.insert(index, (bucket_start, delta));
                }
            }
        }
        self.evict_old(now_sec);
    }

    pub fn replace_total(&mut self, now_sec: u64, total: i64) {
        self.buckets.clear();
        if total != 0 {
            self.buckets.push_back((now_sec, total));
        }
    }

    fn oldest_bucket_sec(&mut self, now_sec: u64) -> Option<u64> {
        self.evict_old(now_sec);
        self.buckets.front().map(|(bucket_sec, _)| *bucket_sec)
    }

    fn oldest_at(&self, now_sec: u64) -> Option<u64> {
        self.buckets
            .iter()
            .find(|(bucket_sec, _)| bucket_sec.saturating_add(self.window_sec) > now_sec)
            .map(|(bucket_sec, _)| *bucket_sec)
    }

    fn evict_old(&mut self, now_sec: u64) {
        while self
            .buckets
            .front()
            .is_some_and(|(bucket_sec, _)| bucket_sec.saturating_add(self.window_sec) <= now_sec)
        {
            self.buckets.pop_front();
        }
    }
}

impl LimitEngine {
    pub fn new(concurrent_mgr: Arc<KeyConcurrencyManager>, clock: ClockHandle) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(LimitEngineInner {
                rolling: RwLock::new(HashMap::new()),
                remote_rolling: RwLock::new(HashMap::new()),
                effective_limits: RwLock::new(HashMap::new()),
                reservations: Mutex::new(HashMap::new()),
                concurrent_mgr,
                clock,
                durable_usage: RwLock::new(None),
            }),
        })
    }

    pub async fn start_durable_usage_sync(
        self: &Arc<Self>,
        storage: Arc<dyn Storage>,
    ) -> Result<ApiKeyUsageHandle, StorageError> {
        let now_sec = unix_secs(self.inner.clock.now());
        let writer_epoch = Uuid::now_v7();
        storage
            .register_api_key_usage_writer(
                writer_epoch,
                now_sec.saturating_add(API_KEY_USAGE_WRITER_LEASE_SECS),
            )
            .await?;
        let state = Arc::new(DurableApiKeyUsage {
            storage,
            writer_epoch: RwLock::new(writer_epoch),
            pending: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(None),
            key_limits: RwLock::new(KeyLimitRegistry::default()),
            refreshed_at: RwLock::new(HashMap::new()),
            refresh_cursor: AtomicU32::new(0),
        });
        *self.inner.durable_usage.write() = Some(state.clone());

        let cancel = tokio_util::sync::CancellationToken::new();
        let task_cancel = cancel.clone();
        let engine = self.clone();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(API_KEY_USAGE_FLUSH_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = task_cancel.cancelled() => {
                        if let Err(error) = flush_all_durable_api_key_usage(
                            &engine,
                            &state,
                            unix_secs(engine.inner.clock.now()),
                        ).await {
                            tracing::warn!(%error, "final durable API-key usage flush failed");
                        }
                        break;
                    }
                    _ = interval.tick() => {
                        let now_sec = unix_secs(engine.inner.clock.now());
                        if let Err(error) = flush_durable_api_key_usage(&engine, &state, now_sec).await {
                            tracing::warn!(%error, "durable API-key usage flush failed; continuing with local counters");
                        }
                        if let Err(error) = refresh_remote_api_key_usage(&engine, &state, now_sec).await {
                            tracing::warn!(%error, "durable API-key usage refresh failed; freezing last remote snapshot");
                        }
                    }
                }
            }
        });
        Ok(ApiKeyUsageHandle { cancel, task })
    }

    pub const fn durable_usage_retention_secs() -> u64 {
        604_800 + API_KEY_USAGE_RETENTION_MARGIN_SECS
    }

    pub const fn durable_usage_writer_inactive_after_secs() -> u64 {
        API_KEY_USAGE_WRITER_INACTIVE_AFTER_SECS
    }

    pub const fn durable_usage_compaction_batch_size() -> usize {
        API_KEY_USAGE_COMPACTION_BATCH_SIZE
    }

    #[allow(clippy::too_many_arguments)]
    pub fn reserve(
        self: &Arc<Self>,
        view: &PrincipalView,
        record: &StoredApiKeyRecord,
        principal_id: &str,
        model: &str,
        max_tokens: i64,
        max_input_estimate: i64,
        cost_estimate_micros: Option<i64>,
    ) -> Result<Reservation, RejectReason> {
        let now_sec = unix_secs(self.inner.clock.now());
        let key_id = key_id_for(record);

        match view.principal_status(principal_id) {
            PrincipalStatus::Missing => return Err(RejectReason::PrincipalMissing),
            PrincipalStatus::Disabled => return Err(RejectReason::PrincipalDisabled),
            PrincipalStatus::Active => {}
        }

        match record.status {
            StoredKeyStatus::Active => {}
            StoredKeyStatus::Disabled => return Err(RejectReason::KeyDisabled),
            StoredKeyStatus::Revoked => return Err(RejectReason::KeyRevoked),
        }

        if record
            .expires_at_unix_secs
            .is_some_and(|expires_at| now_sec > expires_at)
        {
            return Err(RejectReason::Expired);
        }

        if !view.is_model_allowed(principal_id, model) {
            return Err(RejectReason::ModelNotAllowed);
        }

        let effective_limits = effective_limits(record, view.default_limits(principal_id));
        let durable_usage = self.inner.durable_usage.read().clone();
        let mut durable_bucket_widths = Vec::new();
        if let Some(state) = durable_usage.as_ref() {
            for limit in &effective_limits {
                if limit.kind == LimitKind::Concurrent {
                    continue;
                }
                let Some(bucket_width) = durable_bucket_width(limit.window_secs) else {
                    return Err(RejectReason::UnsupportedLimitWindow {
                        window_secs: limit.window_secs,
                    });
                };
                durable_bucket_widths.push(bucket_width);
            }
            durable_bucket_widths.sort_unstable();
            durable_bucket_widths.dedup();
            state.record_key_limits(key_id.clone(), effective_limits.clone());
        }
        let mut reserved = Vec::new();
        let mut concurrent_guards = Vec::new();

        for limit in &effective_limits {
            let window_sec = limit.window_secs;
            let admission_cap = self.admission_cap(&key_id, limit, now_sec);
            match limit.kind {
                LimitKind::Requests => {
                    let amount = 1;
                    if self.current_total(&key_id, limit.kind, window_sec, now_sec) + amount
                        > admission_cap
                    {
                        return Err(RejectReason::RequestsRateLimit);
                    }
                    reserved.push(ReservedAmount {
                        kind: limit.kind,
                        window_sec,
                        amount,
                    });
                }
                LimitKind::OutputTokens => {
                    if max_tokens > limit.cap_micros {
                        return Err(RejectReason::OutputCapExceeded {
                            cap: limit.cap_micros,
                            requested: max_tokens,
                        });
                    }
                    if self.current_total(&key_id, limit.kind, window_sec, now_sec) + max_tokens
                        > admission_cap
                    {
                        return Err(RejectReason::TokenRateLimit { kind: limit.kind });
                    }
                    reserved.push(ReservedAmount {
                        kind: limit.kind,
                        window_sec,
                        amount: max_tokens,
                    });
                }
                LimitKind::InputTokens => {
                    if self.current_total(&key_id, limit.kind, window_sec, now_sec)
                        + max_input_estimate
                        > admission_cap
                    {
                        return Err(RejectReason::TokenRateLimit { kind: limit.kind });
                    }
                    reserved.push(ReservedAmount {
                        kind: limit.kind,
                        window_sec,
                        amount: max_input_estimate,
                    });
                }
                LimitKind::TotalTokens => {
                    let amount = max_input_estimate.saturating_add(max_tokens);
                    if self.current_total(&key_id, limit.kind, window_sec, now_sec) + amount
                        > admission_cap
                    {
                        return Err(RejectReason::TokenRateLimit { kind: limit.kind });
                    }
                    reserved.push(ReservedAmount {
                        kind: limit.kind,
                        window_sec,
                        amount,
                    });
                }
                LimitKind::CostUsd => {
                    let Some(amount) = cost_estimate_micros else {
                        return Err(RejectReason::CostUnavailable);
                    };
                    if self.current_total(&key_id, limit.kind, window_sec, now_sec) + amount
                        > admission_cap
                    {
                        return Err(RejectReason::CostRateLimit);
                    }
                    reserved.push(ReservedAmount {
                        kind: limit.kind,
                        window_sec,
                        amount,
                    });
                }
                LimitKind::Concurrent => {
                    let guard = self
                        .inner
                        .concurrent_mgr
                        .try_acquire(&key_id, limit.cap_micros as u32)
                        .map_err(|_| RejectReason::ConcurrentRateLimit)?;
                    concurrent_guards.push(guard);
                }
            }
        }

        for amount in &reserved {
            self.record_amount(
                &key_id,
                amount.kind,
                amount.window_sec,
                amount.amount,
                now_sec,
            );
        }
        self.inner.effective_limits.write().insert(
            (key_id.clone(), principal_id.to_owned()),
            effective_limits.clone(),
        );

        let id = Uuid::now_v7().to_string();
        self.inner.reservations.lock().insert(
            id.clone(),
            ReservationRecord {
                key_id: key_id.clone(),
                reserved,
                durable_bucket_widths,
                concurrent_guards,
                created_at: Instant::now(),
            },
        );

        Ok(Reservation {
            engine: Arc::downgrade(&self.inner),
            id,
            forgotten: false,
        })
    }

    pub fn reconcile(
        &self,
        reservation: Reservation,
        actual_input: u64,
        actual_output: u64,
        actual_cost_micros: i64,
    ) {
        self.reconcile_by_id(
            &reservation.id,
            actual_input,
            actual_output,
            actual_cost_micros,
        );
    }

    /// Reconcile a reservation by ID (RFC-0002 Phase 7).
    ///
    /// Used by the Phase 8 `LimitReconcileSubscriber` to reconcile out of
    /// band with the handler. Returns `true` if the reservation was found
    /// and reconciled, `false` if the ID is unknown (already reconciled,
    /// refunded, TTL-evicted, or never issued).
    ///
    /// Semantics match the handler-driven [`Self::reconcile`]: subtract the
    /// difference between reserved and actual for each `ReservedAmount`,
    /// then delete the record so `Drop` on the handle is a no-op.
    pub fn reconcile_by_id(
        &self,
        id: &str,
        actual_input: u64,
        actual_output: u64,
        actual_cost_micros: i64,
    ) -> bool {
        let Some(record) = self.inner.reservations.lock().remove(id) else {
            return false;
        };
        let now_sec = unix_secs(self.inner.clock.now());
        refund_difference(
            &self.inner,
            &record.key_id,
            &record.reserved,
            actual_input,
            actual_output,
            actual_cost_micros,
            now_sec,
        );
        if let Some(state) = self.inner.durable_usage.read().as_ref() {
            state.record(
                &record.key_id,
                &record.durable_bucket_widths,
                ApiKeyUsage {
                    requests: 1,
                    input_tokens: u64_to_i64_saturating(actual_input),
                    output_tokens: u64_to_i64_saturating(actual_output),
                    cost_usd_micros: actual_cost_micros,
                },
                now_sec,
            );
        }
        true
    }

    /// Refund a reservation in full by ID (RFC-0002 Phase 7).
    ///
    /// Used by the TTL sweeper and by out-of-band callers that know a
    /// request will never reconcile (e.g. client hang-up before response).
    /// Returns `true` if the reservation was found and refunded.
    pub fn refund_by_id(&self, id: &str) -> bool {
        let Some(record) = self.inner.reservations.lock().remove(id) else {
            return false;
        };
        let now_sec = unix_secs(self.inner.clock.now());
        for amount in &record.reserved {
            self.inner.record_amount(
                &record.key_id,
                amount.kind,
                amount.window_sec,
                -amount.amount,
                now_sec,
            );
        }
        true
    }

    pub fn headers_for(&self, key_id: &str, principal_id: &str) -> Vec<(String, String)> {
        let Some(limits) = self
            .inner
            .effective_limits
            .read()
            .get(&(key_id.to_owned(), principal_id.to_owned()))
            .cloned()
        else {
            return Vec::new();
        };

        let now_sec = unix_secs(self.inner.clock.now());
        let mut headers = Vec::new();
        let mut emitted_requests = false;
        let mut emitted_tokens = false;

        for limit in limits {
            if emitted_requests && emitted_tokens {
                break;
            }

            let prefix = match limit.kind {
                LimitKind::Requests if !emitted_requests => {
                    emitted_requests = true;
                    "anthropic-ratelimit-requests"
                }
                LimitKind::TotalTokens if !emitted_tokens => {
                    emitted_tokens = true;
                    "anthropic-ratelimit-tokens"
                }
                _ => continue,
            };

            let window_sec = limit.window_secs;
            let (total, oldest) =
                self.current_total_and_oldest(key_id, limit.kind, window_sec, now_sec);
            let remaining = limit.cap_micros.saturating_sub(total).max(0);
            let reset_sec = oldest.unwrap_or(now_sec).saturating_add(window_sec);
            headers.push((format!("{prefix}-remaining"), remaining.to_string()));
            headers.push((format!("{prefix}-limit"), limit.cap_micros.to_string()));
            headers.push((format!("{prefix}-reset"), unix_to_iso8601(reset_sec)));
        }

        headers
    }

    pub fn snapshot_for_principal(
        &self,
        view: &PrincipalView,
        principal_id: &str,
        identity_filter: IdentityFilter,
    ) -> PrincipalLimitsSnapshot {
        let now_sec = unix_secs(self.inner.clock.now());
        let defaults = view.default_limits(principal_id).to_vec();
        let effective_limits = self.inner.effective_limits.read().clone();
        let mut identities = Vec::new();

        if matches!(
            identity_filter,
            IdentityFilter::Principal | IdentityFilter::All
        ) {
            identities.push(PrincipalLimitIdentitySnapshot {
                identity_kind: "principal",
                identity_value: None,
                account_observed: true,
                windows: self.snapshot_windows_for_principal(
                    &defaults,
                    principal_id,
                    &effective_limits,
                    now_sec,
                ),
            });
        }

        if matches!(
            identity_filter,
            IdentityFilter::ApiKey | IdentityFilter::All
        ) {
            let mut key_limits = effective_limits
                .iter()
                .filter(|((_, stored_principal_id), _)| stored_principal_id == principal_id)
                .map(|((key_id, _), limits)| (key_id.clone(), limits.clone()))
                .collect::<Vec<_>>();
            key_limits.sort_by(|left, right| left.0.cmp(&right.0));
            for (key_id, limits) in key_limits {
                identities.push(PrincipalLimitIdentitySnapshot {
                    identity_kind: "api_key",
                    identity_value: Some(key_id.clone()),
                    account_observed: false,
                    windows: self.snapshot_windows_for_key(&key_id, &limits, now_sec),
                });
            }
        }

        let observed = identities.iter().any(|identity| {
            identity
                .windows
                .iter()
                .any(|window| window.snapshots.iter().any(|snapshot| snapshot.observed))
        });

        PrincipalLimitsSnapshot {
            principal_id: principal_id.to_owned(),
            observed,
            identities,
        }
    }

    fn snapshot_windows_for_principal(
        &self,
        defaults: &[Limit],
        principal_id: &str,
        effective_limits: &HashMap<(String, String), Vec<Limit>>,
        now_sec: u64,
    ) -> Vec<PrincipalLimitWindowSnapshot> {
        let mut windows = Vec::new();
        for limit in defaults {
            let window_sec = limit.window_secs;
            let mut total = 0_i64;
            let mut oldest: Option<u64> = None;
            for ((key_id, stored_principal_id), limits) in effective_limits {
                if stored_principal_id != principal_id
                    || !limits.iter().any(|candidate| {
                        candidate.kind == limit.kind && candidate.window_secs == limit.window_secs
                    })
                {
                    continue;
                }
                let (key_total, key_oldest) =
                    self.current_total_and_oldest(key_id, limit.kind, window_sec, now_sec);
                total = total.saturating_add(key_total);
                oldest = match (oldest, key_oldest) {
                    (Some(left), Some(right)) => Some(left.min(right)),
                    (None, Some(right)) => Some(right),
                    (current, None) => current,
                };
            }
            push_limit_snapshot(&mut windows, limit, total, oldest, now_sec);
        }
        sort_windows(windows)
    }

    fn snapshot_windows_for_key(
        &self,
        key_id: &str,
        limits: &[Limit],
        now_sec: u64,
    ) -> Vec<PrincipalLimitWindowSnapshot> {
        let mut windows = Vec::new();
        for limit in limits {
            let window_sec = limit.window_secs;
            let (total, oldest) =
                self.current_total_and_oldest(key_id, limit.kind, window_sec, now_sec);
            push_limit_snapshot(&mut windows, limit, total, oldest, now_sec);
        }
        sort_windows(windows)
    }
}

impl LimitEngine {
    pub fn record_principal_limit_state(&self, state: &PrincipalLimitState) {
        let kind = principal_limit_kind(state.kind);
        let Some(window_sec) = parse_limit_window_secs(&state.window) else {
            return;
        };
        let key_id = principal_limit_key_id(state);

        if let Some(limit) = state.limit {
            let limit = Limit {
                kind,
                window_secs: window_sec,
                cap_micros: u64_to_i64_saturating(limit),
            };
            let mut effective_limits = self.inner.effective_limits.write();
            let limits = effective_limits
                .entry((key_id.clone(), state.principal_id.clone()))
                .or_default();
            limits.retain(|candidate| {
                candidate.kind != limit.kind || candidate.window_secs != limit.window_secs
            });
            limits.push(limit);
        }

        if let (Some(limit), Some(remaining)) = (state.limit, state.remaining) {
            let used = u64_to_i64_saturating(limit.saturating_sub(remaining));
            let mut rolling = self.inner.rolling.write();
            rolling
                .entry((key_id, kind, window_sec))
                .or_insert_with(|| RingCounter::new(window_sec))
                .replace_total(state.observed_at_unix_secs, used);
        }
    }

    fn admission_cap(&self, key_id: &str, limit: &Limit, now_sec: u64) -> i64 {
        let Some(state) = self.inner.durable_usage.read().clone() else {
            return limit.cap_micros;
        };
        let refreshed_at = state.refreshed_at.read().get(key_id).copied();
        let registered_keys = state.key_limits.read().sorted_key_ids.len();
        let max_staleness = api_key_usage_max_staleness_secs(limit.window_secs, registered_keys);
        if refreshed_at
            .is_some_and(|refreshed_at| now_sec.saturating_sub(refreshed_at) <= max_staleness)
        {
            return limit.cap_micros;
        }
        limit
            .cap_micros
            .saturating_div(API_KEY_USAGE_COLD_ALLOWANCE_DIVISOR)
            .max(1)
    }

    fn current_total(&self, key_id: &str, kind: LimitKind, window_sec: u64, now_sec: u64) -> i64 {
        let key = (key_id.to_owned(), kind, window_sec);
        let local = self
            .inner
            .rolling
            .write()
            .entry(key.clone())
            .or_insert_with(|| RingCounter::new(window_sec))
            .current_total(now_sec);
        let remote = self
            .inner
            .remote_rolling
            .read()
            .get(&key)
            .map_or(0, |counter| counter.total_at(now_sec));
        local.saturating_add(remote)
    }

    fn current_total_and_oldest(
        &self,
        key_id: &str,
        kind: LimitKind,
        window_sec: u64,
        now_sec: u64,
    ) -> (i64, Option<u64>) {
        let key = (key_id.to_owned(), kind, window_sec);
        let (local_total, local_oldest) = {
            let mut rolling = self.inner.rolling.write();
            let counter = rolling
                .entry(key.clone())
                .or_insert_with(|| RingCounter::new(window_sec));
            (
                counter.current_total(now_sec),
                counter.oldest_bucket_sec(now_sec),
            )
        };
        let (remote_total, remote_oldest) = self
            .inner
            .remote_rolling
            .read()
            .get(&key)
            .map_or((0, None), |counter| {
                (counter.total_at(now_sec), counter.oldest_at(now_sec))
            });
        (
            local_total.saturating_add(remote_total),
            match (local_oldest, remote_oldest) {
                (Some(left), Some(right)) => Some(left.min(right)),
                (Some(value), None) | (None, Some(value)) => Some(value),
                (None, None) => None,
            },
        )
    }

    fn record_amount(
        &self,
        key_id: &str,
        kind: LimitKind,
        window_sec: u64,
        delta: i64,
        now_sec: u64,
    ) {
        self.inner
            .record_amount(key_id, kind, window_sec, delta, now_sec);
    }
}

impl LimitEngineInner {
    fn record_amount(
        &self,
        key_id: &str,
        kind: LimitKind,
        window_sec: u64,
        delta: i64,
        now_sec: u64,
    ) {
        let mut rolling = self.rolling.write();
        rolling
            .entry((key_id.to_owned(), kind, window_sec))
            .or_insert_with(|| RingCounter::new(window_sec))
            .record(now_sec, delta);
    }
}

fn add_usage(aggregate: &mut ApiKeyUsage, usage: ApiKeyUsage) {
    aggregate.requests = aggregate.requests.saturating_add(usage.requests);
    aggregate.input_tokens = aggregate.input_tokens.saturating_add(usage.input_tokens);
    aggregate.output_tokens = aggregate.output_tokens.saturating_add(usage.output_tokens);
    aggregate.cost_usd_micros = aggregate
        .cost_usd_micros
        .saturating_add(usage.cost_usd_micros);
}

fn bucket_retention_secs(bucket_width_secs: u64) -> u64 {
    match bucket_width_secs {
        1 => 60,
        60 => 3_600,
        300 => 18_000,
        3_600 => 604_800,
        _ => 0,
    }
}

fn bound_pending_usage(pending: &mut HashMap<ApiKeyUsageBucketKey, ApiKeyUsage>, now_sec: u64) {
    bound_pending_usage_to(
        pending,
        now_sec,
        API_KEY_USAGE_PENDING_MAX_ENTRIES,
        API_KEY_USAGE_PENDING_TARGET_ENTRIES,
    );
}

fn bound_pending_usage_to(
    pending: &mut HashMap<ApiKeyUsageBucketKey, ApiKeyUsage>,
    now_sec: u64,
    max_entries: usize,
    target_entries: usize,
) {
    if pending.len() <= max_entries {
        return;
    }
    let before_expiry = pending.len();
    pending.retain(|key, _| {
        let retention_secs = bucket_retention_secs(key.bucket_width_secs);
        retention_secs != 0
            && key
                .bucket_start_unix_secs
                .saturating_add(key.bucket_width_secs.saturating_sub(1))
                .saturating_add(retention_secs)
                > now_sec
    });
    let expired = before_expiry.saturating_sub(pending.len());
    if expired > 0 {
        metrics::counter!(
            "cc_lb_api_key_usage_pending_compacted_total",
            "reason" => "expired"
        )
        .increment(expired as u64);
    }
    if pending.len() <= max_entries {
        return;
    }

    let target_entries = target_entries.min(max_entries);
    let drop_count = pending.len().saturating_sub(target_entries);
    let mut oldest = pending.keys().cloned().collect::<Vec<_>>();
    oldest.sort_unstable_by(|left, right| {
        left.bucket_start_unix_secs
            .cmp(&right.bucket_start_unix_secs)
            .then_with(|| left.bucket_width_secs.cmp(&right.bucket_width_secs))
            .then_with(|| left.key_id.cmp(&right.key_id))
    });
    for key in oldest.into_iter().take(drop_count) {
        pending.remove(&key);
    }
    metrics::counter!(
        "cc_lb_api_key_usage_pending_compacted_total",
        "reason" => "capacity"
    )
    .increment(drop_count as u64);
    tracing::warn!(
        dropped = drop_count,
        retained = pending.len(),
        "durable API-key usage pending buffer reached capacity; dropped oldest deltas"
    );
}

fn durable_bucket_width(window_secs: u64) -> Option<u64> {
    match window_secs {
        0..=60 => Some(1),
        61..=3_600 => Some(60),
        3_601..=18_000 => Some(300),
        18_001..=604_800 => Some(3_600),
        _ => None,
    }
}

fn api_key_usage_max_staleness_secs(window_secs: u64, registered_keys: usize) -> u64 {
    let refresh_cycles = registered_keys.div_ceil(API_KEY_USAGE_REFRESH_BATCH_SIZE);
    let refresh_cycle_secs = u64::try_from(refresh_cycles)
        .unwrap_or(u64::MAX)
        .saturating_mul(API_KEY_USAGE_FLUSH_INTERVAL.as_secs());
    window_secs
        .saturating_div(10)
        .max(refresh_cycle_secs.saturating_mul(2))
        .min(API_KEY_USAGE_OUTAGE_CEILING_SECS)
}

fn durable_usage_amount(usage: ApiKeyUsage, kind: LimitKind) -> i64 {
    match kind {
        LimitKind::Requests => usage.requests,
        LimitKind::InputTokens => usage.input_tokens,
        LimitKind::OutputTokens => usage.output_tokens,
        LimitKind::TotalTokens => usage.input_tokens.saturating_add(usage.output_tokens),
        LimitKind::CostUsd => usage.cost_usd_micros,
        LimitKind::Concurrent => 0,
    }
}

async fn flush_durable_api_key_usage(
    engine: &Arc<LimitEngine>,
    state: &Arc<DurableApiKeyUsage>,
    now_sec: u64,
) -> Result<(), StorageError> {
    let request = {
        let mut in_flight = state.in_flight.lock();
        if in_flight.is_none() {
            let deltas = state
                .pending
                .lock()
                .drain()
                .map(|(key, usage)| ApiKeyUsageBucketDelta { key, usage })
                .collect();
            *in_flight = Some(ApiKeyUsageFlush {
                writer_epoch: *state.writer_epoch.read(),
                flush_id: Uuid::now_v7(),
                lease_until_unix_secs: now_sec.saturating_add(API_KEY_USAGE_WRITER_LEASE_SECS),
                deltas,
            });
        }
        let request = in_flight.as_mut().expect("flush initialized");
        request.lease_until_unix_secs = now_sec.saturating_add(API_KEY_USAGE_WRITER_LEASE_SECS);
        request.clone()
    };

    match state.storage.flush_api_key_usage(&request).await {
        Ok(ApiKeyUsageFlushResult::Applied | ApiKeyUsageFlushResult::AlreadyApplied) => {
            let mut in_flight = state.in_flight.lock();
            if in_flight
                .as_ref()
                .is_some_and(|current| current.flush_id == request.flush_id)
            {
                *in_flight = None;
            }
            Ok(())
        }
        Ok(ApiKeyUsageFlushResult::LeaseLost) => {
            handoff_durable_writer(engine, state, now_sec).await
        }
        Err(error) => Err(error),
    }
}

async fn flush_all_durable_api_key_usage(
    engine: &Arc<LimitEngine>,
    state: &Arc<DurableApiKeyUsage>,
    now_sec: u64,
) -> Result<(), StorageError> {
    for _ in 0..API_KEY_USAGE_FINAL_FLUSH_MAX_ATTEMPTS {
        flush_durable_api_key_usage(engine, state, now_sec).await?;
        if state.in_flight.lock().is_none() && state.pending.lock().is_empty() {
            return Ok(());
        }
    }
    Err(StorageError::Unavailable {
        message: format!(
            "durable API-key usage still pending after {API_KEY_USAGE_FINAL_FLUSH_MAX_ATTEMPTS} final flush attempts"
        ),
    })
}

async fn handoff_durable_writer(
    engine: &Arc<LimitEngine>,
    state: &Arc<DurableApiKeyUsage>,
    now_sec: u64,
) -> Result<(), StorageError> {
    let writer_epoch = Uuid::now_v7();
    state
        .storage
        .register_api_key_usage_writer(
            writer_epoch,
            now_sec.saturating_add(API_KEY_USAGE_WRITER_LEASE_SECS),
        )
        .await?;
    *state.writer_epoch.write() = writer_epoch;
    if let Some(in_flight) = state.in_flight.lock().as_mut() {
        in_flight.writer_epoch = writer_epoch;
        in_flight.flush_id = Uuid::now_v7();
        in_flight.lease_until_unix_secs = now_sec.saturating_add(API_KEY_USAGE_WRITER_LEASE_SECS);
    }
    state.refreshed_at.write().clear();
    engine.inner.remote_rolling.write().clear();
    rebuild_local_usage_after_handoff(engine, state, now_sec);
    tracing::warn!(%writer_epoch, "durable API-key usage writer lease lost; started fresh epoch");
    Ok(())
}

fn rebuild_local_usage_after_handoff(
    engine: &Arc<LimitEngine>,
    state: &Arc<DurableApiKeyUsage>,
    now_sec: u64,
) {
    let mut unflushed = state
        .pending
        .lock()
        .iter()
        .map(|(key, usage)| ApiKeyUsageBucketDelta {
            key: key.clone(),
            usage: *usage,
        })
        .collect::<Vec<_>>();
    if let Some(in_flight) = state.in_flight.lock().as_ref() {
        unflushed.extend(in_flight.deltas.iter().cloned());
    }
    unflushed.sort_unstable_by(|left, right| {
        left.key
            .bucket_start_unix_secs
            .cmp(&right.key.bucket_start_unix_secs)
            .then_with(|| left.key.bucket_width_secs.cmp(&right.key.bucket_width_secs))
            .then_with(|| left.key.key_id.cmp(&right.key.key_id))
    });
    let reservations = engine
        .inner
        .reservations
        .lock()
        .values()
        .map(|record| (record.key_id.clone(), record.reserved.clone()))
        .collect::<Vec<_>>();
    let limits = state.key_limits.read();
    engine
        .inner
        .rolling
        .write()
        .retain(|(key_id, _, _), _| !limits.by_key.contains_key(key_id));
    for delta in unflushed {
        let Some(key_limits) = limits.by_key.get(&delta.key.key_id) else {
            continue;
        };
        let observed_at = delta
            .key
            .bucket_start_unix_secs
            .saturating_add(delta.key.bucket_width_secs.saturating_sub(1));
        for limit in key_limits {
            if durable_bucket_width(limit.window_secs) != Some(delta.key.bucket_width_secs) {
                continue;
            }
            let amount = durable_usage_amount(delta.usage, limit.kind);
            if amount != 0 {
                engine.inner.record_amount(
                    &delta.key.key_id,
                    limit.kind,
                    limit.window_secs,
                    amount,
                    observed_at,
                );
            }
        }
    }
    for (key_id, reserved) in reservations {
        if !limits.by_key.contains_key(&key_id) {
            continue;
        }
        for amount in reserved {
            engine.inner.record_amount(
                &key_id,
                amount.kind,
                amount.window_sec,
                amount.amount,
                now_sec,
            );
        }
    }
    drop(limits);
}

async fn refresh_remote_api_key_usage(
    engine: &Arc<LimitEngine>,
    state: &Arc<DurableApiKeyUsage>,
    now_sec: u64,
) -> Result<(), StorageError> {
    let (batch, limits) = {
        let registry = state.key_limits.read();
        if registry.sorted_key_ids.is_empty() {
            return Ok(());
        }
        let start = (state
            .refresh_cursor
            .fetch_add(API_KEY_USAGE_REFRESH_BATCH_SIZE as u32, Ordering::AcqRel)
            as usize)
            % registry.sorted_key_ids.len();
        let batch = registry
            .sorted_key_ids
            .iter()
            .cycle()
            .skip(start)
            .take(API_KEY_USAGE_REFRESH_BATCH_SIZE.min(registry.sorted_key_ids.len()))
            .cloned()
            .collect::<Vec<_>>();
        let limits = batch
            .iter()
            .filter_map(|key_id| {
                registry
                    .by_key
                    .get(key_id)
                    .cloned()
                    .map(|limits| (key_id.clone(), limits))
            })
            .collect::<HashMap<_, _>>();
        (batch, limits)
    };
    let max_window = limits
        .values()
        .flatten()
        .filter(|limit| limit.kind != LimitKind::Concurrent)
        .map(|limit| limit.window_secs)
        .max()
        .unwrap_or(0);
    if max_window == 0 {
        return Ok(());
    }
    let exclude_writer_epoch = *state.writer_epoch.read();
    let buckets = state
        .storage
        .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
            key_ids: batch.clone(),
            since_unix_secs: now_sec.saturating_sub(max_window),
            until_unix_secs: now_sec,
            exclude_writer_epoch,
        })
        .await?;
    let mut replacement = HashMap::<RollingKey, RingCounter>::new();
    for key_id in &batch {
        let Some(key_limits) = limits.get(key_id) else {
            continue;
        };
        for limit in key_limits {
            if limit.kind == LimitKind::Concurrent {
                continue;
            }
            let Some(bucket_width) = durable_bucket_width(limit.window_secs) else {
                continue;
            };
            let mut counter = RingCounter::new(limit.window_secs);
            for bucket in buckets.iter().filter(|bucket| {
                bucket.key.key_id == *key_id && bucket.key.bucket_width_secs == bucket_width
            }) {
                let amount = durable_usage_amount(bucket.usage, limit.kind);
                if amount != 0 {
                    counter.record(
                        bucket
                            .key
                            .bucket_start_unix_secs
                            .saturating_add(bucket_width.saturating_sub(1)),
                        amount,
                    );
                }
            }
            replacement.insert((key_id.clone(), limit.kind, limit.window_secs), counter);
        }
    }
    let batch_keys = batch
        .iter()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    let mut remote = engine.inner.remote_rolling.write();
    remote.retain(|(key_id, _, _), _| !batch_keys.contains(key_id));
    remote.extend(replacement);
    drop(remote);
    let mut refreshed_at = state.refreshed_at.write();
    for key_id in batch {
        refreshed_at.insert(key_id, now_sec);
    }
    Ok(())
}

impl Drop for Reservation {
    fn drop(&mut self) {
        // RAII refund: if the engine still holds our record (nobody called
        // reconcile_by_id / refund_by_id / TTL sweeper), refund in full.
        // Skipped when `forget()` was called (Phase 8 subscriber ownership).
        if self.forgotten {
            return;
        }
        let Some(engine) = self.engine.upgrade() else {
            return;
        };
        let Some(record) = engine.reservations.lock().remove(&self.id) else {
            return;
        };
        let now_sec = unix_secs(engine.clock.now());
        for amount in &record.reserved {
            engine.record_amount(
                &record.key_id,
                amount.kind,
                amount.window_sec,
                -amount.amount,
                now_sec,
            );
        }
    }
}

fn push_limit_snapshot(
    windows: &mut Vec<PrincipalLimitWindowSnapshot>,
    limit: &Limit,
    total: i64,
    oldest: Option<u64>,
    now_sec: u64,
) {
    let window_sec = limit.window_secs;
    let remaining = limit.cap_micros.saturating_sub(total).max(0) as u64;
    let reset_sec = oldest.unwrap_or(now_sec).saturating_add(window_sec);
    let window = format_window(window_sec);
    let snapshot = PrincipalLimitSnapshot {
        kind: limit_kind_name(limit.kind),
        limit: Some(limit.cap_micros.max(0) as u64),
        remaining: Some(remaining),
        reset: Some(unix_to_iso8601(reset_sec)),
        observed_at_unix_secs: now_sec,
        stored_at_unix_secs: now_sec,
        observed: total != 0,
    };

    match windows
        .iter_mut()
        .find(|candidate| candidate.window == window)
    {
        Some(existing) => existing.snapshots.push(snapshot),
        None => windows.push(PrincipalLimitWindowSnapshot {
            window,
            snapshots: vec![snapshot],
        }),
    }
}

fn sort_windows(
    mut windows: Vec<PrincipalLimitWindowSnapshot>,
) -> Vec<PrincipalLimitWindowSnapshot> {
    windows.sort_by(|left, right| {
        window_sort_key(&left.window)
            .cmp(&window_sort_key(&right.window))
            .then_with(|| left.window.cmp(&right.window))
    });
    for window in &mut windows {
        window.snapshots.sort_by(|left, right| {
            limit_kind_sort_key(left.kind)
                .cmp(&limit_kind_sort_key(right.kind))
                .then_with(|| left.kind.cmp(right.kind))
        });
    }
    windows
}

fn format_window(window_sec: u64) -> String {
    if window_sec.is_multiple_of(86_400) {
        format!("{}d", window_sec / 86_400)
    } else if window_sec.is_multiple_of(3_600) {
        format!("{}h", window_sec / 3_600)
    } else if window_sec.is_multiple_of(60) {
        format!("{}m", window_sec / 60)
    } else {
        format!("{window_sec}s")
    }
}

fn window_sort_key(window: &str) -> u8 {
    match window {
        "5h" => 0,
        "7d" | "weekly" => 1,
        _ => 2,
    }
}

fn limit_kind_sort_key(kind: &str) -> u8 {
    match kind {
        "requests" => 0,
        "input_tokens" => 1,
        "output_tokens" => 2,
        "total_tokens" => 3,
        "cost_usd" => 4,
        "concurrent" => 5,
        _ => 6,
    }
}

fn limit_kind_name(kind: LimitKind) -> &'static str {
    match kind {
        LimitKind::Requests => "requests",
        LimitKind::InputTokens => "input_tokens",
        LimitKind::OutputTokens => "output_tokens",
        LimitKind::TotalTokens => "total_tokens",
        LimitKind::CostUsd => "cost_usd",
        LimitKind::Concurrent => "concurrent",
    }
}

fn refund_difference(
    engine: &LimitEngineInner,
    key_id: &str,
    reserved: &[ReservedAmount],
    actual_input: u64,
    actual_output: u64,
    actual_cost_micros: i64,
    now_sec: u64,
) {
    for amount in reserved {
        let actual = match amount.kind {
            LimitKind::Requests => 1,
            LimitKind::InputTokens => actual_input as i64,
            LimitKind::OutputTokens => actual_output as i64,
            LimitKind::TotalTokens => actual_input.saturating_add(actual_output) as i64,
            LimitKind::CostUsd => actual_cost_micros,
            LimitKind::Concurrent => continue,
        };
        let refund = amount.amount.saturating_sub(actual).max(0);
        if refund > 0 {
            engine.record_amount(key_id, amount.kind, amount.window_sec, -refund, now_sec);
        }
    }
    // TODO(T22): enqueue PrincipalLimitStateRow via master's existing limit_state_writer
}

fn effective_limits(record: &StoredApiKeyRecord, defaults: &[Limit]) -> Vec<Limit> {
    let mut limits = defaults.to_vec();

    for override_limit in &record.limit_overrides {
        let limit = Limit {
            kind: override_limit.kind,
            window_secs: override_limit.window_secs,
            cap_micros: override_limit.cap_micros,
        };

        if let Some(existing) = limits.iter_mut().find(|existing| {
            existing.kind == limit.kind && existing.window_secs == limit.window_secs
        }) {
            *existing = limit;
        } else {
            limits.push(limit);
        }
    }

    limits
}

fn principal_limit_kind(kind: PrincipalLimitKind) -> LimitKind {
    match kind {
        PrincipalLimitKind::Requests => LimitKind::Requests,
        PrincipalLimitKind::Tokens => LimitKind::TotalTokens,
        PrincipalLimitKind::InputTokens => LimitKind::InputTokens,
        PrincipalLimitKind::OutputTokens => LimitKind::OutputTokens,
    }
}

fn principal_limit_key_id(state: &PrincipalLimitState) -> String {
    match state.identity_kind {
        PrincipalLimitIdentityKind::Credential => state
            .identity_value
            .clone()
            .unwrap_or_else(|| state.principal_id.clone()),
        PrincipalLimitIdentityKind::Account | PrincipalLimitIdentityKind::Unobserved => {
            state.principal_id.clone()
        }
    }
}

fn parse_limit_window_secs(window: &str) -> Option<u64> {
    match window {
        "default" | "minute" | "1m" => Some(60),
        "hour" | "1h" => Some(3_600),
        "5h" => Some(18_000),
        "weekly" | "7d" => Some(604_800),
        _ => parse_window_suffix(window),
    }
}

fn parse_window_suffix(window: &str) -> Option<u64> {
    let (value, multiplier) = if let Some(value) = window.strip_suffix('s') {
        (value, 1)
    } else if let Some(value) = window.strip_suffix('m') {
        (value, 60)
    } else if let Some(value) = window.strip_suffix('h') {
        (value, 3_600)
    } else {
        let value = window.strip_suffix('d')?;
        (value, 86_400)
    };
    value.parse::<u64>().ok()?.checked_mul(multiplier)
}

fn u64_to_i64_saturating(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn key_id_for(record: &StoredApiKeyRecord) -> String {
    record.key_hash_b64.clone()
}

fn unix_to_iso8601(timestamp: u64) -> String {
    let days = (timestamp / 86_400) as i64;
    let seconds_of_day = timestamp % 86_400;
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += if month <= 2 { 1 } else { 0 };
    (year, month as u32, day as u32)
}

/// Handle to the TTL sweeper task spawned by
/// [`spawn_reservation_ttl_sweeper`]. Dropping the handle stops the task on
/// the next tick.
pub struct ReservationTtlSweeperHandle {
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    join: tokio::task::JoinHandle<()>,
}

impl ReservationTtlSweeperHandle {
    /// Signal the sweeper to stop and wait for it to exit.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        if let Err(error) = self.join.await {
            tracing::warn!(%error, "limit reservation ttl sweeper task panicked");
        }
    }
}

/// Spawn the TTL sweeper (RFC-0002 Phase 7).
///
/// Every `tick_interval` the sweeper walks the engine's reservation map and
/// refunds any reservation older than `ttl`. Off by default via
/// `features.limit_reservation_ttl.enabled`; enabled unconditionally in
/// Phase 8 once the subscriber path is authoritative.
pub fn spawn_reservation_ttl_sweeper(
    engine: Arc<LimitEngine>,
    ttl: Duration,
    tick_interval: Duration,
) -> ReservationTtlSweeperHandle {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let engine = Arc::downgrade(&engine);
    let join = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(tick_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown_rx => break,
                _ = ticker.tick() => {
                    let Some(engine) = engine.upgrade() else { break };
                    sweep_expired(&engine, ttl);
                }
            }
        }
    });
    ReservationTtlSweeperHandle { shutdown_tx, join }
}

fn sweep_expired(engine: &LimitEngine, ttl: Duration) {
    let now = Instant::now();
    let mut evicted = 0u64;
    let expired_ids: Vec<ReservationId> = {
        let map = engine.inner.reservations.lock();
        map.iter()
            .filter(|(_, r)| now.duration_since(r.created_at) >= ttl)
            .map(|(k, _)| k.clone())
            .collect()
    };
    for id in expired_ids {
        if engine.refund_by_id(&id) {
            evicted += 1;
        }
    }
    if evicted > 0 {
        metrics::counter!("cc_lb_limit_reservation_ttl_evicted_total").increment(evicted);
    }
}

#[allow(non_snake_case)]
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cc_lb_storage_api::types::{
        PrincipalLimitIdentityKind, PrincipalLimitKind, PrincipalLimitState,
    };
    use cc_lb_testkit::InMemoryStorage;

    use super::*;

    #[test]
    fn record_principal_limit_state_feeds_limit_engine_snapshot() {
        let engine = LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            fixed_clock(1_700_000_000),
        );
        let observed_at_unix_secs = unix_secs(engine.inner.clock.now());

        engine.record_principal_limit_state(&PrincipalLimitState {
            principal_id: "principal-a".to_owned(),
            identity_kind: PrincipalLimitIdentityKind::Credential,
            identity_value: Some("key-a".to_owned()),
            account_observed: false,
            window: "1m".to_owned(),
            kind: PrincipalLimitKind::Requests,
            limit: Some(10),
            remaining: Some(7),
            reset: None,
            observed_at_unix_secs,
            stored_at_unix_secs: observed_at_unix_secs,
        });

        let view =
            PrincipalView::for_tests("principal-a", true, Vec::new(), Vec::new(), HashMap::new());
        let snapshot = engine.snapshot_for_principal(&view, "principal-a", IdentityFilter::All);
        let api_key_identity = snapshot
            .identities
            .iter()
            .find(|identity| identity.identity_kind == "api_key")
            .expect("api key identity should be recorded");
        let requests = api_key_identity.windows[0]
            .snapshots
            .iter()
            .find(|snapshot| snapshot.kind == "requests")
            .expect("requests snapshot should be recorded");

        assert_eq!(api_key_identity.identity_value.as_deref(), Some("key-a"));
        assert_eq!(requests.limit, Some(10));
        assert_eq!(requests.remaining, Some(7));
        assert!(requests.observed);
    }

    fn engine_with_output_token_limit(
        cap: i64,
    ) -> (Arc<LimitEngine>, PrincipalView, StoredApiKeyRecord) {
        let engine = LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            fixed_clock(1_700_000_000),
        );
        let default_limits = vec![cc_lb_storage_api::principal::Limit {
            kind: cc_lb_storage_api::principal::LimitKind::OutputTokens,
            window_secs: 60,
            cap_micros: cap,
        }];
        let view = PrincipalView::for_tests(
            "principal-a",
            true,
            vec!["claude-3-opus".to_owned()],
            default_limits,
            HashMap::new(),
        );
        let record = StoredApiKeyRecord {
            index_hash: [7u8; 32],
            ..StoredApiKeyRecord::default()
        };
        (engine, view, record)
    }

    fn fixed_clock(unix_secs: u64) -> ClockHandle {
        Arc::new(cc_lb_clock::TestClock::new_at_secs(unix_secs))
    }

    #[test]
    fn t1__reserve_rejects_disallowed_model() {
        let (engine, view, record) = engine_with_output_token_limit(1_000_000);

        assert_eq!(
            engine
                .reserve(&view, &record, "principal-a", "gpt-4", 10, 0, None)
                .err(),
            Some(RejectReason::ModelNotAllowed),
        );
    }

    #[test]
    fn t1__reserve_acquires_and_releases_concurrency_guard() {
        let engine = LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            fixed_clock(1_700_000_000),
        );
        let view = PrincipalView::for_tests(
            "principal-a",
            true,
            vec!["claude-3-opus".to_owned()],
            vec![cc_lb_storage_api::principal::Limit {
                kind: cc_lb_storage_api::principal::LimitKind::Concurrent,
                window_secs: 60,
                cap_micros: 1,
            }],
            HashMap::new(),
        );
        let record = StoredApiKeyRecord {
            index_hash: [8u8; 32],
            ..StoredApiKeyRecord::default()
        };

        let reservation = engine
            .reserve(&view, &record, "principal-a", "claude-3-opus", 0, 0, None)
            .expect("first reservation acquires the only concurrency slot");
        assert_eq!(
            engine
                .reserve(&view, &record, "principal-a", "claude-3-opus", 0, 0, None)
                .err(),
            Some(RejectReason::ConcurrentRateLimit),
        );

        drop(reservation);

        engine
            .reserve(&view, &record, "principal-a", "claude-3-opus", 0, 0, None)
            .expect("dropping the first reservation releases the concurrency slot");
    }

    #[test]
    fn reserve_returns_stable_unique_ids() {
        let (engine, view, record) = engine_with_output_token_limit(1_000_000);
        let a = engine
            .reserve(&view, &record, "principal-a", "claude-3-opus", 10, 0, None)
            .expect("reserve a");
        let b = engine
            .reserve(&view, &record, "principal-a", "claude-3-opus", 10, 0, None)
            .expect("reserve b");
        assert_ne!(a.id(), b.id());
        assert!(!a.id().is_empty());
        assert!(!b.id().is_empty());
    }

    #[test]
    fn reconcile_by_id_refunds_difference_and_removes_entry() {
        let (engine, view, record) = engine_with_output_token_limit(1_000_000);
        let key_id = key_id_for(&record);
        let now_sec = unix_secs(engine.inner.clock.now());
        let res = engine
            .reserve(&view, &record, "principal-a", "claude-3-opus", 100, 0, None)
            .expect("reserve");
        assert_eq!(
            engine.current_total(&key_id, LimitKind::OutputTokens, 60, now_sec),
            100,
        );
        let id = res.id().to_owned();
        std::mem::forget(res); // ensure Drop does not interfere with the refund path we assert

        assert!(engine.reconcile_by_id(&id, 0, 40, 0));
        assert_eq!(
            engine.current_total(&key_id, LimitKind::OutputTokens, 60, now_sec),
            40,
            "reserved 100, actual output 40 → 60 refunded → 40 remaining",
        );
        assert!(
            !engine.reconcile_by_id(&id, 0, 0, 0),
            "second call is idempotent no-op"
        );
    }

    #[test]
    fn refund_by_id_removes_full_reservation() {
        let (engine, view, record) = engine_with_output_token_limit(1_000_000);
        let key_id = key_id_for(&record);
        let now_sec = unix_secs(engine.inner.clock.now());
        let res = engine
            .reserve(&view, &record, "principal-a", "claude-3-opus", 55, 0, None)
            .expect("reserve");
        assert_eq!(
            engine.current_total(&key_id, LimitKind::OutputTokens, 60, now_sec),
            55,
        );
        let id = res.id().to_owned();
        std::mem::forget(res);
        assert!(engine.refund_by_id(&id));
        assert_eq!(
            engine.current_total(&key_id, LimitKind::OutputTokens, 60, now_sec),
            0,
        );
        assert!(!engine.refund_by_id(&id));
    }

    #[test]
    fn drop_refunds_when_neither_reconcile_nor_refund_called() {
        let (engine, view, record) = engine_with_output_token_limit(1_000_000);
        let key_id = key_id_for(&record);
        let now_sec = unix_secs(engine.inner.clock.now());
        {
            let _res = engine
                .reserve(&view, &record, "principal-a", "claude-3-opus", 25, 0, None)
                .expect("reserve");
            assert_eq!(
                engine.current_total(&key_id, LimitKind::OutputTokens, 60, now_sec),
                25,
            );
        }
        assert_eq!(
            engine.current_total(&key_id, LimitKind::OutputTokens, 60, now_sec),
            0,
            "Drop refunds full reservation on scope exit",
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn sweep_expired_refunds_stale_reservation() {
        let (engine, view, record) = engine_with_output_token_limit(1_000_000);
        let key_id = key_id_for(&record);
        let now_sec = unix_secs(engine.inner.clock.now());
        let res = engine
            .reserve(&view, &record, "principal-a", "claude-3-opus", 33, 0, None)
            .expect("reserve");
        let id = res.id().to_owned();
        std::mem::forget(res);
        // Advance the paused tokio clock past the TTL threshold so the
        // reservation's `created_at` is now considered expired.
        tokio::time::advance(Duration::from_millis(200)).await;
        sweep_expired(&engine, Duration::from_millis(50));
        assert_eq!(
            engine.current_total(&key_id, LimitKind::OutputTokens, 60, now_sec),
            0,
            "sweep_expired refunded {id}",
        );
        assert!(
            !engine.refund_by_id(&id),
            "sweep_expired removed the record from the map",
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn sweep_expired_leaves_recent_reservation_intact() {
        let (engine, view, record) = engine_with_output_token_limit(1_000_000);
        let key_id = key_id_for(&record);
        let now_sec = unix_secs(engine.inner.clock.now());
        let res = engine
            .reserve(&view, &record, "principal-a", "claude-3-opus", 33, 0, None)
            .expect("reserve");
        let id = res.id().to_owned();
        std::mem::forget(res);
        // Advance less than the TTL: reservation must survive the sweep.
        tokio::time::advance(Duration::from_millis(20)).await;
        sweep_expired(&engine, Duration::from_millis(50));
        assert_eq!(
            engine.current_total(&key_id, LimitKind::OutputTokens, 60, now_sec),
            33,
        );
        assert!(engine.refund_by_id(&id));
    }

    #[test]
    fn ring_counter_keeps_out_of_order_buckets_sorted_for_eviction() {
        let mut counter = RingCounter::new(60);
        counter.record(120, 1);
        counter.record(60, 2);

        assert_eq!(counter.current_total(121), 1);
        assert_eq!(counter.oldest_bucket_sec(121), Some(120));
    }

    #[test]
    fn key_limit_registry_keeps_key_ids_sorted_without_duplicates() {
        let mut registry = KeyLimitRegistry::default();
        let limit = Limit {
            kind: LimitKind::Requests,
            window_secs: 60,
            cap_micros: 10,
        };

        registry.record("key-c".to_owned(), vec![limit]);
        registry.record("key-a".to_owned(), vec![limit]);
        registry.record("key-b".to_owned(), vec![limit]);
        registry.record("key-b".to_owned(), vec![limit]);

        assert_eq!(registry.sorted_key_ids, ["key-a", "key-b", "key-c"]);
        assert_eq!(registry.by_key.len(), 3);
    }

    #[test]
    fn pending_usage_capacity_drops_oldest_entries_to_target() {
        let mut pending = HashMap::new();
        for (key_id, bucket_start_unix_secs) in [
            ("key-a", 100),
            ("key-b", 101),
            ("key-c", 102),
            ("key-d", 103),
        ] {
            pending.insert(
                ApiKeyUsageBucketKey {
                    key_id: key_id.to_owned(),
                    bucket_width_secs: 60,
                    bucket_start_unix_secs,
                },
                ApiKeyUsage {
                    requests: 1,
                    input_tokens: 0,
                    output_tokens: 0,
                    cost_usd_micros: 0,
                },
            );
        }

        bound_pending_usage_to(&mut pending, 1_000, 3, 2);

        let mut retained = pending
            .keys()
            .map(|key| key.key_id.as_str())
            .collect::<Vec<_>>();
        retained.sort_unstable();
        assert_eq!(retained, ["key-c", "key-d"]);
    }

    #[test]
    fn max_staleness_covers_two_refresh_cycles_and_remains_bounded() {
        assert_eq!(api_key_usage_max_staleness_secs(60, 1), 6);
        assert_eq!(api_key_usage_max_staleness_secs(60, 601), 14);

        assert_eq!(api_key_usage_max_staleness_secs(60, 100_000), 300);
    }

    fn complete_reserved_request(engine: &Arc<LimitEngine>, reservation: Reservation) {
        let id = reservation.id().to_owned();
        reservation.forget();
        assert!(engine.reconcile_by_id(&id, 0, 0, 0));
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn t2__durable_usage_coordinates_request_limits_across_engines() {
        let clock = fixed_clock(1_800_000_000);
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::with_clock(clock.clone()));
        let engine_a = LimitEngine::new(Arc::new(KeyConcurrencyManager::new()), clock.clone());
        let engine_b = LimitEngine::new(Arc::new(KeyConcurrencyManager::new()), clock.clone());
        engine_a
            .start_durable_usage_sync(storage.clone())
            .await
            .expect("start engine A durable usage")
            .shutdown()
            .await;
        engine_b
            .start_durable_usage_sync(storage.clone())
            .await
            .expect("start engine B durable usage")
            .shutdown()
            .await;

        let view = PrincipalView::for_tests(
            "principal-a",
            true,
            vec!["claude-3-opus".to_owned()],
            vec![Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: 10,
            }],
            HashMap::new(),
        );
        let record = StoredApiKeyRecord {
            index_hash: [11u8; 32],
            key_hash_b64: "durable-key".to_owned(),
            ..StoredApiKeyRecord::default()
        };
        let key_id = key_id_for(&record);
        let now_sec = unix_secs(clock.now());

        complete_reserved_request(
            &engine_a,
            engine_a
                .reserve(&view, &record, "principal-a", "claude-3-opus", 0, 0, None)
                .expect("engine A cold allowance"),
        );
        complete_reserved_request(
            &engine_b,
            engine_b
                .reserve(&view, &record, "principal-a", "claude-3-opus", 0, 0, None)
                .expect("engine B cold allowance"),
        );

        let state_a = engine_a
            .inner
            .durable_usage
            .read()
            .clone()
            .expect("engine A durable state");
        let state_b = engine_b
            .inner
            .durable_usage
            .read()
            .clone()
            .expect("engine B durable state");
        flush_durable_api_key_usage(&engine_a, &state_a, now_sec)
            .await
            .expect("flush engine A");
        flush_durable_api_key_usage(&engine_b, &state_b, now_sec)
            .await
            .expect("flush engine B");
        refresh_remote_api_key_usage(&engine_a, &state_a, now_sec)
            .await
            .expect("refresh engine A");
        refresh_remote_api_key_usage(&engine_b, &state_b, now_sec)
            .await
            .expect("refresh engine B");
        assert_eq!(
            engine_a.current_total(&key_id, LimitKind::Requests, 60, now_sec),
            2,
            "engine A should combine its local request with engine B's durable request",
        );
        assert_eq!(
            engine_b.current_total(&key_id, LimitKind::Requests, 60, now_sec),
            2,
            "engine B should combine its local request with engine A's durable request",
        );

        for _ in 0..8 {
            complete_reserved_request(
                &engine_a,
                engine_a
                    .reserve(&view, &record, "principal-a", "claude-3-opus", 0, 0, None)
                    .expect("shared request budget remains"),
            );
        }
        assert_eq!(
            engine_a
                .reserve(&view, &record, "principal-a", "claude-3-opus", 0, 0, None)
                .err(),
            Some(RejectReason::RequestsRateLimit),
        );

        flush_durable_api_key_usage(&engine_a, &state_a, now_sec)
            .await
            .expect("flush engine A final usage");
        refresh_remote_api_key_usage(&engine_b, &state_b, now_sec)
            .await
            .expect("refresh engine B final usage");
        assert_eq!(
            engine_b
                .reserve(&view, &record, "principal-a", "claude-3-opus", 0, 0, None)
                .err(),
            Some(RejectReason::RequestsRateLimit),
        );

        let buckets = storage
            .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
                key_ids: vec![key_id],
                since_unix_secs: now_sec.saturating_sub(60),
                until_unix_secs: now_sec,
                exclude_writer_epoch: Uuid::nil(),
            })
            .await
            .expect("query durable usage");
        assert_eq!(
            buckets
                .iter()
                .map(|bucket| bucket.usage.requests)
                .sum::<i64>(),
            10,
        );
    }
}
