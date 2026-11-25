use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{
    Arc, Weak,
    atomic::{AtomicU32, Ordering},
};
use std::time::Duration;
use tokio::time::Instant;

use cc_lb_storage_api::{
    ApiKeyConcurrencyHoldStore, ApiKeyUsage, ApiKeyUsageBucketDelta, ApiKeyUsageBucketKey,
    ApiKeyUsageBucketQuery, ApiKeyUsageFlush, ApiKeyUsageFlushResult, BackendKind,
    KeyStatus as StoredKeyStatus, MAX_LIMIT_WINDOW_SECS, Storage, StorageError, StoredApiKeyRecord,
};
use parking_lot::{Mutex, RwLock};
use uuid::Uuid;

use crate::api_keys::concurrent_guard::{
    DurableConcurrencyHold, KeyConcurrencyGuard, KeyConcurrencyManager,
};

use crate::api_keys::principal_view::{PrincipalStatus, PrincipalView};
use crate::api_keys::types::{Limit, LimitKind};
use cc_lb_clock::{ClockHandle, unix_secs};
use cc_lb_domain::Provider;
/// Stable identifier for a live [`Reservation`].
///
/// The engine stores reservation state by this ID so out-of-band consumers
/// (`LimitReconcileSubscriber`, TTL sweeper) can reconcile or refund without
/// holding the `Reservation` handle.
pub type ReservationId = String;

type RollingKey = (String, LimitKind, u64);
const API_KEY_USAGE_WRITER_LEASE_SECS: u64 = 30;
const API_KEY_USAGE_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const API_KEY_USAGE_REFRESH_BATCH_SIZE: usize = 250;
const API_KEY_USAGE_COLD_ALLOWANCE_DIVISOR: i64 = 10;
const API_KEY_USAGE_OUTAGE_CEILING_SECS: u64 = 300;
const API_KEY_USAGE_RETENTION_MARGIN_SECS: u64 = 3_600;
const API_KEY_USAGE_PENDING_MAX_ENTRIES: usize = 100_000;
/// Maximum age of a cluster-wide concurrency hold before it is treated as a
/// crash leftover (issue 807). Uses the writer lease window, floored at 90s
/// so live requests are not evicted while their writer still holds a lease.
const API_KEY_CONCURRENCY_HOLD_MAX_AGE_SECS: u64 = if API_KEY_USAGE_WRITER_LEASE_SECS > 90 {
    API_KEY_USAGE_WRITER_LEASE_SECS
} else {
    90
};
/// Interval at which live Postgres concurrency holds refresh `acquired_at_unix_secs`
/// (one-third of the 90s crash-leftover bound) so long streams stay counted across replicas.
const API_KEY_CONCURRENCY_HOLD_REFRESH_INTERVAL_SECS: u64 = 30;
const API_KEY_USAGE_PENDING_TARGET_ENTRIES: usize = 90_000;
const API_KEY_USAGE_WRITER_INACTIVE_AFTER_SECS: u64 = 60;
const API_KEY_USAGE_COMPACTION_BATCH_SIZE: usize = 1_000;
const API_KEY_USAGE_FINAL_FLUSH_MAX_ATTEMPTS: usize = 3;

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
    backend_kind: BackendKind,
    writer_epoch: RwLock<Uuid>,
    pending: Mutex<HashMap<ApiKeyUsageBucketKey, ApiKeyUsage>>,
    in_flight: Mutex<Option<ApiKeyUsageFlush>>,
    key_limits: RwLock<KeyLimitRegistry>,
    refreshed_at: RwLock<HashMap<String, u64>>,
    refresh_cursor: AtomicU32,
    cold_hydrations: Mutex<HashMap<String, tokio::sync::watch::Receiver<bool>>>,
}

#[derive(Default)]
struct KeyLimitRegistry {
    by_key: HashMap<String, Vec<Limit>>,
    sorted_key_ids: Vec<String>,
    last_seen: HashMap<String, u64>,
    cold_keys: VecDeque<String>,
    cold_set: HashSet<String>,
}

impl KeyLimitRegistry {
    #[cfg(test)]
    fn record(&mut self, key_id: String, limits: Vec<Limit>) {
        self.record_at(key_id, limits, 0, true);
    }

    fn record_at(&mut self, key_id: String, limits: Vec<Limit>, now_sec: u64, is_cold: bool) {
        if !self.by_key.contains_key(&key_id) {
            let index = self
                .sorted_key_ids
                .binary_search(&key_id)
                .unwrap_or_else(|index| index);
            self.sorted_key_ids.insert(index, key_id.clone());
        }
        self.by_key.insert(key_id.clone(), limits);
        if now_sec > 0 {
            self.last_seen.insert(key_id.clone(), now_sec);
        }
        if is_cold && self.cold_set.insert(key_id.clone()) {
            self.cold_keys.push_back(key_id);
        }
    }

    fn mark_hydrated(&mut self, key_id: &str) {
        self.cold_set.remove(key_id);
    }

    fn evict_idle(&mut self, now_sec: u64) -> Vec<String> {
        let expired: Vec<String> = self
            .last_seen
            .iter()
            .filter_map(|(key_id, seen)| {
                if *seen == 0 {
                    return None;
                }
                let max_window = self
                    .by_key
                    .get(key_id)
                    .and_then(|limits| {
                        limits
                            .iter()
                            .filter(|limit| limit.kind != LimitKind::Concurrent)
                            .map(|limit| limit.window_secs)
                            .max()
                    })
                    .unwrap_or(3_600)
                    .max(3_600);
                (now_sec.saturating_sub(*seen) > max_window).then(|| key_id.clone())
            })
            .collect();
        if expired.is_empty() {
            return expired;
        }
        let expired_set: HashSet<&str> = expired.iter().map(String::as_str).collect();
        for key_id in &expired {
            self.by_key.remove(key_id);
            self.last_seen.remove(key_id);
            self.cold_set.remove(key_id);
        }
        self.sorted_key_ids
            .retain(|key_id| !expired_set.contains(key_id.as_str()));
        self.cold_keys
            .retain(|key_id| !expired_set.contains(key_id.as_str()));
        expired
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
    /// `None` leaves the monetary accumulator unchanged while retaining
    /// measured request/token usage. It does not assert a known zero price.
    fn record(
        &self,
        key_id: &str,
        bucket_widths: &[u64],
        usage: ApiKeyUsage,
        cost_liability_micros: Option<i64>,
        now_sec: u64,
    ) {
        let mut pending = self.pending.lock();
        for bucket_width_secs in bucket_widths {
            let bucket_start_unix_secs = now_sec - now_sec % bucket_width_secs;
            let key = ApiKeyUsageBucketKey {
                key_id: key_id.to_owned(),
                bucket_width_secs: *bucket_width_secs,
                bucket_start_unix_secs,
            };
            let aggregate = pending.entry(key).or_default();
            let cost = aggregate.cost_usd_micros;
            add_usage(aggregate, usage);
            aggregate.cost_usd_micros = match cost_liability_micros {
                Some(liability) => cost.saturating_add(liability),
                None => cost,
            };
        }
        bound_pending_usage(&mut pending, now_sec);
    }

    #[cfg(test)]
    fn record_key_limits(&self, key_id: String, limits: Vec<Limit>) {
        self.record_key_limits_at(key_id, limits, 0);
    }

    fn record_key_limits_at(&self, key_id: String, limits: Vec<Limit>, now_sec: u64) {
        let is_cold = self.refreshed_at.read().get(&key_id).is_none();
        self.key_limits
            .write()
            .record_at(key_id, limits, now_sec, is_cold);
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

    /// Whether the engine still owns this reservation after orphan sweeping.
    pub fn is_active(&self) -> bool {
        self.engine.upgrade().is_some_and(|engine| {
            let reservations = engine.reservations.lock();
            reservations.contains_key(&self.id)
        })
    }

    /// Suppress the RAII refund on drop.
    ///
    /// The handler hands `id` off to the reconcile subscriber
    /// via `LifecycleEvent::LimitDecision::Reserved` and no longer holds the
    /// reservation to completion. Without this suppressor, dropping the
    /// handle would race the subscriber and full-refund. The TTL sweeper is
    /// the safety net when neither the subscriber nor `forget` ever runs.
    pub fn forget(mut self) {
        self.forgotten = true;
    }

    /// Release only this reservation's concurrent-request guards while keeping
    /// token and cost liability open until terminal reconciliation or expiry.
    pub fn release_concurrent_guards(&self) {
        let guards = self.engine.upgrade().and_then(|engine| {
            let mut reservations = engine.reservations.lock();
            reservations
                .get_mut(&self.id)
                .map(|record| std::mem::take(&mut record.concurrent_guards))
        });
        // Durable hold destruction may enqueue its existing storage cleanup.
        // Do not run it while holding the reservation map lock.
        drop(guards);
    }

    /// Hand a finished response's usage/cost liability to its subscriber.
    ///
    /// Concurrency ends at the response boundary, not when asynchronous pricing
    /// arrives. Keep token/cost reservations alive, but release only this
    /// response's concurrent guards before another turn can be admitted.
    pub fn handoff_after_response(mut self) {
        self.release_concurrent_guards();
        self.forgotten = true;
    }
}

/// Engine-side reservation state. Stored inside
/// [`LimitEngineInner::reservations`] and looked up by [`ReservationId`].
///
/// Kept outside `Reservation` so out-of-band consumers can reconcile without
/// owning the handle. `Reservation` is an RAII refund guard keyed by `id`.
struct ReservationRecord {
    key_id: String,
    reserved: Vec<ReservedAmount>,
    /// One request's admitted estimates, independent of its per-window holds.
    admitted_input_estimate: i64,
    admitted_output_estimate: i64,
    admitted_cost_estimate_micros: Option<i64>,
    durable_bucket_widths: Vec<u64>,
    /// Held until refund/reconcile removes the record, or an explicit response
    /// handoff releases concurrency while retaining token/cost liability.
    concurrent_guards: Vec<ConcurrentSlot>,
    created_at: Instant,
    last_activity_at: Instant,
}

/// A concurrent-request slot held by a [`ReservationRecord`]: either the
/// local in-process counter or a cluster-wide Postgres hold row (issue 807).
#[allow(dead_code)]
enum ConcurrentSlot {
    Local(KeyConcurrencyGuard),
    Durable(DurableConcurrencyHold),
}

#[derive(Clone)]
struct ReservedAmount {
    kind: LimitKind,
    window_sec: u64,
    amount: i64,
    bucket_start: u64,
}

impl ReservedAmount {
    fn is_live_at(&self, now_sec: u64) -> bool {
        self.bucket_start.saturating_add(self.window_sec) > now_sec
    }
}

fn rolling_bucket_start(now_sec: u64, window_sec: u64) -> u64 {
    let bucket_width = durable_bucket_width(window_sec).unwrap_or(window_sec.max(1));
    now_sec - now_sec % bucket_width
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RejectReason {
    PrincipalMissing,
    PrincipalDisabled,
    KeyRevoked,
    Expired,
    ModelNotAllowed,
    RequestsRateLimit,
    TokenRateLimit {
        kind: LimitKind,
    },
    CostRateLimit,
    ConcurrentRateLimit,
    ConcurrentLimitUnavailable,
    CostUnavailable,
    UnsupportedLimitWindow {
        window_secs: u64,
    },
    InvalidOutputCap {
        requested: i64,
    },
    OutputCapExceeded {
        cap: i64,
        requested: i64,
    },
    OutputCapRequired {
        cap: i64,
    },
    Unsatisfiable {
        kind: LimitKind,
        cap: i64,
        requested: i64,
    },
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
        let bucket_start = rolling_bucket_start(now_sec, self.window_sec);
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

    fn remove_from_bucket(&mut self, bucket_start: u64, amount: i64, now_sec: u64) {
        self.evict_old(now_sec);
        if bucket_start.saturating_add(self.window_sec) <= now_sec {
            return;
        }
        if let Some(index) = self
            .buckets
            .iter()
            .position(|(existing_start, _)| *existing_start == bucket_start)
        {
            self.buckets[index].1 = self.buckets[index].1.saturating_sub(amount);
            if self.buckets[index].1 == 0 {
                self.buckets.remove(index);
            }
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
        let backend_kind = storage.backend_kind().await?;
        let state = Arc::new(DurableApiKeyUsage {
            storage,
            backend_kind,
            writer_epoch: RwLock::new(writer_epoch),
            pending: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(None),
            key_limits: RwLock::new(KeyLimitRegistry::default()),
            refreshed_at: RwLock::new(HashMap::new()),
            refresh_cursor: AtomicU32::new(0),
            cold_hydrations: Mutex::new(HashMap::new()),
        });
        *self.inner.durable_usage.write() = Some(state.clone());

        let cancel = tokio_util::sync::CancellationToken::new();
        let task_cancel = cancel.clone();
        let engine = self.clone();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(API_KEY_USAGE_FLUSH_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut last_hold_refresh_sec: Option<u64> = None;
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
                        if state.backend_kind == BackendKind::Postgres {
                            maintain_durable_concurrency_holds(
                                &engine,
                                state.storage.as_ref(),
                                now_sec,
                                &mut last_hold_refresh_sec,
                            )
                            .await;
                        }
                    }
                }
            }
        });
        Ok(ApiKeyUsageHandle { cancel, task })
    }

    pub const fn durable_usage_retention_secs() -> u64 {
        MAX_LIMIT_WINDOW_SECS + API_KEY_USAGE_RETENTION_MARGIN_SECS
    }

    pub const fn durable_usage_writer_inactive_after_secs() -> u64 {
        API_KEY_USAGE_WRITER_INACTIVE_AFTER_SECS
    }

    pub const fn durable_usage_compaction_batch_size() -> usize {
        API_KEY_USAGE_COMPACTION_BATCH_SIZE
    }

    /// An empty model denotes an operation with no model (for example, a
    /// catalog lookup). It skips only model authorization; all other admission
    /// checks and reservations still apply.
    #[allow(clippy::too_many_arguments)]
    pub fn reserve(
        self: &Arc<Self>,
        view: &PrincipalView,
        record: &StoredApiKeyRecord,
        principal_id: &str,
        model: &str,
        max_tokens: Option<i64>,
        max_input_estimate: i64,
        cost_estimate_micros: Option<i64>,
    ) -> Result<Reservation, RejectReason> {
        let plan = self.prepare_reservation_plan(
            view,
            record,
            principal_id,
            model,
            max_tokens,
            max_input_estimate,
            cost_estimate_micros,
        )?;
        let caps = self.compute_admission_caps(
            plan.durable_usage.as_ref(),
            &plan.key_id,
            &plan.planned_rolling,
            plan.now_sec,
        );
        self.precheck_rolling_limits(&plan.key_id, &plan.planned_rolling, &caps, plan.now_sec)?;

        let mut concurrent_guards = Vec::with_capacity(plan.concurrent_caps.len());
        for cap in &plan.concurrent_caps {
            let slot = match plan.durable_usage.as_ref() {
                Some(state) if state.backend_kind == BackendKind::Postgres => {
                    match self.try_durable_concurrent_hold(state, &plan.key_id, *cap) {
                        Ok(hold) => ConcurrentSlot::Durable(hold),
                        Err(DurableHoldError::OverCap) => {
                            drop(concurrent_guards);
                            return Err(RejectReason::ConcurrentRateLimit);
                        }
                        Err(DurableHoldError::Unavailable) => {
                            drop(concurrent_guards);
                            return Err(RejectReason::ConcurrentLimitUnavailable);
                        }
                    }
                }
                _ => match self.inner.concurrent_mgr.try_acquire(&plan.key_id, *cap) {
                    Ok(guard) => ConcurrentSlot::Local(guard),
                    Err(_) => {
                        drop(concurrent_guards);
                        return Err(RejectReason::ConcurrentRateLimit);
                    }
                },
            };
            concurrent_guards.push(slot);
        }

        let reservation = match self.commit_reservation_atomic(
            &plan.key_id,
            &plan.planned_rolling,
            &caps,
            concurrent_guards,
            plan.estimated_input_tokens,
            plan.estimated_output_tokens,
            cost_estimate_micros,
            plan.durable_bucket_widths,
            plan.now_sec,
        ) {
            Ok(reservation) => reservation,
            Err((reason, guards)) => {
                drop(guards);
                return Err(reason);
            }
        };
        self.inner.effective_limits.write().insert(
            (plan.key_id, principal_id.to_owned()),
            plan.effective_limits,
        );
        Ok(reservation)
    }

    /// Asynchronous admission path used by request handlers.
    ///
    /// Performs singleflight remote usage hydration for cold keys before
    /// computing admission caps and runs Postgres concurrency holds directly
    /// on the runtime without `block_in_place`. Fails closed on Postgres hold
    /// errors.
    #[allow(clippy::too_many_arguments)]
    pub async fn reserve_async(
        self: &Arc<Self>,
        view: &PrincipalView,
        record: &StoredApiKeyRecord,
        principal_id: &str,
        model: &str,
        max_tokens: Option<i64>,
        max_input_estimate: i64,
        cost_estimate_micros: Option<i64>,
    ) -> Result<Reservation, RejectReason> {
        let plan = self.prepare_reservation_plan(
            view,
            record,
            principal_id,
            model,
            max_tokens,
            max_input_estimate,
            cost_estimate_micros,
        )?;
        if let Some(state) = plan.durable_usage.as_ref()
            && !plan.planned_rolling.is_empty()
        {
            hydrate_cold_key_usage(
                self,
                state,
                &plan.key_id,
                &plan.effective_limits,
                plan.now_sec,
            )
            .await;
        }
        let caps = self.compute_admission_caps(
            plan.durable_usage.as_ref(),
            &plan.key_id,
            &plan.planned_rolling,
            plan.now_sec,
        );
        self.precheck_rolling_limits(&plan.key_id, &plan.planned_rolling, &caps, plan.now_sec)?;

        let mut concurrent_guards = Vec::with_capacity(plan.concurrent_caps.len());
        for cap in &plan.concurrent_caps {
            let slot = match plan.durable_usage.as_ref() {
                Some(state) if state.backend_kind == BackendKind::Postgres => {
                    match self
                        .try_durable_concurrent_hold_async(state, &plan.key_id, *cap)
                        .await
                    {
                        Ok(hold) => ConcurrentSlot::Durable(hold),
                        Err(DurableHoldError::OverCap) => {
                            drop(concurrent_guards);
                            return Err(RejectReason::ConcurrentRateLimit);
                        }
                        Err(DurableHoldError::Unavailable) => {
                            drop(concurrent_guards);
                            return Err(RejectReason::ConcurrentLimitUnavailable);
                        }
                    }
                }
                _ => match self.inner.concurrent_mgr.try_acquire(&plan.key_id, *cap) {
                    Ok(guard) => ConcurrentSlot::Local(guard),
                    Err(_) => {
                        drop(concurrent_guards);
                        return Err(RejectReason::ConcurrentRateLimit);
                    }
                },
            };
            concurrent_guards.push(slot);
        }

        let reservation = match self.commit_reservation_atomic(
            &plan.key_id,
            &plan.planned_rolling,
            &caps,
            concurrent_guards,
            plan.estimated_input_tokens,
            plan.estimated_output_tokens,
            cost_estimate_micros,
            plan.durable_bucket_widths,
            plan.now_sec,
        ) {
            Ok(reservation) => reservation,
            Err((reason, guards)) => {
                drop(guards);
                return Err(reason);
            }
        };
        self.inner.effective_limits.write().insert(
            (plan.key_id, principal_id.to_owned()),
            plan.effective_limits,
        );
        Ok(reservation)
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_reservation_plan(
        &self,
        view: &PrincipalView,
        record: &StoredApiKeyRecord,
        principal_id: &str,
        model: &str,
        max_tokens: Option<i64>,
        max_input_estimate: i64,
        cost_estimate_micros: Option<i64>,
    ) -> Result<ReservationPlan, RejectReason> {
        let now_sec = unix_secs(self.inner.clock.now());
        let key_id = key_id_for(record);

        match view.principal_status(principal_id) {
            PrincipalStatus::Missing => return Err(RejectReason::PrincipalMissing),
            PrincipalStatus::Disabled => return Err(RejectReason::PrincipalDisabled),
            PrincipalStatus::Active => {}
        }

        match record.status {
            StoredKeyStatus::Active => {}
            StoredKeyStatus::Revoked => return Err(RejectReason::KeyRevoked),
        }

        if record
            .expires_at_unix_secs
            .is_some_and(|expires_at| now_sec > expires_at)
        {
            return Err(RejectReason::Expired);
        }

        if let Some(requested) = max_tokens.filter(|value| *value < 0) {
            return Err(RejectReason::InvalidOutputCap { requested });
        }

        if !model.is_empty() && !view.is_model_allowed(principal_id, model) {
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
            state.record_key_limits_at(key_id.clone(), effective_limits.clone(), now_sec);
        }

        // Check configured capacity before transient usage or concurrency can
        // mask a request that cannot fit even an empty window.
        for limit in &effective_limits {
            let requested = match limit.kind {
                LimitKind::Requests => 1,
                LimitKind::OutputTokens => match max_tokens {
                    Some(value) => value,
                    None => {
                        return Err(RejectReason::OutputCapRequired {
                            cap: limit.cap_micros,
                        });
                    }
                },
                LimitKind::InputTokens => max_input_estimate,
                LimitKind::TotalTokens => {
                    max_input_estimate.saturating_add(max_tokens.unwrap_or(0))
                }
                LimitKind::CostUsd => match cost_estimate_micros {
                    Some(amount) => amount,
                    None => continue,
                },
                LimitKind::Concurrent => continue,
            };
            if requested > limit.cap_micros {
                return Err(match limit.kind {
                    LimitKind::OutputTokens => RejectReason::OutputCapExceeded {
                        cap: limit.cap_micros,
                        requested,
                    },
                    kind => RejectReason::Unsatisfiable {
                        kind,
                        cap: limit.cap_micros,
                        requested,
                    },
                });
            }
        }

        let mut planned_rolling = Vec::new();
        let mut concurrent_caps = Vec::new();
        for limit in &effective_limits {
            match limit.kind {
                LimitKind::Requests => planned_rolling.push((*limit, 1)),
                LimitKind::OutputTokens => {
                    planned_rolling.push((*limit, max_tokens.unwrap_or(0)));
                }
                LimitKind::InputTokens => planned_rolling.push((*limit, max_input_estimate)),
                LimitKind::TotalTokens => {
                    planned_rolling.push((
                        *limit,
                        max_input_estimate.saturating_add(max_tokens.unwrap_or(0)),
                    ));
                }
                LimitKind::CostUsd => {
                    let Some(amount) = cost_estimate_micros else {
                        return Err(RejectReason::CostUnavailable);
                    };
                    planned_rolling.push((*limit, amount));
                }
                LimitKind::Concurrent => {
                    concurrent_caps
                        .push(u32::try_from(limit.cap_micros.max(0)).unwrap_or(u32::MAX));
                }
            }
        }

        Ok(ReservationPlan {
            key_id,
            effective_limits,
            durable_usage,
            durable_bucket_widths,
            planned_rolling,
            concurrent_caps,
            estimated_input_tokens: max_input_estimate.max(0),
            estimated_output_tokens: max_tokens.unwrap_or(0).max(0),
            now_sec,
        })
    }

    fn compute_admission_caps(
        &self,
        durable_usage: Option<&Arc<DurableApiKeyUsage>>,
        key_id: &str,
        planned_rolling: &[(Limit, i64)],
        now_sec: u64,
    ) -> Vec<i64> {
        planned_rolling
            .iter()
            .map(|(limit, _)| self.admission_cap_with_state(durable_usage, key_id, limit, now_sec))
            .collect()
    }

    fn precheck_rolling_limits(
        &self,
        key_id: &str,
        planned_rolling: &[(Limit, i64)],
        caps: &[i64],
        now_sec: u64,
    ) -> Result<(), RejectReason> {
        if planned_rolling.is_empty() {
            return Ok(());
        }
        let rolling = self.inner.rolling.read();
        let remote = self.inner.remote_rolling.read();
        for ((limit, amount), cap) in planned_rolling.iter().zip(caps.iter()) {
            let key = (key_id.to_owned(), limit.kind, limit.window_secs);
            let local = rolling.get(&key).map_or(0, |c| c.total_at(now_sec));
            let remote_total = remote.get(&key).map_or(0, |c| c.total_at(now_sec));
            if local.saturating_add(remote_total).saturating_add(*amount) > *cap {
                return Err(reject_reason_for_limit_kind(limit.kind));
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_reservation_atomic(
        self: &Arc<Self>,
        key_id: &str,
        planned_rolling: &[(Limit, i64)],
        caps: &[i64],
        concurrent_guards: Vec<ConcurrentSlot>,
        admitted_input_estimate: i64,
        admitted_output_estimate: i64,
        cost_estimate_micros: Option<i64>,
        durable_bucket_widths: Vec<u64>,
        now_sec: u64,
    ) -> Result<Reservation, (RejectReason, Vec<ConcurrentSlot>)> {
        let mut reserved = Vec::with_capacity(planned_rolling.len());
        let id = Uuid::now_v7().to_string();
        let now = Instant::now();
        {
            let mut reservations = self.inner.reservations.lock();
            {
                let mut rolling = self.inner.rolling.write();
                {
                    let remote = self.inner.remote_rolling.read();
                    for ((limit, amount), cap) in planned_rolling.iter().zip(caps.iter()) {
                        let key = (key_id.to_owned(), limit.kind, limit.window_secs);
                        let local = rolling.get(&key).map_or(0, |c| c.total_at(now_sec));
                        let remote_total = remote.get(&key).map_or(0, |c| c.total_at(now_sec));
                        if local.saturating_add(remote_total).saturating_add(*amount) > *cap {
                            drop(remote);
                            drop(rolling);
                            drop(reservations);
                            return Err((
                                reject_reason_for_limit_kind(limit.kind),
                                concurrent_guards,
                            ));
                        }
                    }
                }
                for (limit, amount) in planned_rolling {
                    record_amount_locked(
                        &mut rolling,
                        key_id,
                        limit.kind,
                        limit.window_secs,
                        *amount,
                        now_sec,
                    );
                    reserved.push(ReservedAmount {
                        kind: limit.kind,
                        window_sec: limit.window_secs,
                        amount: *amount,
                        bucket_start: rolling_bucket_start(now_sec, limit.window_secs),
                    });
                }
            }
            reservations.insert(
                id.clone(),
                ReservationRecord {
                    key_id: key_id.to_owned(),
                    reserved,
                    admitted_input_estimate,
                    admitted_output_estimate,
                    admitted_cost_estimate_micros: cost_estimate_micros,
                    durable_bucket_widths,
                    concurrent_guards,
                    created_at: now,
                    last_activity_at: now,
                },
            );
        }

        Ok(Reservation {
            engine: Arc::downgrade(&self.inner),
            id,
            forgotten: false,
        })
    }

    /// Acquire a cluster-wide concurrency slot on Postgres synchronously via
    /// [`block_on_storage`]. Fails closed on storage or runtime unavailability.
    fn try_durable_concurrent_hold(
        &self,
        state: &Arc<DurableApiKeyUsage>,
        key_id: &str,
        cap: u32,
    ) -> Result<DurableConcurrencyHold, DurableHoldError> {
        let storage = Arc::clone(&state.storage);
        let writer_epoch = *state.writer_epoch.read();
        let hold_id = Uuid::now_v7();
        let key_id_owned = key_id.to_owned();
        let Some(outcome) = block_on_storage({
            let storage = Arc::clone(&storage);
            let key_id_for_query = key_id_owned.clone();
            async move {
                try_acquire_hold_atomic(
                    storage.as_ref(),
                    hold_id,
                    &key_id_for_query,
                    writer_epoch,
                    cap,
                )
                .await
            }
        }) else {
            return Err(DurableHoldError::Unavailable);
        };
        match outcome {
            Ok(true) => Ok(DurableConcurrencyHold::new(storage, hold_id, key_id_owned)),
            Ok(false) => Err(DurableHoldError::OverCap),
            Err(error) => {
                tracing::warn!(%error, %key_id_owned, "durable API-key concurrency hold failed");
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    let cleanup_storage = Arc::clone(&storage);
                    let cleanup_key_id = key_id_owned;
                    handle.spawn(async move {
                        if let Err(cleanup_err) = cleanup_storage
                            .delete_uncertain_api_key_concurrency_hold(hold_id, &cleanup_key_id)
                            .await
                        {
                            tracing::warn!(%cleanup_err, %hold_id, %cleanup_key_id, "failed to clean up uncertain durable concurrency hold");
                        }
                    });
                }
                Err(DurableHoldError::Unavailable)
            }
        }
    }

    /// Acquire a cluster-wide concurrency slot on Postgres asynchronously.
    /// Fails closed on storage unavailability or caller cancellation.
    async fn try_durable_concurrent_hold_async(
        &self,
        state: &Arc<DurableApiKeyUsage>,
        key_id: &str,
        cap: u32,
    ) -> Result<DurableConcurrencyHold, DurableHoldError> {
        let writer_epoch = *state.writer_epoch.read();
        let hold_id = acquire_durable_hold_raw(&state.storage, writer_epoch, key_id, cap).await?;
        Ok(DurableConcurrencyHold::new(
            Arc::clone(&state.storage),
            hold_id,
            key_id.to_owned(),
        ))
    }

    pub fn reconcile(
        &self,
        reservation: Reservation,
        actual_input: u64,
        actual_output: u64,
        actual_cost_micros: Option<i64>,
    ) {
        self.reconcile_by_id(
            &reservation.id,
            actual_input,
            actual_output,
            actual_cost_micros,
        );
    }

    /// Collects hold IDs for all currently tracked in-flight durable
    /// concurrency guards so the background sync loop can renew their leases.
    fn live_durable_concurrency_hold_ids(&self) -> Vec<Uuid> {
        let reservations = self.inner.reservations.lock();
        reservations
            .values()
            .flat_map(|record| record.concurrent_guards.iter())
            .filter_map(|slot| match slot {
                ConcurrentSlot::Durable(hold) => Some(hold.hold_id()),
                ConcurrentSlot::Local(_) => None,
            })
            .collect()
    }

    /// Refresh the sliding activity timestamp for an active reservation so
    /// long-running streams are not prematurely evicted by the idle TTL.
    pub fn touch_reservation_by_id(&self, id: &str) -> bool {
        let mut map = self.inner.reservations.lock();
        let Some(record) = map.get_mut(id) else {
            return false;
        };
        record.last_activity_at = Instant::now();
        true
    }

    /// Resize an active reservation's rolling token and cost holds after the
    /// final request body is settled, while keeping request counts and
    /// concurrency guards from admission intact. Returns `Ok(false)` if the
    /// reservation expired or was already reconciled.
    #[allow(clippy::too_many_arguments)]
    pub fn resize_reservation_estimates_by_id(
        &self,
        view: &PrincipalView,
        record: &StoredApiKeyRecord,
        principal_id: &str,
        id: &str,
        model: &str,
        max_tokens: Option<i64>,
        max_input_estimate: i64,
        cost_estimate_micros: Option<i64>,
    ) -> Result<bool, RejectReason> {
        let plan = self.prepare_reservation_plan(
            view,
            record,
            principal_id,
            model,
            max_tokens,
            max_input_estimate,
            cost_estimate_micros,
        )?;
        let caps = self.compute_admission_caps(
            plan.durable_usage.as_ref(),
            &plan.key_id,
            &plan.planned_rolling,
            plan.now_sec,
        );
        {
            let mut reservations = self.inner.reservations.lock();
            let Some(res) = reservations.get_mut(id) else {
                return Ok(false);
            };
            {
                let mut rolling = self.inner.rolling.write();
                {
                    let remote = self.inner.remote_rolling.read();
                    for ((limit, new_amount), cap) in plan.planned_rolling.iter().zip(caps.iter()) {
                        let live_old_amount = res
                            .reserved
                            .iter()
                            .find(|r| r.kind == limit.kind && r.window_sec == limit.window_secs)
                            .filter(|r| r.is_live_at(plan.now_sec))
                            .map_or(0, |r| r.amount);
                        let delta = new_amount.saturating_sub(live_old_amount);
                        if delta > 0 {
                            let key = (res.key_id.clone(), limit.kind, limit.window_secs);
                            let local = rolling.get(&key).map_or(0, |c| c.total_at(plan.now_sec));
                            let remote_total =
                                remote.get(&key).map_or(0, |c| c.total_at(plan.now_sec));
                            if local.saturating_add(remote_total).saturating_add(delta) > *cap {
                                return Err(reject_reason_for_limit_kind(limit.kind));
                            }
                        }
                    }
                }
                for old in &res.reserved {
                    if old.is_live_at(plan.now_sec)
                        && !plan.planned_rolling.iter().any(|(limit, _)| {
                            limit.kind == old.kind && limit.window_secs == old.window_sec
                        })
                    {
                        remove_reserved_bucket_locked(&mut rolling, &res.key_id, old, plan.now_sec);
                    }
                }
                let mut next_reserved = Vec::with_capacity(plan.planned_rolling.len());
                for (limit, new_amount) in &plan.planned_rolling {
                    let live_old = res
                        .reserved
                        .iter()
                        .find(|r| r.kind == limit.kind && r.window_sec == limit.window_secs)
                        .filter(|r| r.is_live_at(plan.now_sec));
                    if let Some(old) = live_old {
                        if old.amount == *new_amount {
                            next_reserved.push(old.clone());
                            continue;
                        }
                        remove_reserved_bucket_locked(&mut rolling, &res.key_id, old, plan.now_sec);
                    }
                    if *new_amount != 0 {
                        record_amount_locked(
                            &mut rolling,
                            &res.key_id,
                            limit.kind,
                            limit.window_secs,
                            *new_amount,
                            plan.now_sec,
                        );
                    }
                    next_reserved.push(ReservedAmount {
                        kind: limit.kind,
                        window_sec: limit.window_secs,
                        amount: *new_amount,
                        bucket_start: rolling_bucket_start(plan.now_sec, limit.window_secs),
                    });
                }
                res.reserved = next_reserved;
                res.admitted_input_estimate = plan.estimated_input_tokens;
                res.admitted_output_estimate = plan.estimated_output_tokens;
                res.admitted_cost_estimate_micros = cost_estimate_micros;
                res.durable_bucket_widths = plan.durable_bucket_widths;
                res.last_activity_at = Instant::now();
            }
        }
        self.inner.effective_limits.write().insert(
            (plan.key_id, principal_id.to_owned()),
            plan.effective_limits,
        );
        Ok(true)
    }

    /// Reconcile a reservation by ID.
    ///
    /// Used by the `LimitReconcileSubscriber` to reconcile out of
    /// band with the handler. Returns `true` if the reservation was found
    /// and reconciled, `false` if the ID is unknown (already reconciled,
    /// refunded, TTL-evicted, or never issued).
    ///
    /// A known cost (including zero) settles actual usage. An unknown cost
    /// retains the admitted estimate as liability without claiming an actual
    /// request price. If neither exists, measured request/token usage still
    /// settles, and the monetary liability is explicitly unavailable.
    pub fn reconcile_by_id(
        &self,
        id: &str,
        actual_input: u64,
        actual_output: u64,
        actual_cost_micros: Option<i64>,
    ) -> bool {
        let durable_usage = self.inner.durable_usage.read().clone();
        let concurrent_guards = {
            let mut reservations = self.inner.reservations.lock();
            let Some(mut record) = reservations.remove(id) else {
                return false;
            };
            let concurrent_guards = std::mem::take(&mut record.concurrent_guards);
            let now_sec = unix_secs(self.inner.clock.now());
            let cost_liability_micros = actual_cost_micros.or(record.admitted_cost_estimate_micros);
            if let Some(state) = durable_usage.as_ref() {
                state.record(
                    &record.key_id,
                    &record.durable_bucket_widths,
                    ApiKeyUsage {
                        requests: 1,
                        input_tokens: u64_to_i64_saturating(actual_input),
                        output_tokens: u64_to_i64_saturating(actual_output),
                        ..Default::default()
                    },
                    cost_liability_micros,
                    now_sec,
                );
            }
            {
                let mut rolling = self.inner.rolling.write();
                refund_difference_locked(
                    &mut rolling,
                    &record.key_id,
                    &record.reserved,
                    actual_input,
                    actual_output,
                    cost_liability_micros,
                    now_sec,
                );
            }
            concurrent_guards
        };
        drop(concurrent_guards);
        true
    }

    /// Commit a reservation without altering its already-recorded rolling
    /// usage (e.g. an HTTP stream terminated after content began without a
    /// usage event). Releases concurrency guards and records durable usage.
    pub fn commit_reserved_by_id(&self, id: &str) -> bool {
        let durable_usage = self.inner.durable_usage.read().clone();
        let concurrent_guards = {
            let mut reservations = self.inner.reservations.lock();
            let Some(mut record) = reservations.remove(id) else {
                return false;
            };
            let concurrent_guards = std::mem::take(&mut record.concurrent_guards);
            let now_sec = unix_secs(self.inner.clock.now());
            if let Some(state) = durable_usage.as_ref()
                && !record.durable_bucket_widths.is_empty()
            {
                state.record(
                    &record.key_id,
                    &record.durable_bucket_widths,
                    ApiKeyUsage {
                        requests: 1,
                        input_tokens: record.admitted_input_estimate,
                        output_tokens: record.admitted_output_estimate,
                        cost_usd_micros: 0,
                    },
                    record.admitted_cost_estimate_micros,
                    now_sec,
                );
            }
            concurrent_guards
        };
        drop(concurrent_guards);
        true
    }

    /// Refund a reservation in full by ID.
    ///
    /// Used by the TTL sweeper and by out-of-band callers that know a
    /// request will never reconcile (e.g. client hang-up before response).
    /// Returns `true` if the reservation was found and refunded.
    pub fn refund_by_id(&self, id: &str) -> bool {
        let concurrent_guards = {
            let mut reservations = self.inner.reservations.lock();
            let Some(mut record) = reservations.remove(id) else {
                return false;
            };
            let concurrent_guards = std::mem::take(&mut record.concurrent_guards);
            let now_sec = unix_secs(self.inner.clock.now());
            {
                let mut rolling = self.inner.rolling.write();
                for amount in &record.reserved {
                    remove_reserved_bucket_locked(&mut rolling, &record.key_id, amount, now_sec);
                }
            }
            concurrent_guards
        };
        drop(concurrent_guards);
        true
    }

    fn refund_record(&self, mut record: ReservationRecord) {
        let concurrent_guards = std::mem::take(&mut record.concurrent_guards);
        let now_sec = unix_secs(self.inner.clock.now());
        {
            let mut rolling = self.inner.rolling.write();
            for amount in &record.reserved {
                remove_reserved_bucket_locked(&mut rolling, &record.key_id, amount, now_sec);
            }
        }
        drop(concurrent_guards);
    }

    /// Format the local request/token windows for a provider's response.
    ///
    /// Gemini deliberately uses a neutral `x-cc-lb-ratelimit-*` namespace:
    /// these values describe proxy-enforced limits, not a provider contract.
    pub fn headers_for_provider(
        &self,
        provider: Provider,
        key_id: &str,
        principal_id: &str,
    ) -> Vec<(String, String)> {
        let Some(limits) = self
            .inner
            .effective_limits
            .read()
            .get(&(key_id.to_owned(), principal_id.to_owned()))
            .cloned()
        else {
            return Vec::new();
        };

        self.format_provider_headers(provider, key_id, &limits)
    }

    /// Format local request/token windows from the current record and defaults.
    ///
    /// Unlike [`Self::headers_for_provider`], this does not consult or update the
    /// successful-admission cache, so rejected reservations can be formatted
    /// without changing the limits reported for an admitted request.
    pub fn headers_for_provider_record(
        &self,
        provider: Provider,
        key_id: &str,
        record: &StoredApiKeyRecord,
        default_limits: &[Limit],
    ) -> Vec<(String, String)> {
        let limits = effective_limits(record, default_limits);
        self.format_provider_headers(provider, key_id, &limits)
    }

    fn format_provider_headers(
        &self,
        provider: Provider,
        key_id: &str,
        limits: &[Limit],
    ) -> Vec<(String, String)> {
        let now_sec = unix_secs(self.inner.clock.now());
        let mut headers = Vec::new();
        let mut emitted_requests = false;
        let mut emitted_tokens = false;

        for limit in limits {
            if emitted_requests && emitted_tokens {
                break;
            }

            let kind_name = match limit.kind {
                LimitKind::Requests if !emitted_requests => {
                    emitted_requests = true;
                    "requests"
                }
                LimitKind::TotalTokens if !emitted_tokens => {
                    emitted_tokens = true;
                    "tokens"
                }
                _ => continue,
            };

            let window_sec = limit.window_secs;
            let (total, oldest) =
                self.current_total_and_oldest(key_id, limit.kind, window_sec, now_sec);
            let remaining = limit.cap_micros.saturating_sub(total).max(0);
            let reset_sec = oldest.unwrap_or(now_sec).saturating_add(window_sec);
            match provider {
                Provider::Anthropic => {
                    let prefix = format!("anthropic-ratelimit-{kind_name}");
                    headers.push((format!("{prefix}-remaining"), remaining.to_string()));
                    headers.push((format!("{prefix}-limit"), limit.cap_micros.to_string()));
                    headers.push((format!("{prefix}-reset"), unix_to_iso8601(reset_sec)));
                }
                Provider::Openai | Provider::Gemini => {
                    let prefix = match provider {
                        Provider::Openai => "x-ratelimit",
                        Provider::Gemini => "x-cc-lb-ratelimit",
                        Provider::Anthropic => unreachable!(),
                    };
                    let reset = format!("{}s", reset_sec.saturating_sub(now_sec));
                    headers.push((
                        format!("{prefix}-remaining-{kind_name}"),
                        remaining.to_string(),
                    ));
                    headers.push((
                        format!("{prefix}-limit-{kind_name}"),
                        limit.cap_micros.to_string(),
                    ));
                    headers.push((format!("{prefix}-reset-{kind_name}"), reset));
                }
            }
        }

        headers
    }

    /// Backward-compatible Anthropic formatter for non-HTTP accounting probes.
    pub fn headers_for(&self, key_id: &str, principal_id: &str) -> Vec<(String, String)> {
        self.headers_for_provider(Provider::Anthropic, key_id, principal_id)
    }
}

impl LimitEngine {
    #[allow(dead_code)]
    fn admission_cap(&self, key_id: &str, limit: &Limit, now_sec: u64) -> i64 {
        let durable_usage = self.inner.durable_usage.read().clone();
        self.admission_cap_with_state(durable_usage.as_ref(), key_id, limit, now_sec)
    }

    fn admission_cap_with_state(
        &self,
        durable_usage: Option<&Arc<DurableApiKeyUsage>>,
        key_id: &str,
        limit: &Limit,
        now_sec: u64,
    ) -> i64 {
        let Some(state) = durable_usage else {
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

    #[cfg(test)]
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

    #[cfg(test)]
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
    #[cfg(test)]
    fn record_amount(
        &self,
        key_id: &str,
        kind: LimitKind,
        window_sec: u64,
        delta: i64,
        now_sec: u64,
    ) {
        let mut rolling = self.rolling.write();
        record_amount_locked(&mut rolling, key_id, kind, window_sec, delta, now_sec);
    }
}

struct ReservationPlan {
    key_id: String,
    effective_limits: Vec<Limit>,
    durable_usage: Option<Arc<DurableApiKeyUsage>>,
    durable_bucket_widths: Vec<u64>,
    planned_rolling: Vec<(Limit, i64)>,
    concurrent_caps: Vec<u32>,
    estimated_input_tokens: i64,
    estimated_output_tokens: i64,
    now_sec: u64,
}

fn reject_reason_for_limit_kind(kind: LimitKind) -> RejectReason {
    match kind {
        LimitKind::Requests => RejectReason::RequestsRateLimit,
        LimitKind::InputTokens | LimitKind::OutputTokens | LimitKind::TotalTokens => {
            RejectReason::TokenRateLimit { kind }
        }
        LimitKind::CostUsd => RejectReason::CostRateLimit,
        LimitKind::Concurrent => RejectReason::ConcurrentRateLimit,
    }
}

fn record_amount_locked(
    rolling: &mut HashMap<RollingKey, RingCounter>,
    key_id: &str,
    kind: LimitKind,
    window_sec: u64,
    delta: i64,
    now_sec: u64,
) {
    rolling
        .entry((key_id.to_owned(), kind, window_sec))
        .or_insert_with(|| RingCounter::new(window_sec))
        .record(now_sec, delta);
}

fn remove_reserved_bucket_locked(
    rolling: &mut HashMap<RollingKey, RingCounter>,
    key_id: &str,
    reserved: &ReservedAmount,
    now_sec: u64,
) {
    if !reserved.is_live_at(now_sec) {
        return;
    }
    if let Some(counter) = rolling.get_mut(&(key_id.to_owned(), reserved.kind, reserved.window_sec))
    {
        counter.remove_from_bucket(reserved.bucket_start, reserved.amount, now_sec);
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
        3_600 => MAX_LIMIT_WINDOW_SECS,
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

/// Outcome of a cluster-wide concurrency hold attempt (issue 807).
enum DurableHoldError {
    /// Live hold count reached the cap; reject with `ConcurrentRateLimit`.
    OverCap,
    /// Storage failed or no multi-thread runtime was available; caller falls
    /// back to the local counter.
    Unavailable,
}

/// Runs a storage future from synchronous `reserve` code.
///
/// Returns `None` when there is no multi-thread Tokio runtime to borrow; the
/// caller then falls back to the local concurrency counter.
fn block_on_storage<F, T>(future: F) -> Option<T>
where
    F: Future<Output = T> + Send,
    T: Send,
{
    let handle = tokio::runtime::Handle::try_current().ok()?;
    if handle.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
        return None;
    }
    Some(tokio::task::block_in_place(|| handle.block_on(future)))
}

async fn maintain_durable_concurrency_holds(
    engine: &LimitEngine,
    store: &(impl ApiKeyConcurrencyHoldStore + ?Sized),
    now_sec: u64,
    last_hold_refresh_sec: &mut Option<u64>,
) {
    let should_refresh = last_hold_refresh_sec.is_none_or(|prev| {
        now_sec.saturating_sub(prev) >= API_KEY_CONCURRENCY_HOLD_REFRESH_INTERVAL_SECS
    });
    if should_refresh {
        let hold_ids = engine.live_durable_concurrency_hold_ids();
        if hold_ids.is_empty() {
            *last_hold_refresh_sec = Some(now_sec);
        } else {
            match store
                .refresh_api_key_concurrency_holds(&hold_ids, now_sec)
                .await
            {
                Ok(_) => *last_hold_refresh_sec = Some(now_sec),
                Err(error) => {
                    tracing::warn!(
                        %error,
                        count = hold_ids.len(),
                        "failed to refresh live API-key concurrency holds",
                    );
                }
            }
        }
    }
    if let Err(error) = store
        .delete_expired_api_key_concurrency_holds(now_sec, API_KEY_CONCURRENCY_HOLD_MAX_AGE_SECS)
        .await
    {
        tracing::warn!(%error, "expired API-key concurrency hold sweep failed");
    }
}

struct PendingDurableHoldGuard<S: ApiKeyConcurrencyHoldStore + ?Sized + 'static> {
    storage: Arc<S>,
    hold_id: Uuid,
    key_id: String,
    acquire_task: Option<tokio::task::JoinHandle<Result<bool, StorageError>>>,
    armed: bool,
}

impl<S: ApiKeyConcurrencyHoldStore + ?Sized + 'static> Drop for PendingDurableHoldGuard<S> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let storage = Arc::clone(&self.storage);
        let hold_id = self.hold_id;
        let key_id = std::mem::take(&mut self.key_id);
        let acquire_task = self.acquire_task.take();
        handle.spawn(async move {
            if let Some(task) = acquire_task
                && let Ok(Ok(false)) = task.await
            {
                return;
            }
            if let Err(error) = storage
                .delete_uncertain_api_key_concurrency_hold(hold_id, &key_id)
                .await
            {
                tracing::warn!(%error, %hold_id, %key_id, "failed to clean up uncertain durable concurrency hold");
            }
        });
    }
}

async fn acquire_durable_hold_raw<S: ApiKeyConcurrencyHoldStore + ?Sized + 'static>(
    storage: &Arc<S>,
    writer_epoch: Uuid,
    key_id: &str,
    cap: u32,
) -> Result<Uuid, DurableHoldError> {
    let hold_id = Uuid::now_v7();
    let storage_for_task = Arc::clone(storage);
    let key_id_for_task = key_id.to_owned();
    let acquire_task = tokio::spawn(async move {
        try_acquire_hold_atomic(
            storage_for_task.as_ref(),
            hold_id,
            &key_id_for_task,
            writer_epoch,
            cap,
        )
        .await
    });
    let mut guard = PendingDurableHoldGuard {
        storage: Arc::clone(storage),
        hold_id,
        key_id: key_id.to_owned(),
        acquire_task: Some(acquire_task),
        armed: true,
    };
    let join_outcome = match guard.acquire_task.as_mut() {
        Some(task) => task.await,
        None => unreachable!("acquire_task initialized as Some"),
    };
    guard.acquire_task = None;
    match join_outcome {
        Ok(Ok(true)) => {
            guard.armed = false;
            Ok(hold_id)
        }
        Ok(Ok(false)) => {
            guard.armed = false;
            Err(DurableHoldError::OverCap)
        }
        Ok(Err(error)) => {
            tracing::warn!(%error, %key_id, "durable API-key concurrency hold failed");
            Err(DurableHoldError::Unavailable)
        }
        Err(join_error) => {
            tracing::warn!(%join_error, %key_id, "durable API-key concurrency hold task failed");
            Err(DurableHoldError::Unavailable)
        }
    }
}

/// Insert-then-count admission for `api_key_concurrency_holds_v1`.
///
/// Returns `Ok(true)` when the hold is admitted, `Ok(false)` when the live
/// hold count exceeds `cap` (the inserted row is deleted again), or `Err` on
/// storage failure. Holds older than `max_age` are excluded from the count as
/// crash leftovers.
async fn try_acquire_hold_atomic(
    store: &(impl ApiKeyConcurrencyHoldStore + ?Sized),
    hold_id: Uuid,
    key_id: &str,
    writer_epoch: Uuid,
    cap: u32,
) -> Result<bool, StorageError> {
    store
        .try_acquire_api_key_concurrency_hold(
            hold_id,
            key_id,
            writer_epoch,
            cap,
            API_KEY_CONCURRENCY_HOLD_MAX_AGE_SECS,
        )
        .await
}

#[cfg(test)]
async fn try_acquire_hold(
    store: &(impl ApiKeyConcurrencyHoldStore + ?Sized),
    hold: &cc_lb_storage_api::ApiKeyConcurrencyHold,
    cap: u32,
    _now_sec: u64,
) -> Result<bool, StorageError> {
    try_acquire_hold_atomic(store, hold.hold_id, &hold.key_id, hold.writer_epoch, cap).await
}

fn durable_bucket_width(window_secs: u64) -> Option<u64> {
    match window_secs {
        0..=60 => Some(1),
        61..=3_600 => Some(60),
        3_601..=18_000 => Some(300),
        18_001..=MAX_LIMIT_WINDOW_SECS => Some(3_600),
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
        // Keep `flush_id` stable across writer-epoch rotation so a retry of a
        // previously committed flush is deduplicated by `api_key_usage_flushes_v1`.
        in_flight.writer_epoch = writer_epoch;
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
    // Global lock order: reservations -> in_flight -> pending -> key_limits -> rolling.
    let reservations_guard = engine.inner.reservations.lock();
    let in_flight_guard = state.in_flight.lock();
    let pending_guard = state.pending.lock();
    let limits = state.key_limits.read();
    let mut rolling = engine.inner.rolling.write();

    let mut unflushed = pending_guard
        .iter()
        .map(|(key, usage)| ApiKeyUsageBucketDelta {
            key: key.clone(),
            usage: *usage,
        })
        .collect::<Vec<_>>();
    if let Some(in_flight) = in_flight_guard.as_ref() {
        unflushed.extend(in_flight.deltas.iter().cloned());
    }
    unflushed.sort_unstable_by(|left, right| {
        left.key
            .bucket_start_unix_secs
            .cmp(&right.key.bucket_start_unix_secs)
            .then_with(|| left.key.bucket_width_secs.cmp(&right.key.bucket_width_secs))
            .then_with(|| left.key.key_id.cmp(&right.key.key_id))
    });
    let reservations = reservations_guard
        .values()
        .map(|record| (record.key_id.clone(), record.reserved.clone()))
        .collect::<Vec<_>>();

    rolling.retain(|(key_id, _, _), _| !limits.by_key.contains_key(key_id));
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
                record_amount_locked(
                    &mut rolling,
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
            if !amount.is_live_at(now_sec) {
                continue;
            }
            record_amount_locked(
                &mut rolling,
                &key_id,
                amount.kind,
                amount.window_sec,
                amount.amount,
                amount.bucket_start,
            );
        }
    }
}

fn collect_key_refresh_batch(
    registry: &mut KeyLimitRegistry,
    cursor: &AtomicU32,
) -> (Vec<String>, HashMap<String, Vec<Limit>>) {
    let total_keys = registry.sorted_key_ids.len();
    let target = API_KEY_USAGE_REFRESH_BATCH_SIZE.min(total_keys);
    let mut batch = Vec::with_capacity(target);
    let mut batch_set = HashSet::with_capacity(target);

    while batch.len() < target {
        let Some(cold_key) = registry.cold_keys.pop_front() else {
            break;
        };
        registry.cold_set.remove(&cold_key);
        if registry.by_key.contains_key(&cold_key) && batch_set.insert(cold_key.clone()) {
            batch.push(cold_key);
        }
    }

    let remaining = target.saturating_sub(batch.len());
    if remaining > 0 && total_keys > 0 {
        let start = (cursor.fetch_add(remaining as u32, Ordering::AcqRel) as usize) % total_keys;
        for key_id in registry
            .sorted_key_ids
            .iter()
            .cycle()
            .skip(start)
            .take(total_keys)
        {
            if batch.len() >= target {
                break;
            }
            if batch_set.insert(key_id.clone()) {
                batch.push(key_id.clone());
            }
        }
    }

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
}

fn build_remote_replacement(
    key_ids: &[String],
    limits: &HashMap<String, Vec<Limit>>,
    buckets: &[cc_lb_storage_api::ApiKeyUsageBucket],
) -> HashMap<RollingKey, RingCounter> {
    let mut replacement = HashMap::<RollingKey, RingCounter>::new();
    for key_id in key_ids {
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
    replacement
}

struct ColdHydrationGuard {
    state: Arc<DurableApiKeyUsage>,
    key_id: String,
    tx: tokio::sync::watch::Sender<bool>,
}

impl Drop for ColdHydrationGuard {
    fn drop(&mut self) {
        self.state.cold_hydrations.lock().remove(&self.key_id);
        let _ = self.tx.send(true);
    }
}

fn is_key_fresh(state: &DurableApiKeyUsage, key_id: &str, limits: &[Limit], now_sec: u64) -> bool {
    let registered_keys = state.key_limits.read().sorted_key_ids.len().max(1);
    let Some(refreshed_at) = state.refreshed_at.read().get(key_id).copied() else {
        return false;
    };
    limits
        .iter()
        .filter(|limit| limit.kind != LimitKind::Concurrent)
        .all(|limit| {
            let max_staleness =
                api_key_usage_max_staleness_secs(limit.window_secs, registered_keys);
            now_sec.saturating_sub(refreshed_at) <= max_staleness
        })
}

async fn hydrate_cold_key_usage(
    engine: &Arc<LimitEngine>,
    state: &Arc<DurableApiKeyUsage>,
    key_id: &str,
    limits: &[Limit],
    now_sec: u64,
) {
    let max_window = limits
        .iter()
        .filter(|limit| limit.kind != LimitKind::Concurrent)
        .map(|limit| limit.window_secs)
        .max()
        .unwrap_or(0);
    if max_window == 0 || is_key_fresh(state, key_id, limits, now_sec) {
        return;
    }

    enum HydrationRole {
        Leader(ColdHydrationGuard),
        Follower(tokio::sync::watch::Receiver<bool>),
    }
    let role = {
        let mut hydrations = state.cold_hydrations.lock();
        if let Some(rx) = hydrations.get(key_id).cloned() {
            HydrationRole::Follower(rx)
        } else {
            let (tx, rx) = tokio::sync::watch::channel(false);
            hydrations.insert(key_id.to_owned(), rx);
            HydrationRole::Leader(ColdHydrationGuard {
                state: Arc::clone(state),
                key_id: key_id.to_owned(),
                tx,
            })
        }
    };
    let leader_guard = match role {
        HydrationRole::Follower(mut rx) => {
            let _ = tokio::time::timeout(Duration::from_secs(2), rx.wait_for(|done| *done)).await;
            return;
        }
        HydrationRole::Leader(guard) => guard,
    };
    if is_key_fresh(state, key_id, limits, now_sec) {
        drop(leader_guard);
        return;
    }

    let exclude_writer_epoch = *state.writer_epoch.read();
    let buckets = match state
        .storage
        .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
            key_ids: vec![key_id.to_owned()],
            since_unix_secs: now_sec.saturating_sub(max_window),
            until_unix_secs: now_sec,
            exclude_writer_epoch,
        })
        .await
    {
        Ok(buckets) => buckets,
        Err(error) => {
            tracing::warn!(%error, %key_id, "on-demand cold API-key usage hydration failed; using cold allowance");
            drop(leader_guard);
            return;
        }
    };
    let single_key = vec![key_id.to_owned()];
    let single_limits = HashMap::from([(key_id.to_owned(), limits.to_vec())]);
    let replacement = build_remote_replacement(&single_key, &single_limits, &buckets);
    {
        let mut remote = engine.inner.remote_rolling.write();
        remote.retain(|(existing_key, _, _), _| existing_key != key_id);
        remote.extend(replacement);
    }
    state
        .refreshed_at
        .write()
        .insert(key_id.to_owned(), now_sec);
    state.key_limits.write().mark_hydrated(key_id);
    drop(leader_guard);
}

async fn refresh_remote_api_key_usage(
    engine: &Arc<LimitEngine>,
    state: &Arc<DurableApiKeyUsage>,
    now_sec: u64,
) -> Result<(), StorageError> {
    let (evicted, batch, limits) = {
        let mut registry = state.key_limits.write();
        let evicted = registry.evict_idle(now_sec);
        if registry.sorted_key_ids.is_empty() {
            (evicted, Vec::new(), HashMap::new())
        } else {
            let (batch, limits) = collect_key_refresh_batch(&mut registry, &state.refresh_cursor);
            (evicted, batch, limits)
        }
    };
    if !evicted.is_empty() {
        let evicted_set: HashSet<&str> = evicted.iter().map(String::as_str).collect();
        engine
            .inner
            .remote_rolling
            .write()
            .retain(|(key_id, _, _), _| !evicted_set.contains(key_id.as_str()));
        let mut refreshed_at = state.refreshed_at.write();
        for key_id in &evicted {
            refreshed_at.remove(key_id);
        }
    }
    if batch.is_empty() {
        return Ok(());
    }
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
    let replacement = build_remote_replacement(&batch, &limits, &buckets);
    let batch_keys = batch.iter().cloned().collect::<HashSet<_>>();
    {
        let mut remote = engine.inner.remote_rolling.write();
        remote.retain(|(key_id, _, _), _| !batch_keys.contains(key_id));
        remote.extend(replacement);
    }
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
        // Skipped when `forget()` was called (subscriber ownership).
        if self.forgotten {
            return;
        }
        let Some(engine) = self.engine.upgrade() else {
            return;
        };
        let concurrent_guards = {
            let mut reservations = engine.reservations.lock();
            let Some(mut record) = reservations.remove(&self.id) else {
                return;
            };
            let concurrent_guards = std::mem::take(&mut record.concurrent_guards);
            let now_sec = unix_secs(engine.clock.now());
            {
                let mut rolling = engine.rolling.write();
                for amount in &record.reserved {
                    remove_reserved_bucket_locked(&mut rolling, &record.key_id, amount, now_sec);
                }
            }
            concurrent_guards
        };
        drop(concurrent_guards);
    }
}

fn refund_difference_locked(
    rolling: &mut HashMap<RollingKey, RingCounter>,
    key_id: &str,
    reserved: &[ReservedAmount],
    actual_input: u64,
    actual_output: u64,
    actual_cost_micros: Option<i64>,
    now_sec: u64,
) {
    for amount in reserved {
        let actual = match amount.kind {
            LimitKind::Requests => 1,
            LimitKind::InputTokens => actual_input as i64,
            LimitKind::OutputTokens => actual_output as i64,
            LimitKind::TotalTokens => actual_input.saturating_add(actual_output) as i64,
            LimitKind::CostUsd => match actual_cost_micros {
                Some(cost) => cost,
                None => continue,
            },
            LimitKind::Concurrent => continue,
        };
        let delta = actual.saturating_sub(amount.amount);
        if delta != 0 {
            record_amount_locked(
                rolling,
                key_id,
                amount.kind,
                amount.window_sec,
                delta,
                now_sec,
            );
        }
    }
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
/// [`spawn_reservation_ttl_sweeper_with_max_ttl`]. Dropping the handle stops the task on
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

/// Spawn the TTL sweeper with explicit sliding idle TTL and hard max TTL.
///
/// Every `tick_interval` the sweeper walks the engine's reservation map and
/// refunds any reservation inactive for `idle_ttl` or older than `max_ttl`.
pub fn spawn_reservation_ttl_sweeper_with_max_ttl(
    engine: Arc<LimitEngine>,
    idle_ttl: Duration,
    max_ttl: Duration,
    tick_interval: Duration,
) -> ReservationTtlSweeperHandle {
    let effective_max_ttl = max_ttl.max(idle_ttl);
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
                    sweep_expired(&engine, idle_ttl, effective_max_ttl);
                }
            }
        }
    });
    ReservationTtlSweeperHandle { shutdown_tx, join }
}

fn sweep_expired(engine: &LimitEngine, idle_ttl: Duration, max_ttl: Duration) {
    let effective_max_ttl = max_ttl.max(idle_ttl);
    let expired_records: Vec<ReservationRecord> = {
        let mut map = engine.inner.reservations.lock();
        let now = Instant::now();
        let expired_ids: Vec<ReservationId> = map
            .iter()
            .filter(|(_, r)| {
                now.saturating_duration_since(r.last_activity_at) >= idle_ttl
                    || now.saturating_duration_since(r.created_at) >= effective_max_ttl
            })
            .map(|(k, _)| k.clone())
            .collect();
        expired_ids
            .into_iter()
            .filter_map(|id| map.remove(&id))
            .collect()
    };
    let evicted = u64::try_from(expired_records.len()).unwrap_or(u64::MAX);
    for record in expired_records {
        engine.refund_record(record);
    }
    if evicted > 0 {
        metrics::counter!("cc_lb_limit_reservation_ttl_evicted_total").increment(evicted);
    }
}

#[cfg(test)]
mod tests {
    use cc_lb_clock::Clock;
    use std::collections::HashMap;

    use cc_lb_storage_api::{ApiKeyConcurrencyHold, MetaStore};

    use super::*;

    fn formatter_fixture(limits: Vec<Limit>) -> (Arc<LimitEngine>, Arc<cc_lb_clock::TestClock>) {
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_700_000_000));
        let engine = LimitEngine::new(Arc::new(KeyConcurrencyManager::new()), clock.clone());
        engine
            .inner
            .effective_limits
            .write()
            .insert(("key-a".to_owned(), "principal-a".to_owned()), limits);
        (engine, clock)
    }

    fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
        headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    #[test]
    fn provider_formatter_uses_anthropic_iso_contract() {
        let (engine, _) = formatter_fixture(vec![
            Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: 10,
            },
            Limit {
                kind: LimitKind::TotalTokens,
                window_secs: 120,
                cap_micros: 20,
            },
        ]);

        let headers = engine.headers_for_provider(Provider::Anthropic, "key-a", "principal-a");

        assert_eq!(
            header_value(&headers, "anthropic-ratelimit-requests-reset"),
            Some("2023-11-14T22:14:20Z")
        );
        assert_eq!(
            header_value(&headers, "anthropic-ratelimit-requests-limit"),
            Some("10")
        );
        assert_eq!(
            header_value(&headers, "anthropic-ratelimit-tokens-limit"),
            Some("20")
        );
        assert_eq!(
            header_value(&headers, "anthropic-ratelimit-tokens-reset"),
            Some("2023-11-14T22:15:20Z")
        );
        assert!(headers.iter().all(|(name, _)| !name.starts_with("x-")));
    }

    #[test]
    fn provider_formatter_reports_anthropic_remaining_and_oldest_bucket_reset() {
        let (engine, clock) = formatter_fixture(vec![
            Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: 10,
            },
            Limit {
                kind: LimitKind::TotalTokens,
                window_secs: 120,
                cap_micros: 20,
            },
        ]);
        let now = unix_secs(clock.now());
        engine.record_amount("key-a", LimitKind::Requests, 60, 3, now);
        engine.record_amount("key-a", LimitKind::TotalTokens, 120, 5, now);
        clock.advance_secs(10);

        let headers = engine.headers_for_provider(Provider::Anthropic, "key-a", "principal-a");

        assert_eq!(
            header_value(&headers, "anthropic-ratelimit-requests-remaining"),
            Some("7")
        );
        assert_eq!(
            header_value(&headers, "anthropic-ratelimit-tokens-remaining"),
            Some("15")
        );
        assert_eq!(
            header_value(&headers, "anthropic-ratelimit-requests-reset"),
            Some("2023-11-14T22:14:20Z")
        );
        assert_eq!(
            header_value(&headers, "anthropic-ratelimit-tokens-reset"),
            Some("2023-11-14T22:15:00Z")
        );
    }

    #[test]
    fn provider_formatter_emits_only_requests_tokens_or_nothing() {
        let (engine, _) = formatter_fixture(vec![Limit {
            kind: LimitKind::Requests,
            window_secs: 60,
            cap_micros: 10,
        }]);

        let anthropic = engine.headers_for_provider(Provider::Anthropic, "key-a", "principal-a");
        assert_eq!(anthropic.len(), 3);
        assert!(
            anthropic
                .iter()
                .all(|(name, _)| name.starts_with("anthropic-ratelimit-requests-"))
        );
        let openai = engine.headers_for_provider(Provider::Openai, "key-a", "principal-a");
        assert_eq!(openai.len(), 3);
        assert!(
            openai
                .iter()
                .all(|(name, _)| name.starts_with("x-ratelimit-") && name.ends_with("-requests"))
        );
        let gemini = engine.headers_for_provider(Provider::Gemini, "key-a", "principal-a");
        assert_eq!(gemini.len(), 3);
        assert!(gemini.iter().all(|(name, _)| {
            name.starts_with("x-cc-lb-ratelimit-") && name.ends_with("-requests")
        }));

        let (engine, _) = formatter_fixture(vec![Limit {
            kind: LimitKind::TotalTokens,
            window_secs: 60,
            cap_micros: 20,
        }]);
        let anthropic = engine.headers_for_provider(Provider::Anthropic, "key-a", "principal-a");
        assert_eq!(anthropic.len(), 3);
        assert!(
            anthropic
                .iter()
                .all(|(name, _)| name.starts_with("anthropic-ratelimit-tokens-"))
        );
        let openai = engine.headers_for_provider(Provider::Openai, "key-a", "principal-a");
        assert_eq!(openai.len(), 3);
        assert!(
            openai
                .iter()
                .all(|(name, _)| name.starts_with("x-ratelimit-") && name.ends_with("-tokens"))
        );
        let gemini = engine.headers_for_provider(Provider::Gemini, "key-a", "principal-a");
        assert_eq!(gemini.len(), 3);
        assert!(gemini.iter().all(|(name, _)| {
            name.starts_with("x-cc-lb-ratelimit-") && name.ends_with("-tokens")
        }));

        let (engine, _) = formatter_fixture(Vec::new());
        assert!(
            engine
                .headers_for_provider(Provider::Anthropic, "key-a", "principal-a")
                .is_empty()
        );
        assert!(
            engine
                .headers_for_provider(Provider::Openai, "key-a", "principal-a")
                .is_empty()
        );
        assert!(
            engine
                .headers_for_provider(Provider::Gemini, "key-a", "principal-a")
                .is_empty()
        );
    }

    #[test]
    fn provider_formatter_uses_openai_and_gemini_duration_contracts() {
        let (engine, clock) = formatter_fixture(vec![
            Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: 10,
            },
            Limit {
                kind: LimitKind::TotalTokens,
                window_secs: 120,
                cap_micros: 20,
            },
        ]);
        let now = unix_secs(clock.now());
        engine.record_amount("key-a", LimitKind::Requests, 60, 15, now);
        engine.record_amount("key-a", LimitKind::TotalTokens, 120, 5, now);
        clock.advance_secs(10);

        let openai = engine.headers_for_provider(Provider::Openai, "key-a", "principal-a");
        assert_eq!(
            header_value(&openai, "x-ratelimit-remaining-requests"),
            Some("0")
        );
        assert_eq!(
            header_value(&openai, "x-ratelimit-limit-tokens"),
            Some("20")
        );
        assert_eq!(
            header_value(&openai, "x-ratelimit-reset-requests"),
            Some("50s")
        );
        assert_eq!(
            header_value(&openai, "x-ratelimit-reset-tokens"),
            Some("90s")
        );
        assert_eq!(
            header_value(&openai, "x-ratelimit-remaining-tokens"),
            Some("15")
        );
        assert!(
            openai
                .iter()
                .all(|(name, _)| !name.starts_with("anthropic-"))
        );

        let gemini = engine.headers_for_provider(Provider::Gemini, "key-a", "principal-a");
        assert_eq!(
            header_value(&gemini, "x-cc-lb-ratelimit-reset-requests"),
            Some("50s")
        );
        assert_eq!(
            header_value(&gemini, "x-cc-lb-ratelimit-reset-tokens"),
            Some("90s")
        );
        assert!(
            gemini
                .iter()
                .all(|(name, _)| !name.starts_with("anthropic-"))
        );
    }

    #[test]
    fn provider_formatter_clamps_reset_at_now_and_omits_unsupported_duplicates() {
        let (engine, _) = formatter_fixture(vec![
            Limit {
                kind: LimitKind::CostUsd,
                window_secs: 60,
                cap_micros: 100,
            },
            Limit {
                kind: LimitKind::InputTokens,
                window_secs: 60,
                cap_micros: 100,
            },
            Limit {
                kind: LimitKind::OutputTokens,
                window_secs: 60,
                cap_micros: 100,
            },
            Limit {
                kind: LimitKind::Concurrent,
                window_secs: 0,
                cap_micros: 100,
            },
            Limit {
                kind: LimitKind::Requests,
                window_secs: 0,
                cap_micros: 10,
            },
            Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: 20,
            },
            Limit {
                kind: LimitKind::TotalTokens,
                window_secs: 60,
                cap_micros: 30,
            },
            Limit {
                kind: LimitKind::TotalTokens,
                window_secs: 120,
                cap_micros: 40,
            },
        ]);

        let openai = engine.headers_for_provider(Provider::Openai, "key-a", "principal-a");
        assert_eq!(
            header_value(&openai, "x-ratelimit-reset-requests"),
            Some("0s")
        );
        assert_eq!(
            header_value(&openai, "x-ratelimit-limit-requests"),
            Some("10")
        );
        assert_eq!(
            header_value(&openai, "x-ratelimit-limit-tokens"),
            Some("30")
        );
        assert_eq!(openai.len(), 6);
        assert!(openai.iter().all(|(name, _)| {
            !name.starts_with("anthropic-")
                && !name.starts_with("x-cc-lb-ratelimit-")
                && !name.contains("input")
                && !name.contains("output")
                && !name.contains("concurrent")
                && !name.contains("cost")
        }));

        let headers = engine.headers_for_provider(Provider::Gemini, "key-a", "principal-a");

        assert_eq!(
            header_value(&headers, "x-cc-lb-ratelimit-reset-requests"),
            Some("0s")
        );
        assert_eq!(
            header_value(&headers, "x-cc-lb-ratelimit-limit-requests"),
            Some("10")
        );
        assert_eq!(
            header_value(&headers, "x-cc-lb-ratelimit-limit-tokens"),
            Some("30")
        );
        assert_eq!(
            headers
                .iter()
                .filter(|(name, _)| name.ends_with("-limit-requests"))
                .count(),
            1
        );
        assert_eq!(
            headers
                .iter()
                .filter(|(name, _)| name.ends_with("-limit-tokens"))
                .count(),
            1
        );
        assert_eq!(headers.len(), 6);
        assert!(
            headers
                .iter()
                .all(|(name, _)| !name.starts_with("anthropic-")
                    && !name.starts_with("x-ratelimit-")
                    && !name.contains("input")
                    && !name.contains("output")
                    && !name.contains("concurrent")
                    && !name.contains("cost"))
        );
    }

    #[test]
    fn provider_record_formatter_does_not_mutate_cached_effective_limits() {
        let (engine, _) = formatter_fixture(vec![
            Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: 20,
            },
            Limit {
                kind: LimitKind::TotalTokens,
                window_secs: 60,
                cap_micros: 40,
            },
        ]);
        let record = StoredApiKeyRecord {
            limit_overrides: vec![Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: 7,
            }],
            ..StoredApiKeyRecord::default()
        };
        let defaults = vec![
            Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: 12,
            },
            Limit {
                kind: LimitKind::TotalTokens,
                window_secs: 60,
                cap_micros: 14,
            },
        ];

        let rejection_headers =
            engine.headers_for_provider_record(Provider::Openai, "key-a", &record, &defaults);
        assert_eq!(
            header_value(&rejection_headers, "x-ratelimit-limit-requests"),
            Some("7")
        );
        assert_eq!(
            header_value(&rejection_headers, "x-ratelimit-limit-tokens"),
            Some("14")
        );

        let cached_headers = engine.headers_for_provider(Provider::Openai, "key-a", "principal-a");
        assert_eq!(
            header_value(&cached_headers, "x-ratelimit-limit-requests"),
            Some("20")
        );
        assert_eq!(
            header_value(&cached_headers, "x-ratelimit-limit-tokens"),
            Some("40")
        );
    }

    fn engine_with_output_token_limit(
        cap: i64,
    ) -> (Arc<LimitEngine>, PrincipalView, StoredApiKeyRecord) {
        let engine = LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            Arc::new(cc_lb_clock::SystemClock),
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

    #[test]
    fn negative_output_cap_is_rejected_before_output_reservation() {
        let (engine, view, record) = engine_with_output_token_limit(100);
        assert_eq!(
            engine
                .reserve(
                    &view,
                    &record,
                    "principal-a",
                    "claude-3-opus",
                    Some(-1),
                    0,
                    None
                )
                .err(),
            Some(RejectReason::InvalidOutputCap { requested: -1 }),
        );
        assert_eq!(
            engine.current_total(
                &key_id_for(&record),
                LimitKind::OutputTokens,
                60,
                unix_secs(engine.inner.clock.now()),
            ),
            0,
        );
    }

    #[test]
    fn missing_output_cap_is_rejected_before_output_reservation() {
        let (engine, view, record) = engine_with_output_token_limit(100);
        assert_eq!(
            engine
                .reserve(
                    &view,
                    &record,
                    "principal-a",
                    "claude-3-opus",
                    None,
                    0,
                    None
                )
                .err(),
            Some(RejectReason::OutputCapRequired { cap: 100 }),
        );
        assert_eq!(
            engine.current_total(
                &key_id_for(&record),
                LimitKind::OutputTokens,
                60,
                unix_secs(engine.inner.clock.now()),
            ),
            0,
        );
    }

    #[test]
    fn explicit_zero_output_cap_still_reserves_successfully() {
        let (engine, view, record) = engine_with_output_token_limit(100);
        let reservation = engine
            .reserve(
                &view,
                &record,
                "principal-a",
                "claude-3-opus",
                Some(0),
                0,
                None,
            )
            .expect("explicit zero output cap remains valid");
        assert_eq!(
            engine.current_total(
                &key_id_for(&record),
                LimitKind::OutputTokens,
                60,
                unix_secs(engine.inner.clock.now()),
            ),
            0,
        );
        drop(reservation);
    }

    #[test]
    fn missing_output_cap_is_allowed_when_only_total_tokens_is_limited() {
        let engine = LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            Arc::new(cc_lb_clock::SystemClock),
        );
        let view = PrincipalView::for_tests(
            "principal-a",
            true,
            vec!["claude-3-opus".to_owned()],
            vec![Limit {
                kind: LimitKind::TotalTokens,
                window_secs: 60,
                cap_micros: 100,
            }],
            HashMap::new(),
        );
        let record = StoredApiKeyRecord {
            index_hash: [8u8; 32],
            ..StoredApiKeyRecord::default()
        };
        let reservation = engine
            .reserve(
                &view,
                &record,
                "principal-a",
                "claude-3-opus",
                None,
                7,
                None,
            )
            .expect("input-only request can use the total-token limit");
        assert_eq!(
            engine.current_total(
                &key_id_for(&record),
                LimitKind::TotalTokens,
                60,
                unix_secs(engine.inner.clock.now()),
            ),
            7,
        );
        drop(reservation);
    }

    #[test]
    fn model_less_operations_skip_only_model_authorization() {
        let (engine, view, record) = engine_with_output_token_limit(100);
        assert!(
            engine
                .reserve(&view, &record, "principal-a", "", Some(0), 0, Some(0))
                .is_ok()
        );
        assert!(matches!(
            engine.reserve(
                &view,
                &record,
                "principal-a",
                "unknown",
                Some(0),
                0,
                Some(0)
            ),
            Err(RejectReason::ModelNotAllowed)
        ));
        assert!(matches!(
            engine.reserve(&view, &record, "principal-a", "", Some(101), 0, Some(0)),
            Err(RejectReason::OutputCapExceeded { .. })
        ));
        assert!(matches!(
            engine.reserve(&view, &record, "missing-principal", "", Some(0), 0, Some(0)),
            Err(RejectReason::PrincipalMissing)
        ));
    }

    #[test]
    fn model_less_operations_still_reserve_request_capacity() {
        let engine = LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            Arc::new(cc_lb_clock::SystemClock),
        );
        let view = PrincipalView::for_tests(
            "principal-a",
            true,
            vec!["claude-only".to_owned()],
            vec![Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: 1,
            }],
            HashMap::new(),
        );
        let record = StoredApiKeyRecord {
            index_hash: [9; 32],
            ..Default::default()
        };
        let _reservation = engine
            .reserve(&view, &record, "principal-a", "", Some(0), 0, Some(0))
            .expect("model-less request is admitted");
        assert!(matches!(
            engine.reserve(&view, &record, "principal-a", "", Some(0), 0, Some(0)),
            Err(RejectReason::RequestsRateLimit)
        ));
    }

    #[test]
    fn swept_reservation_cannot_authorize_a_queued_dispatch() {
        let (engine, view, record) = engine_with_output_token_limit(1_000);
        let reservation = engine
            .reserve(
                &view,
                &record,
                "principal-a",
                "claude-3-opus",
                Some(100),
                0,
                None,
            )
            .unwrap();
        assert!(reservation.is_active());
        sweep_expired(&engine, Duration::ZERO, Duration::ZERO);
        assert!(!reservation.is_active());
        assert!(
            !engine.refund_by_id(reservation.id()),
            "the sweeper already released this reservation"
        );
    }

    #[test]
    fn finished_response_handoff_releases_only_its_concurrency_before_pricing() {
        let concurrency = Arc::new(KeyConcurrencyManager::new());
        let engine = LimitEngine::new(concurrency.clone(), Arc::new(cc_lb_clock::SystemClock));
        let view = PrincipalView::for_tests(
            "principal-a",
            true,
            vec!["gpt-test".to_owned()],
            vec![
                Limit {
                    kind: LimitKind::Concurrent,
                    window_secs: 0,
                    cap_micros: 1,
                },
                Limit {
                    kind: LimitKind::OutputTokens,
                    window_secs: 60,
                    cap_micros: 1_000,
                },
            ],
            HashMap::new(),
        );
        let record = StoredApiKeyRecord {
            key_hash_b64: "handoff-key".to_owned(),
            ..Default::default()
        };
        let first = engine
            .reserve(
                &view,
                &record,
                "principal-a",
                "gpt-test",
                Some(100),
                0,
                None,
            )
            .unwrap();
        let first_id = first.id().to_owned();
        assert_eq!(concurrency.current(&record.key_hash_b64), 1);

        first.handoff_after_response();

        assert_eq!(concurrency.current(&record.key_hash_b64), 0);
        let now = unix_secs(engine.inner.clock.now());
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::OutputTokens, 60, now),
            100
        );
        let next = engine
            .reserve(
                &view,
                &record,
                "principal-a",
                "gpt-test",
                Some(100),
                0,
                None,
            )
            .unwrap();
        assert_eq!(concurrency.current(&record.key_hash_b64), 1);
        assert!(engine.reconcile_by_id(&first_id, 0, 40, Some(0)));
        assert_eq!(
            concurrency.current(&record.key_hash_b64),
            1,
            "late pricing cannot release another turn's slot"
        );
        assert!(!engine.refund_by_id(&first_id));
        drop(next);
        assert_eq!(concurrency.current(&record.key_hash_b64), 0);
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::OutputTokens, 60, now),
            40
        );
    }

    #[test]
    fn reserve_returns_stable_unique_ids() {
        let (engine, view, record) = engine_with_output_token_limit(1_000_000);
        let a = engine
            .reserve(
                &view,
                &record,
                "principal-a",
                "claude-3-opus",
                Some(10),
                0,
                None,
            )
            .expect("reserve a");
        let b = engine
            .reserve(
                &view,
                &record,
                "principal-a",
                "claude-3-opus",
                Some(10),
                0,
                None,
            )
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
            .reserve(
                &view,
                &record,
                "principal-a",
                "claude-3-opus",
                Some(100),
                0,
                None,
            )
            .expect("reserve");
        assert_eq!(
            engine.current_total(&key_id, LimitKind::OutputTokens, 60, now_sec),
            100,
        );
        let id = res.id().to_owned();
        std::mem::forget(res); // ensure Drop does not interfere with the refund path we assert

        assert!(engine.reconcile_by_id(&id, 0, 40, Some(0)));
        assert_eq!(
            engine.current_total(&key_id, LimitKind::OutputTokens, 60, now_sec),
            40,
            "reserved 100, actual output 40 → 60 refunded → 40 remaining",
        );
        assert!(
            !engine.reconcile_by_id(&id, 0, 0, Some(0)),
            "second call is idempotent no-op"
        );
    }

    #[test]
    fn reconcile_by_id_tops_up_when_actual_exceeds_reserved() {
        let (engine, view, record) = engine_with_output_token_limit(1_000_000);
        let key_id = key_id_for(&record);
        let now_sec = unix_secs(engine.inner.clock.now());
        let res = engine
            .reserve(
                &view,
                &record,
                "principal-a",
                "claude-3-opus",
                Some(100),
                0,
                None,
            )
            .expect("reserve");
        let id = res.id().to_owned();
        std::mem::forget(res);

        assert!(engine.reconcile_by_id(&id, 0, 250, Some(0)));
        assert_eq!(
            engine.current_total(&key_id, LimitKind::OutputTokens, 60, now_sec),
            250,
            "reserved 100, actual output 250 → +150 top-up → 250 recorded",
        );
    }

    #[test]
    fn refund_by_id_removes_full_reservation() {
        let (engine, view, record) = engine_with_output_token_limit(1_000_000);
        let key_id = key_id_for(&record);
        let now_sec = unix_secs(engine.inner.clock.now());
        let res = engine
            .reserve(
                &view,
                &record,
                "principal-a",
                "claude-3-opus",
                Some(55),
                0,
                None,
            )
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
                .reserve(
                    &view,
                    &record,
                    "principal-a",
                    "claude-3-opus",
                    Some(25),
                    0,
                    None,
                )
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
            .reserve(
                &view,
                &record,
                "principal-a",
                "claude-3-opus",
                Some(33),
                0,
                None,
            )
            .expect("reserve");
        let id = res.id().to_owned();
        std::mem::forget(res);
        // Advance the paused tokio clock past the TTL threshold so the
        // reservation's `created_at` is now considered expired.
        tokio::time::advance(Duration::from_millis(200)).await;
        sweep_expired(
            &engine,
            Duration::from_millis(50),
            Duration::from_millis(200),
        );
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
            .reserve(
                &view,
                &record,
                "principal-a",
                "claude-3-opus",
                Some(33),
                0,
                None,
            )
            .expect("reserve");
        let id = res.id().to_owned();
        std::mem::forget(res);
        // Advance less than the TTL: reservation must survive the sweep.
        tokio::time::advance(Duration::from_millis(20)).await;
        sweep_expired(
            &engine,
            Duration::from_millis(50),
            Duration::from_millis(200),
        );
        assert_eq!(
            engine.current_total(&key_id, LimitKind::OutputTokens, 60, now_sec),
            33,
        );
        assert!(engine.refund_by_id(&id));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn touch_reservation_extends_idle_ttl_up_to_hard_max_ttl() {
        let (engine, view, record) = engine_with_output_token_limit(1_000_000);
        let key_id = key_id_for(&record);
        let now_sec = unix_secs(engine.inner.clock.now());
        let res = engine
            .reserve(
                &view,
                &record,
                "principal-a",
                "claude-3-opus",
                Some(40),
                0,
                None,
            )
            .expect("reserve");
        let id = res.id().to_owned();
        std::mem::forget(res);

        assert!(!engine.touch_reservation_by_id("unknown-id"));

        // Touch at 30ms and 60ms: total age reaches 80ms (> 50ms idle_ttl) while
        // idle gap is only 20ms (< 50ms) and age < 120ms max_ttl.
        tokio::time::advance(Duration::from_millis(30)).await;
        assert!(engine.touch_reservation_by_id(&id));
        tokio::time::advance(Duration::from_millis(30)).await;
        assert!(engine.touch_reservation_by_id(&id));
        tokio::time::advance(Duration::from_millis(20)).await;
        sweep_expired(
            &engine,
            Duration::from_millis(50),
            Duration::from_millis(120),
        );
        assert_eq!(
            engine.current_total(&key_id, LimitKind::OutputTokens, 60, now_sec),
            40,
            "touched reservation must survive past idle_ttl",
        );

        // Touching again past max_ttl (80 + 40 = 120ms) cannot prevent hard-cap eviction.
        tokio::time::advance(Duration::from_millis(40)).await;
        assert!(engine.touch_reservation_by_id(&id));
        sweep_expired(
            &engine,
            Duration::from_millis(50),
            Duration::from_millis(120),
        );
        assert_eq!(
            engine.current_total(&key_id, LimitKind::OutputTokens, 60, now_sec),
            0,
            "reservation must still expire at max_ttl despite recent activity",
        );
        assert!(!engine.touch_reservation_by_id(&id));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn touched_reservation_reconciles_cleanly_after_idle_ttl() {
        let (engine, view, record) = engine_with_output_token_limit(1_000_000);
        let key_id = key_id_for(&record);
        let now_sec = unix_secs(engine.inner.clock.now());
        let res = engine
            .reserve(
                &view,
                &record,
                "principal-a",
                "claude-3-opus",
                Some(40),
                0,
                None,
            )
            .expect("reserve");
        let id = res.id().to_owned();
        std::mem::forget(res);

        tokio::time::advance(Duration::from_millis(30)).await;
        assert!(engine.touch_reservation_by_id(&id));
        tokio::time::advance(Duration::from_millis(30)).await;
        sweep_expired(
            &engine,
            Duration::from_millis(50),
            Duration::from_millis(200),
        );

        assert!(engine.reconcile_by_id(&id, 5, 12, None));
        assert!(!engine.touch_reservation_by_id(&id));
        assert_eq!(
            engine.current_total(&key_id, LimitKind::OutputTokens, 60, now_sec),
            12,
        );
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
        assert_eq!(api_key_usage_max_staleness_secs(60, 601), 6);
        assert_eq!(api_key_usage_max_staleness_secs(60, 1_501), 14);

        assert_eq!(api_key_usage_max_staleness_secs(60, 100_000), 300);
    }

    fn complete_reserved_request(engine: &Arc<LimitEngine>, reservation: Reservation) {
        let id = reservation.id().to_owned();
        reservation.forget();
        assert!(engine.reconcile_by_id(&id, 0, 0, Some(0)));
    }

    #[tokio::test]
    async fn local_limit_retryability_cold_durable_allowance_is_temporary_but_zero_cap_is_impossible()
     {
        // QA-05/09/13: stop the refresh task before registering any keys, so
        // every reserve deterministically uses the cold durable allowance.
        let directory = tempfile::tempdir().expect("tempdir");
        let database_url = format!(
            "sqlite://{}",
            directory
                .path()
                .join("cold-limit-retryability.sqlite")
                .display()
        );
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
        let storage_impl = cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
            .await
            .expect("open sqlite");
        storage_impl.initialize().await.expect("migrate sqlite");
        let engine = LimitEngine::new(Arc::new(KeyConcurrencyManager::new()), clock);
        engine
            .start_durable_usage_sync(Arc::new(storage_impl))
            .await
            .expect("start durable usage")
            .shutdown()
            .await;
        let view = PrincipalView::for_tests(
            "principal-a",
            true,
            vec!["claude-3-opus".to_owned()],
            Vec::new(),
            HashMap::new(),
        );

        for (index, (kind, max_output, max_input, cost)) in [
            (LimitKind::Requests, Some(0), 0, None),
            (LimitKind::InputTokens, Some(0), 11, None),
            (LimitKind::OutputTokens, Some(11), 0, None),
            (LimitKind::TotalTokens, Some(6), 5, None),
            (LimitKind::CostUsd, Some(0), 0, Some(11)),
        ]
        .into_iter()
        .enumerate()
        {
            let record = StoredApiKeyRecord {
                index_hash: [u8::try_from(index + 1).expect("small case index"); 32],
                limit_overrides: vec![Limit {
                    kind,
                    window_secs: 60,
                    cap_micros: 100,
                }],
                ..StoredApiKeyRecord::default()
            };
            // A request reserves one unit; fill its ten-unit cold allowance.
            let held: Vec<_> = if kind == LimitKind::Requests {
                (0..10)
                    .map(|_| {
                        engine
                            .reserve(
                                &view,
                                &record,
                                "principal-a",
                                "claude-3-opus",
                                Some(0),
                                0,
                                None,
                            )
                            .expect("positive cold request allowance")
                    })
                    .collect()
            } else {
                Vec::new()
            };
            let reason = engine
                .reserve(
                    &view,
                    &record,
                    "principal-a",
                    "claude-3-opus",
                    max_output,
                    max_input,
                    cost,
                )
                .err()
                .expect("cold allowance is exhausted");
            match kind {
                LimitKind::Requests => assert!(matches!(reason, RejectReason::RequestsRateLimit)),
                LimitKind::CostUsd => assert!(matches!(reason, RejectReason::CostRateLimit)),
                _ => assert!(
                    matches!(reason, RejectReason::TokenRateLimit { kind: rejected } if rejected == kind)
                ),
            }
            drop(held);
        }

        let record = StoredApiKeyRecord {
            index_hash: [21; 32],
            limit_overrides: vec![Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: 0,
            }],
            ..StoredApiKeyRecord::default()
        };

        let unsupported_record = StoredApiKeyRecord {
            limit_overrides: vec![
                Limit {
                    kind: LimitKind::Requests,
                    window_secs: 60,
                    cap_micros: 0,
                },
                Limit {
                    kind: LimitKind::InputTokens,
                    window_secs: 604_801,
                    cap_micros: 100,
                },
            ],
            ..record.clone()
        };
        assert!(matches!(
            engine
                .reserve(
                    &view,
                    &unsupported_record,
                    "principal-a",
                    "claude-3-opus",
                    Some(0),
                    0,
                    None,
                )
                .err(),
            Some(RejectReason::UnsupportedLimitWindow {
                window_secs: 604_801
            }),
        ));
        let reason = engine
            .reserve(
                &view,
                &record,
                "principal-a",
                "claude-3-opus",
                Some(0),
                0,
                None,
            )
            .err()
            .expect("configured zero cannot admit even the cold minimum of one");
        assert!(
            !matches!(reason, RejectReason::RequestsRateLimit),
            "{reason:?}"
        );
    }

    #[tokio::test]
    async fn durable_usage_coordinates_request_limits_across_engines() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_url = format!(
            "sqlite://{}",
            directory.path().join("durable-limits.sqlite").display()
        );
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
        let storage_impl = cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
            .await
            .expect("open sqlite");
        storage_impl.initialize().await.expect("migrate sqlite");
        let storage: Arc<dyn Storage> = Arc::new(storage_impl);
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
                .reserve(
                    &view,
                    &record,
                    "principal-a",
                    "claude-3-opus",
                    Some(0),
                    0,
                    None,
                )
                .expect("engine A cold allowance"),
        );
        complete_reserved_request(
            &engine_b,
            engine_b
                .reserve(
                    &view,
                    &record,
                    "principal-a",
                    "claude-3-opus",
                    Some(0),
                    0,
                    None,
                )
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
                    .reserve(
                        &view,
                        &record,
                        "principal-a",
                        "claude-3-opus",
                        Some(0),
                        0,
                        None,
                    )
                    .expect("shared request budget remains"),
            );
        }
        assert_eq!(
            engine_a
                .reserve(
                    &view,
                    &record,
                    "principal-a",
                    "claude-3-opus",
                    Some(0),
                    0,
                    None
                )
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
                .reserve(
                    &view,
                    &record,
                    "principal-a",
                    "claude-3-opus",
                    Some(0),
                    0,
                    None
                )
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

    #[tokio::test]
    async fn durable_cost_reload_keeps_each_window_separate() {
        for (actual_cost, cost_limited, expected_cost) in [
            (Some(25), true, 50),
            (Some(0), true, 25),
            (None, true, 125),
            (None, false, 25),
        ] {
            let directory = tempfile::tempdir().expect("tempdir");
            let database_url = format!(
                "sqlite://{}",
                directory.path().join("cost-reload.sqlite").display()
            );
            let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
            let storage_impl = cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
                .await
                .expect("open sqlite");
            storage_impl.initialize().await.expect("migrate sqlite");
            let storage: Arc<dyn Storage> = Arc::new(storage_impl);
            let mut limits = Vec::new();
            for window_secs in [60, 3_600] {
                for kind in [
                    LimitKind::Requests,
                    LimitKind::InputTokens,
                    LimitKind::OutputTokens,
                    LimitKind::TotalTokens,
                ] {
                    limits.push(Limit {
                        kind,
                        window_secs,
                        cap_micros: 1_000,
                    });
                }
                if cost_limited {
                    limits.push(Limit {
                        kind: LimitKind::CostUsd,
                        window_secs,
                        cap_micros: 1_000,
                    });
                }
            }
            limits.push(Limit {
                kind: LimitKind::Concurrent,
                window_secs: 0,
                cap_micros: 1,
            });
            let view = PrincipalView::for_tests(
                "cost-reload-principal",
                true,
                vec!["gpt-test".to_owned()],
                limits.clone(),
                HashMap::new(),
            );
            let record = StoredApiKeyRecord {
                key_hash_b64: "cost-reload-key".to_owned(),
                ..Default::default()
            };
            let key_id = key_id_for(&record);
            let now_sec = unix_secs(clock.now());
            let concurrency = Arc::new(KeyConcurrencyManager::new());
            let engine_a = LimitEngine::new(concurrency.clone(), clock.clone());
            let handle_a = engine_a
                .start_durable_usage_sync(storage.clone())
                .await
                .expect("start durable usage");
            let state_a = engine_a
                .inner
                .durable_usage
                .read()
                .clone()
                .expect("engine A durable state");
            state_a.record_key_limits(key_id.clone(), limits.clone());
            // Load the empty durable snapshot before admission so the fixture
            // uses full capacity, not the intentionally smaller cold allowance.
            refresh_remote_api_key_usage(&engine_a, &state_a, now_sec)
                .await
                .expect("initialize durable admission capacity");

            // Keep an existing monetary balance so unknown-without-estimate
            // must preserve it, rather than merely writing another zero.
            let seed = engine_a
                .reserve(
                    &view,
                    &record,
                    "cost-reload-principal",
                    "gpt-test",
                    Some(100),
                    100,
                    Some(25),
                )
                .expect("reserve initial measured cost");
            engine_a.reconcile(seed, 7, 11, Some(25));
            assert_eq!(concurrency.current(&key_id), 0);

            let reservation = engine_a
                .reserve(
                    &view,
                    &record,
                    "cost-reload-principal",
                    "gpt-test",
                    Some(100),
                    100,
                    cost_limited.then_some(100),
                )
                .expect("reserve estimated cost");
            let reservation_id = reservation.id().to_owned();
            reservation.forget();
            assert_eq!(concurrency.current(&key_id), 1);
            if cost_limited {
                for window in [60, 3_600] {
                    assert_eq!(
                        engine_a.current_total(&key_id, LimitKind::CostUsd, window, now_sec),
                        125
                    );
                }
            }
            assert!(engine_a.reconcile_by_id(&reservation_id, 13, 17, actual_cost));
            assert_eq!(concurrency.current(&key_id), 0);
            assert!(
                !engine_a
                    .inner
                    .reservations
                    .lock()
                    .contains_key(&reservation_id)
            );
            sweep_expired(&engine_a, Duration::ZERO, Duration::ZERO);
            assert!(!engine_a.refund_by_id(&reservation_id));
            assert!(!engine_a.reconcile_by_id(&reservation_id, 99, 99, Some(999)));
            assert!(!engine_a.reconcile_by_id(&reservation_id, 0, 0, Some(0)));
            assert_eq!(concurrency.current(&key_id), 0);
            for window in [60, 3_600] {
                for (kind, expected) in [
                    (LimitKind::Requests, 2),
                    (LimitKind::InputTokens, 20),
                    (LimitKind::OutputTokens, 28),
                    (LimitKind::TotalTokens, 48),
                ] {
                    assert_eq!(
                        engine_a.current_total(&key_id, kind, window, now_sec),
                        expected
                    );
                }
                if cost_limited {
                    assert_eq!(
                        engine_a.current_total(&key_id, LimitKind::CostUsd, window, now_sec),
                        expected_cost,
                        "each window retains one liability, not the sum of all windows"
                    );
                }
            }

            flush_durable_api_key_usage(&engine_a, &state_a, now_sec)
                .await
                .expect("flush settled usage");
            handle_a.shutdown().await;

            let concurrency_b = Arc::new(KeyConcurrencyManager::new());
            let engine_b = LimitEngine::new(concurrency_b.clone(), clock);
            let handle_b = engine_b
                .start_durable_usage_sync(storage.clone())
                .await
                .expect("restart durable usage");
            let state_b = engine_b
                .inner
                .durable_usage
                .read()
                .clone()
                .expect("engine B durable state");
            state_b.record_key_limits(key_id.clone(), limits);
            refresh_remote_api_key_usage(&engine_b, &state_b, now_sec)
                .await
                .expect("reload durable cost");
            for window in [60, 3_600] {
                for (kind, expected) in [
                    (LimitKind::Requests, 2),
                    (LimitKind::InputTokens, 20),
                    (LimitKind::OutputTokens, 28),
                    (LimitKind::TotalTokens, 48),
                ] {
                    assert_eq!(
                        engine_b.current_total(&key_id, kind, window, now_sec),
                        expected
                    );
                }
                if cost_limited {
                    assert_eq!(
                        engine_b.current_total(&key_id, LimitKind::CostUsd, window, now_sec),
                        expected_cost
                    );
                }
            }
            if cost_limited {
                let probe = engine_b.reserve(
                    &view,
                    &record,
                    "cost-reload-principal",
                    "gpt-test",
                    Some(0),
                    0,
                    Some(1_001 - expected_cost),
                );
                assert!(matches!(probe, Err(RejectReason::CostRateLimit)));
                let probe = engine_b
                    .reserve(
                        &view,
                        &record,
                        "cost-reload-principal",
                        "gpt-test",
                        Some(0),
                        0,
                        Some(1_000 - expected_cost),
                    )
                    .expect("the exact remaining monetary budget is admissible");
                assert_eq!(concurrency_b.current(&key_id), 1);
                drop(probe);
                assert_eq!(concurrency_b.current(&key_id), 0);
            }
            let buckets = storage
                .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
                    key_ids: vec![key_id.clone()],
                    since_unix_secs: now_sec.saturating_sub(3_600),
                    until_unix_secs: now_sec,
                    exclude_writer_epoch: Uuid::nil(),
                })
                .await
                .expect("query persisted usage");
            let mut usage_by_width = buckets
                .into_iter()
                .map(|bucket| {
                    (
                        bucket.key.bucket_width_secs,
                        bucket.usage.requests,
                        bucket.usage.input_tokens,
                        bucket.usage.output_tokens,
                        bucket.usage.cost_usd_micros,
                    )
                })
                .collect::<Vec<_>>();
            usage_by_width.sort_unstable();
            // The 60-second and 3,600-second limit windows persist in
            // 1-second and 60-second buckets, respectively.
            assert_eq!(
                usage_by_width,
                vec![
                    (1, 2, 20, 28, expected_cost),
                    (60, 2, 20, 28, expected_cost)
                ]
            );
            handle_b.shutdown().await;
        }
    }

    /// In-memory stand-in for `api_key_concurrency_holds_v1` implementing the
    /// Postgres hold semantics (insert / delete / age-filtered count).
    #[derive(Default)]
    struct FakeConcurrencyHoldStore {
        holds: Mutex<HashMap<Uuid, ApiKeyConcurrencyHold>>,
        now_secs: std::sync::atomic::AtomicU64,
    }

    impl FakeConcurrencyHoldStore {
        fn set_now(&self, now_secs: u64) {
            self.now_secs.store(now_secs, Ordering::Relaxed);
        }

        fn current_now(&self) -> u64 {
            let configured = self.now_secs.load(Ordering::Relaxed);
            if configured == 0 {
                unix_secs(std::time::SystemTime::now())
            } else {
                configured
            }
        }
    }

    #[async_trait::async_trait]
    impl ApiKeyConcurrencyHoldStore for FakeConcurrencyHoldStore {
        async fn insert_api_key_concurrency_hold(
            &self,
            hold: &ApiKeyConcurrencyHold,
        ) -> Result<(), StorageError> {
            self.holds.lock().insert(hold.hold_id, hold.clone());
            Ok(())
        }

        async fn delete_api_key_concurrency_hold(&self, hold_id: Uuid) -> Result<(), StorageError> {
            self.holds.lock().remove(&hold_id);
            Ok(())
        }

        async fn count_api_key_concurrency_holds(
            &self,
            key_id: &str,
            now_unix_secs: u64,
            max_age_secs: u64,
        ) -> Result<u64, StorageError> {
            let cutoff = now_unix_secs.saturating_sub(max_age_secs);
            Ok(self
                .holds
                .lock()
                .values()
                .filter(|hold| hold.key_id == key_id && hold.acquired_at_unix_secs > cutoff)
                .count() as u64)
        }

        async fn delete_expired_api_key_concurrency_holds(
            &self,
            now_unix_secs: u64,
            max_age_secs: u64,
        ) -> Result<u64, StorageError> {
            let cutoff = now_unix_secs.saturating_sub(max_age_secs);
            let mut holds = self.holds.lock();
            let before = holds.len();
            holds.retain(|_, hold| hold.acquired_at_unix_secs > cutoff);
            Ok((before - holds.len()) as u64)
        }

        async fn refresh_api_key_concurrency_holds(
            &self,
            hold_ids: &[Uuid],
            now_unix_secs: u64,
        ) -> Result<u64, StorageError> {
            let mut holds = self.holds.lock();
            let mut updated = 0_u64;
            for hold_id in hold_ids {
                if let Some(hold) = holds.get_mut(hold_id) {
                    hold.acquired_at_unix_secs = now_unix_secs;
                    updated += 1;
                }
            }
            Ok(updated)
        }

        async fn try_acquire_api_key_concurrency_hold(
            &self,
            hold_id: Uuid,
            key_id: &str,
            writer_epoch: Uuid,
            cap: u32,
            max_age_secs: u64,
        ) -> Result<bool, StorageError> {
            let now_unix_secs = self.current_now();
            let cutoff = now_unix_secs.saturating_sub(max_age_secs);
            let mut holds = self.holds.lock();
            let live = holds
                .values()
                .filter(|hold| hold.key_id == key_id && hold.acquired_at_unix_secs > cutoff)
                .count() as u64;
            if live >= u64::from(cap) {
                return Ok(false);
            }
            holds.insert(
                hold_id,
                ApiKeyConcurrencyHold {
                    hold_id,
                    key_id: key_id.to_owned(),
                    writer_epoch,
                    acquired_at_unix_secs: now_unix_secs,
                },
            );
            Ok(true)
        }
    }

    fn hold_for(key_id: &str, writer_epoch: Uuid, acquired_at: u64) -> ApiKeyConcurrencyHold {
        ApiKeyConcurrencyHold {
            hold_id: Uuid::now_v7(),
            key_id: key_id.to_owned(),
            writer_epoch,
            acquired_at_unix_secs: acquired_at,
        }
    }

    /// Two writer epochs against the same postgres-like store cannot both
    /// hold a `Concurrent` cap of 1 (issue 807).
    #[tokio::test]
    async fn concurrent_holds_enforce_cap_across_writer_epochs() {
        let store = FakeConcurrencyHoldStore::default();
        let now_sec = 1_800_000_000_u64;
        store.set_now(now_sec);
        let writer_a = Uuid::now_v7();
        let writer_b = Uuid::now_v7();

        let hold_a = hold_for("key-a", writer_a, now_sec);
        assert!(
            try_acquire_hold(&store, &hold_a, 1, now_sec)
                .await
                .expect("first hold"),
            "first writer should win the single slot",
        );

        let hold_b = hold_for("key-a", writer_b, now_sec);
        assert!(
            !try_acquire_hold(&store, &hold_b, 1, now_sec)
                .await
                .expect("second hold"),
            "second writer must be rejected while the slot is held",
        );
        assert_eq!(
            store.holds.lock().len(),
            1,
            "rejected hold must be rolled back",
        );

        // Crash leftover: a hold older than the writer-lease window no longer
        // counts, so a new writer can take the slot.
        let stale = now_sec + API_KEY_CONCURRENCY_HOLD_MAX_AGE_SECS + 1;
        store.set_now(stale);
        let hold_c = hold_for("key-a", writer_b, stale);
        assert!(
            try_acquire_hold(&store, &hold_c, 1, stale)
                .await
                .expect("stale hold"),
            "expired crash leftover must not block admission",
        );
    }

    /// SQLite keeps the local in-process counter: durable sync is active but
    /// the `Concurrent` cap is still enforced by `KeyConcurrencyManager`.
    #[tokio::test]
    async fn sqlite_durable_sync_keeps_local_concurrent_cap() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_url = format!(
            "sqlite://{}",
            directory.path().join("concurrent.sqlite").display()
        );
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
        let storage_impl = cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
            .await
            .expect("open sqlite");
        storage_impl.initialize().await.expect("migrate sqlite");
        let storage: Arc<dyn Storage> = Arc::new(storage_impl);
        let engine = LimitEngine::new(Arc::new(KeyConcurrencyManager::new()), clock);
        let handle = engine
            .start_durable_usage_sync(storage)
            .await
            .expect("start durable usage");

        let view = PrincipalView::for_tests(
            "principal-a",
            true,
            vec!["claude-3-opus".to_owned()],
            vec![Limit {
                kind: LimitKind::Concurrent,
                window_secs: 0,
                cap_micros: 1,
            }],
            HashMap::new(),
        );
        let record = StoredApiKeyRecord {
            index_hash: [13u8; 32],
            key_hash_b64: "sqlite-concurrent-key".to_owned(),
            ..StoredApiKeyRecord::default()
        };

        let first = engine
            .reserve(
                &view,
                &record,
                "principal-a",
                "claude-3-opus",
                Some(0),
                0,
                None,
            )
            .expect("first reservation takes the local slot");
        assert_eq!(
            engine
                .reserve(
                    &view,
                    &record,
                    "principal-a",
                    "claude-3-opus",
                    Some(0),
                    0,
                    None
                )
                .err(),
            Some(RejectReason::ConcurrentRateLimit),
            "local cap must still reject the second reservation",
        );
        drop(first);
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn live_durable_concurrency_holds_survive_past_max_age_and_release_on_handoff() {
        let store = Arc::new(FakeConcurrencyHoldStore::default());
        let storage_dyn: Arc<dyn Storage> = {
            let directory = tempfile::tempdir().expect("tempdir");
            let database_url = format!(
                "sqlite://{}",
                directory.path().join("hold-refresh.sqlite").display()
            );
            let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
            let storage_impl = cc_lb_storage_sqlite::open_sqlite(&database_url, clock)
                .await
                .expect("open sqlite");
            storage_impl.initialize().await.expect("migrate sqlite");
            Arc::new(storage_impl)
        };
        let t0 = 1_800_000_000_u64;
        let engine = LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            Arc::new(cc_lb_clock::TestClock::new_at_secs(t0)),
        );
        let writer = Uuid::now_v7();
        let live_hold = hold_for("key-long", writer, t0);
        let live_hold_id = live_hold.hold_id;
        let crashed_hold = hold_for("key-long", Uuid::now_v7(), t0);
        let crashed_hold_id = crashed_hold.hold_id;
        store
            .insert_api_key_concurrency_hold(&live_hold)
            .await
            .expect("insert live hold");
        store
            .insert_api_key_concurrency_hold(&crashed_hold)
            .await
            .expect("insert crashed hold");

        let res_id = "res-durable-refresh".to_owned();
        let mono_now = Instant::now();
        engine.inner.reservations.lock().insert(
            res_id.clone(),
            ReservationRecord {
                key_id: "key-long".to_owned(),
                reserved: Vec::new(),
                admitted_input_estimate: 0,
                admitted_output_estimate: 0,
                admitted_cost_estimate_micros: None,
                durable_bucket_widths: Vec::new(),
                concurrent_guards: vec![ConcurrentSlot::Durable(DurableConcurrencyHold::new(
                    storage_dyn,
                    live_hold_id,
                    "key-long".to_owned(),
                ))],
                created_at: mono_now,
                last_activity_at: mono_now,
            },
        );
        assert_eq!(
            engine.live_durable_concurrency_hold_ids(),
            vec![live_hold_id]
        );

        let mut last_refresh = None;
        for elapsed in [30_u64, 60, 95] {
            maintain_durable_concurrency_holds(
                &engine,
                store.as_ref(),
                t0 + elapsed,
                &mut last_refresh,
            )
            .await;
        }
        let holds = store.holds.lock();
        assert!(
            holds.contains_key(&live_hold_id),
            "refreshed live hold must survive past the 90s crash-leftover bound"
        );
        assert_eq!(holds[&live_hold_id].acquired_at_unix_secs, t0 + 95);
        assert!(
            !holds.contains_key(&crashed_hold_id),
            "unrefreshed crash leftover must still expire after 90s"
        );
        drop(holds);

        let reservation = Reservation {
            engine: Arc::downgrade(&engine.inner),
            id: res_id,
            forgotten: false,
        };
        reservation.handoff_after_response();
        assert!(
            engine.live_durable_concurrency_hold_ids().is_empty(),
            "handoff must drop durable concurrency guards out of the live refresh set"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn sweep_expired_evicts_atomically_at_boundary_after_touch() {
        let (engine, view, record) = engine_with_output_token_limit(1_000_000);
        let key_id = key_id_for(&record);
        let now_sec = unix_secs(engine.inner.clock.now());
        let res = engine
            .reserve(
                &view,
                &record,
                "principal-a",
                "claude-3-opus",
                Some(55),
                0,
                None,
            )
            .expect("reserve");
        let id = res.id().to_owned();
        res.forget();

        // Advance right up to the idle boundary, then refresh immediately before sweeping.
        tokio::time::advance(Duration::from_millis(50)).await;
        assert!(engine.touch_reservation_by_id(&id));
        sweep_expired(
            &engine,
            Duration::from_millis(50),
            Duration::from_millis(200),
        );
        assert_eq!(
            engine.current_total(&key_id, LimitKind::OutputTokens, 60, now_sec),
            55,
            "refreshed reservation must survive atomic expiry sweep",
        );

        // Once truly idle past idle_ttl without refresh, sweep removes and refunds atomically.
        tokio::time::advance(Duration::from_millis(50)).await;
        sweep_expired(
            &engine,
            Duration::from_millis(50),
            Duration::from_millis(200),
        );
        assert!(!engine.touch_reservation_by_id(&id));
        assert_eq!(
            engine.current_total(&key_id, LimitKind::OutputTokens, 60, now_sec),
            0,
        );
    }

    struct FailingConcurrencyHoldStore;

    #[async_trait::async_trait]
    impl ApiKeyConcurrencyHoldStore for FailingConcurrencyHoldStore {
        async fn insert_api_key_concurrency_hold(
            &self,
            _hold: &ApiKeyConcurrencyHold,
        ) -> Result<(), StorageError> {
            Err(StorageError::Unavailable {
                message: "postgres down".to_owned(),
            })
        }

        async fn delete_api_key_concurrency_hold(
            &self,
            _hold_id: Uuid,
        ) -> Result<(), StorageError> {
            Err(StorageError::Unavailable {
                message: "postgres down".to_owned(),
            })
        }

        async fn count_api_key_concurrency_holds(
            &self,
            _key_id: &str,
            _now_unix_secs: u64,
            _max_age_secs: u64,
        ) -> Result<u64, StorageError> {
            Err(StorageError::Unavailable {
                message: "postgres down".to_owned(),
            })
        }

        async fn delete_expired_api_key_concurrency_holds(
            &self,
            _now_unix_secs: u64,
            _max_age_secs: u64,
        ) -> Result<u64, StorageError> {
            Err(StorageError::Unavailable {
                message: "postgres down".to_owned(),
            })
        }

        async fn refresh_api_key_concurrency_holds(
            &self,
            _hold_ids: &[Uuid],
            _now_unix_secs: u64,
        ) -> Result<u64, StorageError> {
            Err(StorageError::Unavailable {
                message: "postgres down".to_owned(),
            })
        }

        async fn try_acquire_api_key_concurrency_hold(
            &self,
            _hold_id: Uuid,
            _key_id: &str,
            _writer_epoch: Uuid,
            _cap: u32,
            _max_age_secs: u64,
        ) -> Result<bool, StorageError> {
            Err(StorageError::Unavailable {
                message: "postgres down".to_owned(),
            })
        }
    }

    #[test]
    fn atomic_rolling_reservation_prevents_concurrent_over_admission() {
        let engine = LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000)),
        );
        let view = PrincipalView::for_tests(
            "principal-atomic",
            true,
            vec!["claude-3-opus".to_owned()],
            vec![
                Limit {
                    kind: LimitKind::Requests,
                    window_secs: 60,
                    cap_micros: 5,
                },
                Limit {
                    kind: LimitKind::OutputTokens,
                    window_secs: 60,
                    cap_micros: 500,
                },
            ],
            HashMap::new(),
        );
        let record = StoredApiKeyRecord {
            key_hash_b64: "atomic-key".to_owned(),
            ..StoredApiKeyRecord::default()
        };
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(16));
        let mut results = Vec::new();
        std::thread::scope(|scope| {
            let handles = (0..16)
                .map(|_| {
                    let engine = Arc::clone(&engine);
                    let view = &view;
                    let record = &record;
                    let barrier = Arc::clone(&barrier);
                    scope.spawn(move || {
                        barrier.wait();
                        let res = engine.reserve(
                            view,
                            record,
                            "principal-atomic",
                            "claude-3-opus",
                            Some(100),
                            0,
                            None,
                        );
                        if let Ok(reservation) = &res {
                            let _ = reservation.id().len();
                        }
                        res
                    })
                })
                .collect::<Vec<_>>();
            for handle in handles {
                results.push(handle.join().expect("thread join"));
            }
        });
        let admitted: Vec<_> = results.into_iter().filter_map(Result::ok).collect();
        assert_eq!(admitted.len(), 5, "exact cap must be admitted under race");
        let now_sec = unix_secs(engine.inner.clock.now());
        assert_eq!(
            engine.current_total("atomic-key", LimitKind::Requests, 60, now_sec),
            5
        );
        assert_eq!(
            engine.current_total("atomic-key", LimitKind::OutputTokens, 60, now_sec),
            500
        );
        drop(admitted);
    }

    #[tokio::test]
    async fn reserve_async_hydrates_cold_key_with_singleflight_and_uses_full_cap() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_url = format!(
            "sqlite://{}",
            directory.path().join("cold-hydrate.sqlite").display()
        );
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
        let storage_impl = cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
            .await
            .expect("open sqlite");
        storage_impl.initialize().await.expect("migrate sqlite");
        let storage: Arc<dyn Storage> = Arc::new(storage_impl);
        let view = PrincipalView::for_tests(
            "principal-cold",
            true,
            vec!["claude-3-opus".to_owned()],
            vec![Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: 20,
            }],
            HashMap::new(),
        );
        let record = StoredApiKeyRecord {
            key_hash_b64: "cold-hydrate-key".to_owned(),
            ..StoredApiKeyRecord::default()
        };

        // Pre-populate 15 durable requests from another writer epoch.
        let writer_a = LimitEngine::new(Arc::new(KeyConcurrencyManager::new()), clock.clone());
        let handle_a = writer_a
            .start_durable_usage_sync(storage.clone())
            .await
            .expect("start writer A");
        for _ in 0..15 {
            let res = writer_a
                .reserve_async(
                    &view,
                    &record,
                    "principal-cold",
                    "claude-3-opus",
                    Some(0),
                    0,
                    None,
                )
                .await
                .expect("seed usage");
            complete_reserved_request(&writer_a, res);
        }
        handle_a.shutdown().await;

        // Fresh engine starts cold for `cold-hydrate-key`; cold allowance cap/10
        // would only be 2, whereas singleflight hydration loads 15 remote
        // requests and unlocks the true remaining capacity of 5 (20 - 15).
        let engine_b = LimitEngine::new(Arc::new(KeyConcurrencyManager::new()), clock.clone());
        let handle_b = engine_b
            .start_durable_usage_sync(storage.clone())
            .await
            .expect("start engine B");

        let (r1, r2, r3, r4) = tokio::join!(
            engine_b.reserve_async(
                &view,
                &record,
                "principal-cold",
                "claude-3-opus",
                Some(0),
                0,
                None
            ),
            engine_b.reserve_async(
                &view,
                &record,
                "principal-cold",
                "claude-3-opus",
                Some(0),
                0,
                None
            ),
            engine_b.reserve_async(
                &view,
                &record,
                "principal-cold",
                "claude-3-opus",
                Some(0),
                0,
                None
            ),
            engine_b.reserve_async(
                &view,
                &record,
                "principal-cold",
                "claude-3-opus",
                Some(0),
                0,
                None
            ),
        );
        let held = [r1, r2, r3, r4]
            .into_iter()
            .map(|r| r.expect("hydrated full cap must admit 4 concurrent requests"))
            .collect::<Vec<_>>();
        let fifth = engine_b
            .reserve_async(
                &view,
                &record,
                "principal-cold",
                "claude-3-opus",
                Some(0),
                0,
                None,
            )
            .await
            .expect("5th request fills 20/20 cap");
        let sixth = engine_b
            .reserve_async(
                &view,
                &record,
                "principal-cold",
                "claude-3-opus",
                Some(0),
                0,
                None,
            )
            .await;
        assert_eq!(sixth.err(), Some(RejectReason::RequestsRateLimit));

        drop(fifth);
        drop(held);
        handle_b.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn durable_concurrent_hold_fails_closed_when_postgres_store_unavailable() {
        let store = FailingConcurrencyHoldStore;
        let err = try_acquire_hold_atomic(&store, Uuid::now_v7(), "key-pg", Uuid::now_v7(), 1)
            .await
            .expect_err("failing hold store must return error");
        assert!(matches!(err, StorageError::Unavailable { .. }));

        let directory = tempfile::tempdir().expect("tempdir");
        let database_url = format!(
            "sqlite://{}",
            directory.path().join("fail-closed-conc.sqlite").display()
        );
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
        let storage_impl = cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
            .await
            .expect("open sqlite");
        storage_impl.initialize().await.expect("migrate sqlite");
        let concurrency = Arc::new(KeyConcurrencyManager::new());
        let engine = LimitEngine::new(concurrency.clone(), clock);
        *engine.inner.durable_usage.write() = Some(Arc::new(DurableApiKeyUsage {
            storage: Arc::new(storage_impl),
            backend_kind: BackendKind::Postgres,
            writer_epoch: RwLock::new(Uuid::now_v7()),
            pending: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(None),
            key_limits: RwLock::new(KeyLimitRegistry::default()),
            refreshed_at: RwLock::new(HashMap::new()),
            refresh_cursor: AtomicU32::new(0),
            cold_hydrations: Mutex::new(HashMap::new()),
        }));

        let view = PrincipalView::for_tests(
            "principal-pg",
            true,
            vec!["claude-3-opus".to_owned()],
            vec![Limit {
                kind: LimitKind::Concurrent,
                window_secs: 0,
                cap_micros: 1,
            }],
            HashMap::new(),
        );
        let record = StoredApiKeyRecord {
            key_hash_b64: "pg-conc-key".to_owned(),
            ..StoredApiKeyRecord::default()
        };
        let err = engine
            .reserve(
                &view,
                &record,
                "principal-pg",
                "claude-3-opus",
                Some(0),
                0,
                None,
            )
            .err()
            .expect("unavailable durable concurrency backend must fail closed");
        assert_eq!(err, RejectReason::ConcurrentLimitUnavailable);
        assert_eq!(
            concurrency.current("pg-conc-key"),
            0,
            "local fallback must not be used when Postgres durable holds are unavailable"
        );
    }

    #[tokio::test]
    async fn handoff_preserves_flush_id_across_epoch_rotation() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_url = format!(
            "sqlite://{}",
            directory.path().join("handoff-flush-id.sqlite").display()
        );
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
        let storage_impl = cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
            .await
            .expect("open sqlite");
        storage_impl.initialize().await.expect("migrate sqlite");
        let storage: Arc<dyn Storage> = Arc::new(storage_impl);
        let engine = LimitEngine::new(Arc::new(KeyConcurrencyManager::new()), clock.clone());
        let handle = engine
            .start_durable_usage_sync(storage.clone())
            .await
            .expect("start durable usage");
        let state = engine
            .inner
            .durable_usage
            .read()
            .clone()
            .expect("durable state");
        let old_epoch = *state.writer_epoch.read();
        // Deterministically stop the background sync loop before mutating `in_flight`
        // and manually driving `handoff_durable_writer` / `flush_durable_api_key_usage`.
        handle.shutdown().await;
        let now_sec = unix_secs(clock.now());
        let flush_id = Uuid::now_v7();
        let deltas = vec![ApiKeyUsageBucketDelta {
            key: ApiKeyUsageBucketKey {
                key_id: "handoff-key".to_owned(),
                bucket_width_secs: 60,
                bucket_start_unix_secs: now_sec - (now_sec % 60),
            },
            usage: ApiKeyUsage {
                requests: 3,
                input_tokens: 30,
                output_tokens: 60,
                cost_usd_micros: 90,
            },
        }];

        // Apply the batch to storage as if the response was lost in flight, then
        // keep the identical batch in `state.in_flight` while expiring the lease.
        let applied = storage
            .flush_api_key_usage(&ApiKeyUsageFlush {
                writer_epoch: old_epoch,
                flush_id,
                lease_until_unix_secs: now_sec + 30,
                deltas: deltas.clone(),
            })
            .await
            .expect("seed flush");
        assert_eq!(applied, ApiKeyUsageFlushResult::Applied);

        clock.advance_secs(60);
        *state.in_flight.lock() = Some(ApiKeyUsageFlush {
            writer_epoch: old_epoch,
            flush_id,
            lease_until_unix_secs: now_sec + 30,
            deltas,
        });

        // `flush_durable_api_key_usage` sees `AlreadyApplied` or if `LeaseLost`
        // rotated epoch, `handoff_durable_writer` preserves `flush_id`.
        handoff_durable_writer(&engine, &state, unix_secs(clock.now()))
            .await
            .expect("handoff preserves flush_id");
        {
            let in_flight = state.in_flight.lock();
            let current = in_flight.as_ref().expect("in_flight retained");
            assert_eq!(current.flush_id, flush_id, "flush_id must survive handoff");
            assert_ne!(current.writer_epoch, old_epoch, "writer_epoch rotates");
        }

        flush_durable_api_key_usage(&engine, &state, unix_secs(clock.now()))
            .await
            .expect("retry deduplicates against rotated epoch");
        assert!(state.in_flight.lock().is_none());

        let buckets = storage
            .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
                key_ids: vec!["handoff-key".to_owned()],
                since_unix_secs: now_sec - 120,
                until_unix_secs: unix_secs(clock.now()) + 120,
                exclude_writer_epoch: Uuid::now_v7(),
            })
            .await
            .expect("query buckets");
        assert_eq!(buckets.iter().map(|b| b.usage.requests).sum::<i64>(), 3);
    }

    #[tokio::test]
    async fn commit_reserved_by_id_keeps_rolling_and_durable_usage_and_releases_concurrency() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_url = format!(
            "sqlite://{}",
            directory.path().join("commit-reserved.sqlite").display()
        );
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
        let storage_impl = cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
            .await
            .expect("open sqlite");
        storage_impl.initialize().await.expect("migrate sqlite");
        let storage: Arc<dyn Storage> = Arc::new(storage_impl);
        let concurrency = Arc::new(KeyConcurrencyManager::new());
        let engine = LimitEngine::new(concurrency.clone(), clock.clone());
        let handle = engine
            .start_durable_usage_sync(storage.clone())
            .await
            .expect("start durable usage");

        let view = PrincipalView::for_tests(
            "principal-commit",
            true,
            vec!["claude-3-opus".to_owned()],
            vec![
                Limit {
                    kind: LimitKind::Requests,
                    window_secs: 60,
                    cap_micros: 10,
                },
                Limit {
                    kind: LimitKind::InputTokens,
                    window_secs: 60,
                    cap_micros: 1_000,
                },
                Limit {
                    kind: LimitKind::OutputTokens,
                    window_secs: 60,
                    cap_micros: 1_000,
                },
                Limit {
                    kind: LimitKind::CostUsd,
                    window_secs: 60,
                    cap_micros: 1_000,
                },
                Limit {
                    kind: LimitKind::Concurrent,
                    window_secs: 0,
                    cap_micros: 1,
                },
            ],
            HashMap::new(),
        );
        let record = StoredApiKeyRecord {
            key_hash_b64: "commit-key".to_owned(),
            ..StoredApiKeyRecord::default()
        };
        let now_sec = unix_secs(clock.now());
        let res = engine
            .reserve_async(
                &view,
                &record,
                "principal-commit",
                "claude-3-opus",
                Some(100),
                50,
                Some(200),
            )
            .await
            .expect("reserve");
        let res_id = res.id().to_owned();
        assert_eq!(concurrency.current("commit-key"), 1);
        res.forget();

        assert!(engine.commit_reserved_by_id(&res_id));
        assert!(!engine.commit_reserved_by_id(&res_id));
        assert_eq!(concurrency.current("commit-key"), 0);
        assert_eq!(
            engine.current_total("commit-key", LimitKind::Requests, 60, now_sec),
            1
        );
        assert_eq!(
            engine.current_total("commit-key", LimitKind::InputTokens, 60, now_sec),
            50
        );
        assert_eq!(
            engine.current_total("commit-key", LimitKind::OutputTokens, 60, now_sec),
            100
        );
        assert_eq!(
            engine.current_total("commit-key", LimitKind::CostUsd, 60, now_sec),
            200
        );

        handle.shutdown().await;
        let buckets = storage
            .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
                key_ids: vec!["commit-key".to_owned()],
                since_unix_secs: now_sec - 60,
                until_unix_secs: now_sec + 60,
                exclude_writer_epoch: Uuid::now_v7(),
            })
            .await
            .expect("query buckets");
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].usage.requests, 1);
        assert_eq!(buckets[0].usage.input_tokens, 50);
        assert_eq!(buckets[0].usage.output_tokens, 100);
        assert_eq!(buckets[0].usage.cost_usd_micros, 200);
    }

    #[tokio::test]
    async fn commit_reserved_by_id_records_admitted_estimates_for_total_tokens_only_key() {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_url = format!(
            "sqlite://{}",
            directory.path().join("commit-total-only.sqlite").display()
        );
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
        let storage_impl = cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
            .await
            .expect("open sqlite");
        storage_impl.initialize().await.expect("migrate sqlite");
        let storage: Arc<dyn Storage> = Arc::new(storage_impl);
        let concurrency = Arc::new(KeyConcurrencyManager::new());
        let engine = LimitEngine::new(concurrency, clock.clone());
        let handle = engine
            .start_durable_usage_sync(storage.clone())
            .await
            .expect("start durable usage");

        let view = PrincipalView::for_tests(
            "principal-total-only",
            true,
            vec!["claude-3-opus".to_owned()],
            vec![Limit {
                kind: LimitKind::TotalTokens,
                window_secs: 60,
                cap_micros: 1_000,
            }],
            HashMap::new(),
        );
        let record = StoredApiKeyRecord {
            key_hash_b64: "total-only-key".to_owned(),
            ..StoredApiKeyRecord::default()
        };
        let now_sec = unix_secs(clock.now());
        let res = engine
            .reserve_async(
                &view,
                &record,
                "principal-total-only",
                "claude-3-opus",
                Some(120),
                45,
                None,
            )
            .await
            .expect("reserve");
        let res_id = res.id().to_owned();
        res.forget();

        assert!(engine.commit_reserved_by_id(&res_id));
        handle.shutdown().await;

        let buckets = storage
            .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
                key_ids: vec!["total-only-key".to_owned()],
                since_unix_secs: now_sec - 60,
                until_unix_secs: now_sec + 60,
                exclude_writer_epoch: Uuid::now_v7(),
            })
            .await
            .expect("query buckets");
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].usage.input_tokens, 45);
        assert_eq!(buckets[0].usage.output_tokens, 120);
    }

    #[tokio::test]
    async fn acquire_durable_hold_raw_cleans_up_committed_hold_on_drop_and_uncertain_error() {
        enum Mode {
            BlockThenCommit {
                entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
                release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
            },
            CommitThenErr,
        }

        struct RaceHoldStore {
            mode: Mode,
            committed: Mutex<Vec<Uuid>>,
            deleted: Mutex<Vec<Uuid>>,
            deleted_notify: tokio::sync::Notify,
        }

        #[async_trait::async_trait]
        impl ApiKeyConcurrencyHoldStore for RaceHoldStore {
            async fn insert_api_key_concurrency_hold(
                &self,
                _hold: &cc_lb_storage_api::ApiKeyConcurrencyHold,
            ) -> Result<(), StorageError> {
                Ok(())
            }

            async fn delete_api_key_concurrency_hold(
                &self,
                hold_id: Uuid,
            ) -> Result<(), StorageError> {
                self.deleted.lock().push(hold_id);
                self.committed.lock().retain(|id| *id != hold_id);
                self.deleted_notify.notify_waiters();
                Ok(())
            }

            async fn count_api_key_concurrency_holds(
                &self,
                _key_id: &str,
                _now_unix_secs: u64,
                _max_age_secs: u64,
            ) -> Result<u64, StorageError> {
                Ok(0)
            }

            async fn delete_expired_api_key_concurrency_holds(
                &self,
                _now_unix_secs: u64,
                _max_age_secs: u64,
            ) -> Result<u64, StorageError> {
                Ok(0)
            }

            async fn refresh_api_key_concurrency_holds(
                &self,
                _hold_ids: &[Uuid],
                _now_unix_secs: u64,
            ) -> Result<u64, StorageError> {
                Ok(0)
            }

            async fn try_acquire_api_key_concurrency_hold(
                &self,
                hold_id: Uuid,
                _key_id: &str,
                _writer_epoch: Uuid,
                _cap: u32,
                _max_age_secs: u64,
            ) -> Result<bool, StorageError> {
                match &self.mode {
                    Mode::BlockThenCommit { entered, release } => {
                        let tx = entered.lock().take();
                        if let Some(tx) = tx {
                            let _ = tx.send(());
                        }
                        let rx = release.lock().take();
                        if let Some(rx) = rx {
                            let _ = rx.await;
                        }
                        self.committed.lock().push(hold_id);
                        Ok(true)
                    }
                    Mode::CommitThenErr => {
                        self.committed.lock().push(hold_id);
                        Err(StorageError::Unavailable {
                            message: "connection reset after commit".to_owned(),
                        })
                    }
                }
            }
        }

        // Case 1: Caller future cancelled/dropped mid-flight still awaits commit and deletes hold.
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let store1 = Arc::new(RaceHoldStore {
            mode: Mode::BlockThenCommit {
                entered: Mutex::new(Some(entered_tx)),
                release: Mutex::new(Some(release_rx)),
            },
            committed: Mutex::new(Vec::new()),
            deleted: Mutex::new(Vec::new()),
            deleted_notify: tokio::sync::Notify::new(),
        });
        let epoch = Uuid::now_v7();
        {
            let mut fut = Box::pin(acquire_durable_hold_raw(&store1, epoch, "k-cancel", 1));
            let notified = entered_rx;
            tokio::select! {
                _ = &mut fut => panic!("hold attempt was blocked behind release_rx"),
                res = notified => res.expect("acquire started"),
            }
            drop(fut);
        }
        let _ = release_tx.send(());
        tokio::time::timeout(Duration::from_secs(2), async {
            while store1.deleted.lock().is_empty() {
                store1.deleted_notify.notified().await;
            }
        })
        .await
        .expect("cancelled acquire deletes committed hold");
        assert!(store1.committed.lock().is_empty());

        // Case 2: Uncertain storage error after server-side commit triggers cleanup deletion.
        let store2 = Arc::new(RaceHoldStore {
            mode: Mode::CommitThenErr,
            committed: Mutex::new(Vec::new()),
            deleted: Mutex::new(Vec::new()),
            deleted_notify: tokio::sync::Notify::new(),
        });
        assert!(matches!(
            acquire_durable_hold_raw(&store2, epoch, "k-err", 1).await,
            Err(DurableHoldError::Unavailable)
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
            while store2.deleted.lock().is_empty() {
                store2.deleted_notify.notified().await;
            }
        })
        .await
        .expect("uncertain error deletes committed hold");
        assert!(store2.committed.lock().is_empty());
    }

    #[test]
    fn resize_reservation_estimates_by_id_enforces_caps_atomically_and_preserves_concurrency() {
        let concurrency = Arc::new(KeyConcurrencyManager::new());
        let engine = LimitEngine::new(concurrency.clone(), Arc::new(cc_lb_clock::SystemClock));
        let view = PrincipalView::for_tests(
            "principal-resize",
            true,
            vec!["gpt-5".to_owned()],
            vec![
                Limit {
                    kind: LimitKind::Concurrent,
                    window_secs: 0,
                    cap_micros: 1,
                },
                Limit {
                    kind: LimitKind::Requests,
                    window_secs: 60,
                    cap_micros: 1,
                },
                Limit {
                    kind: LimitKind::TotalTokens,
                    window_secs: 60,
                    cap_micros: 1_000,
                },
                Limit {
                    kind: LimitKind::CostUsd,
                    window_secs: 60,
                    cap_micros: 500,
                },
            ],
            HashMap::new(),
        );
        let record = StoredApiKeyRecord {
            key_hash_b64: "resize-key".to_owned(),
            ..Default::default()
        };
        let baseline = engine
            .reserve(
                &view,
                &record,
                "principal-resize",
                "gpt-5",
                Some(100),
                200,
                Some(100),
            )
            .expect("baseline admission succeeds");
        let now = unix_secs(engine.inner.clock.now());
        assert_eq!(concurrency.current(&record.key_hash_b64), 1);
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::Requests, 60, now),
            1
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 60, now),
            300
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::CostUsd, 60, now),
            100
        );

        // Unsatisfiable CostUsd (> 500) must reject atomically without mutating TotalTokens.
        assert_eq!(
            engine.resize_reservation_estimates_by_id(
                &view,
                &record,
                "principal-resize",
                baseline.id(),
                "gpt-5",
                Some(100),
                800,
                Some(600),
            ),
            Err(RejectReason::Unsatisfiable {
                kind: LimitKind::CostUsd,
                cap: 500,
                requested: 600,
            })
        );
        // Rolling-window rejection: simulate existing rolling CostUsd usage pushing delta over cap
        // even when per-request CostUsd=450 is below 500.
        {
            let mut rolling = engine.inner.rolling.write();
            record_amount_locked(
                &mut rolling,
                &record.key_hash_b64,
                LimitKind::CostUsd,
                60,
                100,
                now,
            );
        }
        assert_eq!(
            engine.resize_reservation_estimates_by_id(
                &view,
                &record,
                "principal-resize",
                baseline.id(),
                "gpt-5",
                Some(100),
                800,
                Some(450),
            ),
            Err(RejectReason::CostRateLimit)
        );
        {
            let mut rolling = engine.inner.rolling.write();
            record_amount_locked(
                &mut rolling,
                &record.key_hash_b64,
                LimitKind::CostUsd,
                60,
                -100,
                now,
            );
        }
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 60, now),
            300,
            "failed resize must leave previously reserved TotalTokens unchanged"
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::CostUsd, 60, now),
            100,
            "failed resize must leave previously reserved CostUsd unchanged"
        );
        assert_eq!(concurrency.current(&record.key_hash_b64), 1);

        // Scaling up within caps succeeds even when Requests=1/1 and Concurrent=1/1 are full.
        assert_eq!(
            engine.resize_reservation_estimates_by_id(
                &view,
                &record,
                "principal-resize",
                baseline.id(),
                "gpt-5",
                Some(100),
                800,
                Some(400),
            ),
            Ok(true)
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 60, now),
            900
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::CostUsd, 60, now),
            400
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::Requests, 60, now),
            1
        );
        assert_eq!(concurrency.current(&record.key_hash_b64), 1);

        // Scaling back down refunds the token and cost delta.
        assert_eq!(
            engine.resize_reservation_estimates_by_id(
                &view,
                &record,
                "principal-resize",
                baseline.id(),
                "gpt-5",
                Some(100),
                250,
                Some(120),
            ),
            Ok(true)
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 60, now),
            350
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::CostUsd, 60, now),
            120
        );

        drop(baseline);
        assert_eq!(concurrency.current(&record.key_hash_b64), 0);
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 60, now),
            0
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::CostUsd, 60, now),
            0
        );
        assert_eq!(
            engine.resize_reservation_estimates_by_id(
                &view,
                &record,
                "principal-resize",
                "missing-id",
                "gpt-5",
                Some(100),
                200,
                Some(100),
            ),
            Ok(false)
        );
    }

    #[test]
    fn resize_reservation_after_bucket_expiry_checks_full_amount_and_never_credits_new_window() {
        let concurrency = Arc::new(KeyConcurrencyManager::new());
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
        let engine = LimitEngine::new(concurrency, clock.clone());
        let view = PrincipalView::for_tests(
            "principal-resize-expiry",
            true,
            vec!["gpt-5".to_owned()],
            vec![
                Limit {
                    kind: LimitKind::Requests,
                    window_secs: 60,
                    cap_micros: 10,
                },
                Limit {
                    kind: LimitKind::TotalTokens,
                    window_secs: 60,
                    cap_micros: 1_000,
                },
                Limit {
                    kind: LimitKind::CostUsd,
                    window_secs: 60,
                    cap_micros: 500,
                },
            ],
            HashMap::new(),
        );
        let record = StoredApiKeyRecord {
            key_hash_b64: "resize-expiry-key".to_owned(),
            ..Default::default()
        };

        // 1. Reserve baseline hold at t0 (300 TotalTokens = 200 in + 100 out, 100 CostUsd, 1 Request).
        let baseline = engine
            .reserve(
                &view,
                &record,
                "principal-resize-expiry",
                "gpt-5",
                Some(100),
                200,
                Some(100),
            )
            .expect("baseline admission succeeds");
        let t0 = unix_secs(clock.now());
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 60, t0),
            300
        );

        // 2. Advance past the 60s rolling bucket so baseline's original bucket expires.
        clock.advance_secs(60);
        let t1 = unix_secs(clock.now());
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 60, t1),
            0
        );

        // Another request consumes 800/1000 TotalTokens in the fresh window.
        let other = engine
            .reserve(
                &view,
                &record,
                "principal-resize-expiry",
                "gpt-5",
                Some(100),
                700,
                Some(50),
            )
            .expect("other request admission succeeds");
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 60, t1),
            800
        );

        // 3. Resizing UP from expired 300 to 400 (200+200) must check full 400 against 800+400 > 1000
        // rather than subtracting expired 300 (`delta=100` -> 900 <= 1000).
        assert_eq!(
            engine.resize_reservation_estimates_by_id(
                &view,
                &record,
                "principal-resize-expiry",
                baseline.id(),
                "gpt-5",
                Some(200),
                200,
                Some(100),
            ),
            Err(RejectReason::TokenRateLimit {
                kind: LimitKind::TotalTokens,
            })
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 60, t1),
            800,
            "rejected cross-window resize must not mutate live rolling totals"
        );

        // 4. Resizing DOWN from expired 300 to 100 (50+50) must re-admit full +100 into t1 (total 900),
        // rather than crediting -200 into the new window.
        assert_eq!(
            engine.resize_reservation_estimates_by_id(
                &view,
                &record,
                "principal-resize-expiry",
                baseline.id(),
                "gpt-5",
                Some(50),
                50,
                Some(40),
            ),
            Ok(true)
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 60, t1),
            900
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::CostUsd, 60, t1),
            90
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::Requests, 60, t1),
            2
        );

        // 5. In-window resize replaces the live t1 bucket cleanly without double counting.
        assert_eq!(
            engine.resize_reservation_estimates_by_id(
                &view,
                &record,
                "principal-resize-expiry",
                baseline.id(),
                "gpt-5",
                Some(25),
                25,
                Some(20),
            ),
            Ok(true)
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 60, t1),
            850
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::CostUsd, 60, t1),
            70
        );

        // 6. Dropping `other` in-window removes its live 800 hold leaving only baseline's 50;
        // then advancing past t1 and dropping `baseline` does not make the next window negative.
        drop(other);
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 60, t1),
            50
        );
        clock.advance_secs(60);
        let t2 = unix_secs(clock.now());
        drop(baseline);
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 60, t2),
            0
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::CostUsd, 60, t2),
            0
        );
    }

    #[tokio::test]
    async fn rebuild_local_usage_after_handoff_preserves_bucket_starts_for_drop_and_skips_expired()
    {
        let directory = tempfile::tempdir().expect("tempdir");
        let database_url = format!(
            "sqlite://{}",
            directory.path().join("handoff-buckets.sqlite").display()
        );
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
        let storage_impl = cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
            .await
            .expect("open sqlite");
        storage_impl.initialize().await.expect("migrate sqlite");
        let storage: Arc<dyn Storage> = Arc::new(storage_impl);
        let engine = LimitEngine::new(Arc::new(KeyConcurrencyManager::new()), clock.clone());
        let handle = engine
            .start_durable_usage_sync(storage.clone())
            .await
            .expect("start durable usage");
        let state = engine
            .inner
            .durable_usage
            .read()
            .clone()
            .expect("durable state");
        handle.shutdown().await;

        let view = PrincipalView::for_tests(
            "principal-handoff",
            true,
            vec!["claude-3-opus".to_owned()],
            vec![
                Limit {
                    kind: LimitKind::Requests,
                    window_secs: 60,
                    cap_micros: 10,
                },
                Limit {
                    kind: LimitKind::TotalTokens,
                    window_secs: 60,
                    cap_micros: 1_000,
                },
                Limit {
                    kind: LimitKind::TotalTokens,
                    window_secs: 3_600,
                    cap_micros: 10_000,
                },
            ],
            HashMap::new(),
        );
        let record = StoredApiKeyRecord {
            key_hash_b64: "handoff-bucket-key".to_owned(),
            ..StoredApiKeyRecord::default()
        };

        // 1. Reserve at t0 under 60s (1s buckets) and 3600s (60s buckets).
        let res1 = engine
            .reserve_async(
                &view,
                &record,
                "principal-handoff",
                "claude-3-opus",
                Some(100),
                200,
                None,
            )
            .await
            .expect("res1 succeeds");

        // Cross a 1s bucket boundary (t1 = t0 + 5), hand off writer, then drop res1:
        // rebuilt charge must stay at original bucket_start and disappear completely on drop.
        clock.advance_secs(5);
        let t1 = unix_secs(clock.now());
        rebuild_local_usage_after_handoff(&engine, &state, t1);
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 60, t1),
            300
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 3_600, t1),
            300
        );
        drop(res1);
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 60, t1),
            0,
            "dropping active reservation after 1s-bucket-crossing handoff must remove rebuilt 60s hold"
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 3_600, t1),
            0,
            "dropping active reservation after handoff must remove rebuilt 3600s hold"
        );

        // 2. Reserve at t1, advance 65s to t2 (expires 60s hold, crosses 60s bucket boundary inside
        // still-live 3600s window), hand off writer, and verify expired 60s hold is skipped while
        // live 3600s hold preserves its original bucket and drops cleanly.
        let res2 = engine
            .reserve_async(
                &view,
                &record,
                "principal-handoff",
                "claude-3-opus",
                Some(100),
                200,
                None,
            )
            .await
            .expect("res2 succeeds");
        clock.advance_secs(65);
        let t2 = unix_secs(clock.now());
        rebuild_local_usage_after_handoff(&engine, &state, t2);
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 60, t2),
            0,
            "expired 60s hold must not be resurrected into new window during handoff rebuild"
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 3_600, t2),
            300
        );
        drop(res2);
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 60, t2),
            0
        );
        assert_eq!(
            engine.current_total(&record.key_hash_b64, LimitKind::TotalTokens, 3_600, t2),
            0,
            "dropping active 3600s hold after 60s-bucket-crossing handoff must leave 0"
        );
    }
}
