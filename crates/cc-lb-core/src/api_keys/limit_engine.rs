use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cc_lb_storage_api::Storage;
use cc_lb_storage_api::types::{KeyStatus as StoredKeyStatus, StoredApiKeyRecord};
use parking_lot::RwLock;
use serde::Serialize;

use crate::api_keys::concurrent_guard::{KeyConcurrencyGuard, KeyConcurrencyManager};
use crate::api_keys::principal_view::{PrincipalStatus, PrincipalView};
use crate::api_keys::types::{Limit, LimitKind};

impl Hash for LimitKind {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (*self as u8).hash(state);
    }
}

type RollingKey = (String, LimitKind, u64);

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
    effective_limits: RwLock<HashMap<(String, String), Vec<Limit>>>,
    concurrent_mgr: Arc<KeyConcurrencyManager>,
}

pub struct Reservation {
    engine: Weak<LimitEngineInner>,
    key_id: String,
    principal_id: String,
    effective_limits: Vec<Limit>,
    reserved: Vec<ReservedAmount>,
    concurrent_guards: Vec<KeyConcurrencyGuard>,
    refund_done: bool,
}

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

    pub fn record(&mut self, now_sec: u64, delta: i64) {
        self.buckets.push_back((now_sec, delta));
        self.evict_old(now_sec);
    }

    fn oldest_bucket_sec(&mut self, now_sec: u64) -> Option<u64> {
        self.evict_old(now_sec);
        self.buckets.front().map(|(bucket_sec, _)| *bucket_sec)
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
    pub fn new(
        concurrent_mgr: Arc<KeyConcurrencyManager>,
        _principal_view: Arc<arc_swap::ArcSwap<PrincipalView>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(LimitEngineInner {
                rolling: RwLock::new(HashMap::new()),
                effective_limits: RwLock::new(HashMap::new()),
                concurrent_mgr,
            }),
        })
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
        let now_sec = now_sec();
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
        let mut reserved = Vec::new();
        let mut concurrent_guards = Vec::new();

        for limit in &effective_limits {
            let window_sec = limit.window.as_secs();
            match limit.kind {
                LimitKind::Requests => {
                    let amount = 1;
                    if self.current_total(&key_id, limit.kind, window_sec, now_sec) + amount
                        > limit.cap_micros
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
                        > limit.cap_micros
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
                        > limit.cap_micros
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
                        > limit.cap_micros
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
                        > limit.cap_micros
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

        Ok(Reservation {
            engine: Arc::downgrade(&self.inner),
            key_id,
            principal_id: principal_id.to_owned(),
            effective_limits,
            reserved,
            concurrent_guards,
            refund_done: false,
        })
    }

    pub fn reconcile(
        &self,
        mut reservation: Reservation,
        actual_input: u64,
        actual_output: u64,
        actual_cost_micros: i64,
    ) {
        if let Some(inner) = reservation.engine.upgrade() {
            refund_difference(
                &inner,
                &reservation.key_id,
                &reservation.reserved,
                actual_input,
                actual_output,
                actual_cost_micros,
                now_sec(),
            );
        }
        reservation.refund_done = true;
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

        let now_sec = now_sec();
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

            let window_sec = limit.window.as_secs();
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
        let now_sec = now_sec();
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
            let window_sec = limit.window.as_secs();
            let mut total = 0_i64;
            let mut oldest: Option<u64> = None;
            for ((key_id, stored_principal_id), limits) in effective_limits {
                if stored_principal_id != principal_id
                    || !limits.iter().any(|candidate| {
                        candidate.kind == limit.kind && candidate.window == limit.window
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
            let window_sec = limit.window.as_secs();
            let (total, oldest) =
                self.current_total_and_oldest(key_id, limit.kind, window_sec, now_sec);
            push_limit_snapshot(&mut windows, limit, total, oldest, now_sec);
        }
        sort_windows(windows)
    }

    pub fn startup_replay(&self, storage: Arc<dyn Storage>) {
        let _ = storage;
        tracing::warn!(
            "limit engine startup replay is not implemented for trait storage; rolling counters start empty"
        );
    }

    fn current_total(&self, key_id: &str, kind: LimitKind, window_sec: u64, now_sec: u64) -> i64 {
        let mut rolling = self.inner.rolling.write();
        rolling
            .entry((key_id.to_owned(), kind, window_sec))
            .or_insert_with(|| RingCounter::new(window_sec))
            .current_total(now_sec)
    }

    fn current_total_and_oldest(
        &self,
        key_id: &str,
        kind: LimitKind,
        window_sec: u64,
        now_sec: u64,
    ) -> (i64, Option<u64>) {
        let mut rolling = self.inner.rolling.write();
        let counter = rolling
            .entry((key_id.to_owned(), kind, window_sec))
            .or_insert_with(|| RingCounter::new(window_sec));
        let total = counter.current_total(now_sec);
        let oldest = counter.oldest_bucket_sec(now_sec);
        (total, oldest)
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

impl Drop for Reservation {
    fn drop(&mut self) {
        let _ = (
            &self.principal_id,
            &self.effective_limits,
            &self.concurrent_guards,
        );
        if self.refund_done {
            return;
        }

        if let Some(engine) = self.engine.upgrade() {
            for amount in &self.reserved {
                engine.record_amount(
                    &self.key_id,
                    amount.kind,
                    amount.window_sec,
                    -amount.amount,
                    now_sec(),
                );
            }
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
    let window_sec = limit.window.as_secs();
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
            kind: convert_limit_kind(override_limit.kind),
            window: std::time::Duration::from_secs(override_limit.window_secs),
            cap_micros: override_limit.cap_micros,
        };

        if let Some(existing) = limits
            .iter_mut()
            .find(|existing| existing.kind == limit.kind && existing.window == limit.window)
        {
            *existing = limit;
        } else {
            limits.push(limit);
        }
    }

    limits
}

fn convert_limit_kind(kind: cc_lb_storage_api::types::LimitKind) -> LimitKind {
    match kind {
        cc_lb_storage_api::types::LimitKind::Requests => LimitKind::Requests,
        cc_lb_storage_api::types::LimitKind::InputTokens => LimitKind::InputTokens,
        cc_lb_storage_api::types::LimitKind::OutputTokens => LimitKind::OutputTokens,
        cc_lb_storage_api::types::LimitKind::TotalTokens => LimitKind::TotalTokens,
        cc_lb_storage_api::types::LimitKind::CostUsd => LimitKind::CostUsd,
        cc_lb_storage_api::types::LimitKind::Concurrent => LimitKind::Concurrent,
    }
}

fn key_id_for(record: &StoredApiKeyRecord) -> String {
    record.key_hash_b64.clone()
}

fn now_sec() -> u64 {
    // unix_epoch is a precondition; sub-epoch system clocks fall back to 0
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
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
