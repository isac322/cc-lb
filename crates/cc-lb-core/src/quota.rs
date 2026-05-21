use std::collections::HashMap;
use std::sync::{Arc, Once};

use cc_lb_plugin_api::PrincipalQuotas;
use cc_lb_storage_redb::{Storage, StorageError};
use dashmap::DashMap;
use metrics::Unit;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};

use crate::clock::{Clock, SystemClock};

pub use cc_lb_storage_redb::BucketKind;

static REGISTER_QUOTA_METRICS: Once = Once::new();

pub type QuotaConfig = PrincipalQuotas;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QuotaPolicy {
    pub window_secs: u64,
    pub capacity_requests: u64,
    pub capacity_input_tokens: u64,
    pub capacity_output_tokens: u64,
}

impl From<&PrincipalQuotas> for QuotaPolicy {
    fn from(value: &PrincipalQuotas) -> Self {
        Self {
            window_secs: value.window.as_secs().max(1),
            capacity_requests: value.requests_per_window,
            capacity_input_tokens: value.input_tokens_per_window,
            capacity_output_tokens: value.output_tokens_per_window,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QuotaDecision {
    Allow {
        remaining_in_window: u64,
        retry_after_secs: Option<u64>,
    },
    Reject {
        reason: String,
        retry_after_secs: u64,
        bucket: BucketKind,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Reservation {
    pub principal_id: String,
    pub window_start: u64,
    pub output_tokens_reserved: u64,
}

#[derive(Debug, Error)]
pub enum QuotaError {
    #[error("quota storage operation failed: {0}")]
    Storage(#[from] StorageError),
    #[error("quota storage task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("quota adjustment {delta} exceeds i64 range")]
    AdjustmentOutOfRange { delta: u64 },
}

pub struct QuotaManager {
    pub storage: Arc<Storage>,
    pub defaults: Arc<RwLock<QuotaPolicy>>,
    pub per_principal: Arc<RwLock<HashMap<String, QuotaPolicy>>>,
    pub per_principal_mutexes: Arc<DashMap<String, Arc<Mutex<()>>>>,
    reservations: Arc<Mutex<HashMap<(String, u64), u64>>>,
    clock: Arc<dyn Clock>,
}

impl QuotaManager {
    pub fn new(storage: Arc<Storage>, defaults: QuotaPolicy) -> Self {
        Self::with_clock(storage, defaults, Arc::new(SystemClock))
    }

    pub fn with_clock(storage: Arc<Storage>, defaults: QuotaPolicy, clock: Arc<dyn Clock>) -> Self {
        register_quota_metrics();
        Self {
            storage,
            defaults: Arc::new(RwLock::new(normalize_policy(defaults))),
            per_principal: Arc::new(RwLock::new(HashMap::new())),
            per_principal_mutexes: Arc::new(DashMap::new()),
            reservations: Arc::new(Mutex::new(HashMap::new())),
            clock,
        }
    }

    pub async fn set_principal_policy(&self, principal_id: impl Into<String>, policy: QuotaPolicy) {
        self.per_principal
            .write()
            .await
            .insert(principal_id.into(), normalize_policy(policy));
    }

    pub async fn set_defaults(&self, policy: QuotaPolicy) {
        *self.defaults.write().await = normalize_policy(policy);
    }

    pub async fn default_policy(&self) -> QuotaPolicy {
        *self.defaults.read().await
    }

    pub async fn try_consume_request(
        &self,
        principal_id: &str,
        _request_size_bytes: usize,
    ) -> QuotaDecision {
        self.consume_with_limit(principal_id, BucketKind::Requests, 1)
            .await
    }

    pub async fn reserve_output(
        &self,
        principal_id: &str,
        max_tokens_estimate: u64,
    ) -> Result<Reservation, QuotaDecision> {
        let policy = self.policy_for(principal_id).await;
        let window_start = window_start_for(self.clock.now_unix_secs(), policy.window_secs);
        match self
            .consume_with_policy(
                principal_id,
                BucketKind::OutputTokens,
                max_tokens_estimate,
                policy,
                window_start,
            )
            .await
        {
            QuotaDecision::Allow { .. } => {
                let reservation = Reservation {
                    principal_id: principal_id.to_owned(),
                    window_start,
                    output_tokens_reserved: max_tokens_estimate,
                };
                let key = (reservation.principal_id.clone(), reservation.window_start);
                let mut reservations = self.reservations.lock().await;
                let entry = reservations.entry(key).or_insert(0);
                *entry = entry.saturating_add(max_tokens_estimate);
                Ok(reservation)
            }
            reject @ QuotaDecision::Reject { .. } => Err(reject),
        }
    }

    pub async fn reconcile_output(&self, reservation: Reservation, actual_output_tokens: u64) {
        {
            let key = (reservation.principal_id.clone(), reservation.window_start);
            let mut reservations = self.reservations.lock().await;
            if let Some(reserved) = reservations.get_mut(&key) {
                *reserved = reserved.saturating_sub(reservation.output_tokens_reserved);
                if *reserved == 0 {
                    reservations.remove(&key);
                }
            }
        }

        if actual_output_tokens == reservation.output_tokens_reserved {
            return;
        }

        let result = if actual_output_tokens > reservation.output_tokens_reserved {
            self.adjust_bucket(
                &reservation.principal_id,
                reservation.window_start,
                BucketKind::OutputTokens,
                actual_output_tokens - reservation.output_tokens_reserved,
            )
            .await
        } else {
            self.adjust_bucket_down(
                &reservation.principal_id,
                reservation.window_start,
                BucketKind::OutputTokens,
                reservation.output_tokens_reserved - actual_output_tokens,
            )
            .await
        };

        if result.is_err() {
            emit_reject_metric(&reservation.principal_id, BucketKind::OutputTokens);
        }
    }

    pub async fn count_input(&self, principal_id: &str, n: u64) -> QuotaDecision {
        self.consume_with_limit(principal_id, BucketKind::InputTokens, n)
            .await
    }

    pub async fn count_output(&self, principal_id: &str, n: u64) -> QuotaDecision {
        self.consume_with_limit(principal_id, BucketKind::OutputTokens, n)
            .await
    }

    async fn consume_with_limit(
        &self,
        principal_id: &str,
        bucket: BucketKind,
        amount: u64,
    ) -> QuotaDecision {
        let policy = self.policy_for(principal_id).await;
        let window_start = window_start_for(self.clock.now_unix_secs(), policy.window_secs);
        self.consume_with_policy(principal_id, bucket, amount, policy, window_start)
            .await
    }

    async fn consume_with_policy(
        &self,
        principal_id: &str,
        bucket: BucketKind,
        amount: u64,
        policy: QuotaPolicy,
        window_start: u64,
    ) -> QuotaDecision {
        let capacity = capacity_for(policy, bucket);
        let now = self.clock.now_unix_secs();
        let retry_after_secs = retry_after_secs(now, window_start, policy.window_secs);
        let principal = principal_id.to_owned();
        let storage = Arc::clone(&self.storage);
        let mutex = self.mutex_for(principal_id);
        let _guard = mutex.lock().await;

        let result =
            tokio::task::spawn_blocking(move || -> Result<ConsumeAttempt, StorageError> {
                match storage.try_incr_quota(&principal, window_start, bucket, amount, capacity)? {
                    Some(new_value) => Ok(ConsumeAttempt::Allowed { new_value }),
                    None => Ok(ConsumeAttempt::Rejected),
                }
            })
            .await;

        match result {
            Ok(Ok(ConsumeAttempt::Allowed { new_value })) => QuotaDecision::Allow {
                remaining_in_window: capacity.saturating_sub(new_value),
                retry_after_secs: None,
            },
            Ok(Ok(ConsumeAttempt::Rejected)) => reject_decision(
                principal_id,
                bucket,
                quota_exhausted_reason(bucket).to_owned(),
                retry_after_secs,
            ),
            Ok(Err(_source)) => reject_decision(
                principal_id,
                bucket,
                "quota storage unavailable".to_owned(),
                retry_after_secs,
            ),
            Err(_source) => reject_decision(
                principal_id,
                bucket,
                "quota storage unavailable".to_owned(),
                retry_after_secs,
            ),
        }
    }

    async fn policy_for(&self, principal_id: &str) -> QuotaPolicy {
        let defaults = self.default_policy().await;
        self.per_principal
            .read()
            .await
            .get(principal_id)
            .copied()
            .unwrap_or(defaults)
    }

    fn mutex_for(&self, principal_id: &str) -> Arc<Mutex<()>> {
        self.per_principal_mutexes
            .entry(principal_id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn adjust_bucket(
        &self,
        principal_id: &str,
        window_start: u64,
        bucket: BucketKind,
        amount: u64,
    ) -> Result<u64, QuotaError> {
        let delta = i64::try_from(amount)
            .map_err(|_| QuotaError::AdjustmentOutOfRange { delta: amount })?;
        self.adjust_bucket_delta(principal_id, window_start, bucket, delta)
            .await
    }

    async fn adjust_bucket_down(
        &self,
        principal_id: &str,
        window_start: u64,
        bucket: BucketKind,
        amount: u64,
    ) -> Result<u64, QuotaError> {
        let delta = i64::try_from(amount)
            .map_err(|_| QuotaError::AdjustmentOutOfRange { delta: amount })?;
        self.adjust_bucket_delta(principal_id, window_start, bucket, -delta)
            .await
    }

    async fn adjust_bucket_delta(
        &self,
        principal_id: &str,
        window_start: u64,
        bucket: BucketKind,
        delta: i64,
    ) -> Result<u64, QuotaError> {
        let principal = principal_id.to_owned();
        let storage = Arc::clone(&self.storage);
        let mutex = self.mutex_for(principal_id);
        let _guard = mutex.lock().await;
        tokio::task::spawn_blocking(move || {
            storage.adjust_quota(&principal, window_start, bucket, delta)
        })
        .await?
        .map_err(QuotaError::from)
    }
}

enum ConsumeAttempt {
    Allowed { new_value: u64 },
    Rejected,
}

pub fn current_window_start(window_secs: u64) -> u64 {
    window_start_for(SystemClock.now_unix_secs(), window_secs.max(1))
}

fn window_start_for(now_unix: u64, window_secs: u64) -> u64 {
    let window_secs = window_secs.max(1);
    (now_unix / window_secs) * window_secs
}

fn retry_after_secs(now_unix: u64, window_start: u64, window_secs: u64) -> u64 {
    window_start
        .saturating_add(window_secs.max(1))
        .saturating_sub(now_unix)
        .max(1)
}

fn normalize_policy(policy: QuotaPolicy) -> QuotaPolicy {
    QuotaPolicy {
        window_secs: policy.window_secs.max(1),
        ..policy
    }
}

fn capacity_for(policy: QuotaPolicy, bucket: BucketKind) -> u64 {
    match bucket {
        BucketKind::Requests => policy.capacity_requests,
        BucketKind::InputTokens => policy.capacity_input_tokens,
        BucketKind::OutputTokens => policy.capacity_output_tokens,
    }
}

fn reject_decision(
    principal_id: &str,
    bucket: BucketKind,
    reason: String,
    retry_after_secs: u64,
) -> QuotaDecision {
    emit_reject_metric(principal_id, bucket);
    QuotaDecision::Reject {
        reason,
        retry_after_secs,
        bucket,
    }
}

fn emit_reject_metric(principal_id: &str, bucket: BucketKind) {
    metrics::counter!(
        "cc_lb_quota_rejected_total",
        "principal" => principal_id.to_owned(),
        "kind" => bucket_label(bucket)
    )
    .increment(1);
}

fn bucket_label(bucket: BucketKind) -> &'static str {
    match bucket {
        BucketKind::Requests => "requests",
        BucketKind::InputTokens => "input_tokens",
        BucketKind::OutputTokens => "output_tokens",
    }
}

fn quota_exhausted_reason(bucket: BucketKind) -> &'static str {
    match bucket {
        BucketKind::Requests => "requests quota exhausted",
        BucketKind::InputTokens => "input_tokens quota exhausted",
        BucketKind::OutputTokens => "output_tokens quota exhausted",
    }
}

fn register_quota_metrics() {
    REGISTER_QUOTA_METRICS.call_once(|| {
        metrics::describe_counter!(
            "cc_lb_quota_rejected_total",
            Unit::Count,
            "Quota rejections by principal and quota kind."
        );
    });
}
