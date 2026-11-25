use std::time::SystemTime;

use async_trait::async_trait;
use axum::http::{HeaderMap, StatusCode};
use cc_lb_quota::UnifiedQuotaObservation;
use cc_lb_storage_api::{
    Storage, UpstreamRecord, WarmupAttemptOutcome, WarmupDispatchKind, WarmupLane,
    WarmupPermanentFailureReason, WarmupSkipReason, WarmupTransientFailureReason,
};
use uuid::Uuid;

#[derive(Clone, Copy)]
pub struct WarmupAttemptInput<'a> {
    pub storage: &'a dyn Storage,
    pub upstream: &'a UpstreamRecord,
    pub lane: WarmupLane,
    pub scheduled_for_unix_secs: i64,
    pub lease_holder: Option<&'a str>,
    pub expected_cycle_key: Option<i64>,
    pub attempted_at_unix_secs: i64,
    pub completed_at_unix_secs: Option<i64>,
    pub dispatch_kind: WarmupDispatchKind,
    pub result: WarmupAttemptResult<'a>,
}

#[derive(Clone, Copy, Debug)]
pub enum WarmupAttemptResult<'a> {
    Response {
        status: StatusCode,
        observations: &'a [UnifiedQuotaObservation],
        error_detail: Option<&'a str>,
    },
    TransientFailure {
        reason: WarmupTransientFailureReason,
        http_status: Option<StatusCode>,
        error_detail: Option<&'a str>,
    },
    PermanentFailure {
        reason: WarmupPermanentFailureReason,
        http_status: Option<StatusCode>,
        error_detail: Option<&'a str>,
    },
    Skipped {
        reason: WarmupSkipReason,
        cycle_key: Option<i64>,
        error_detail: Option<&'a str>,
    },
}

#[derive(Debug)]
pub struct WarmupAttemptOutcomeSnapshot {
    pub outcome: WarmupAttemptOutcome,
    pub cycle_key: Option<i64>,
    pub error_detail: Option<String>,
}

#[async_trait]
pub trait WarmupPort: Send + Sync {
    async fn record_attempt(&self, input: WarmupAttemptInput<'_>) -> WarmupAttemptOutcomeSnapshot;
}

pub trait SubscriptionQuotaIngestionPort: Send + Sync {
    fn ingest_subscription_quota_headers(
        &self,
        headers: &HeaderMap,
        upstream_id: Uuid,
        observed_at: SystemTime,
    );

    fn ingest_subscription_quota_observations(
        &self,
        observations: &[UnifiedQuotaObservation],
        upstream_id: Uuid,
        observed_at: SystemTime,
    );
}

#[derive(Debug, Clone)]
pub struct SplitManualWarmupTicket {
    pub upstream_id: Uuid,
    pub lane: WarmupLane,
    pub nonce: Uuid,
    pub idempotency_key: String,
    pub expires_at_unix_secs: u64,
    pub deadline: tokio::time::Instant,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SplitManualWarmupError {
    #[error("split manual warmup unavailable: {0}")]
    Unavailable(String),
    #[error("split manual warmup expired before starting: {0}")]
    ExpiredBeforeStart(String),
    #[error("split manual warmup outcome unknown: {0}")]
    OutcomeUnknown(String),
}

#[async_trait]
pub trait SplitManualWarmupPort: Send + Sync {
    async fn enqueue_manual_warmup(
        &self,
        upstream_id: Uuid,
        lane: WarmupLane,
        holder: String,
        candidate_cycle_key: i64,
    ) -> Result<SplitManualWarmupTicket, SplitManualWarmupError>;

    async fn wait_manual_warmup(
        &self,
        ticket: SplitManualWarmupTicket,
    ) -> Result<cc_lb_control::ManualWarmupJobResult, SplitManualWarmupError>;

    async fn fire_manual_warmup(
        &self,
        upstream_id: Uuid,
        lane: WarmupLane,
        holder: String,
        candidate_cycle_key: i64,
    ) -> Result<cc_lb_control::ManualWarmupJobResult, SplitManualWarmupError> {
        let ticket = self
            .enqueue_manual_warmup(upstream_id, lane, holder, candidate_cycle_key)
            .await?;
        self.wait_manual_warmup(ticket).await
    }
}
