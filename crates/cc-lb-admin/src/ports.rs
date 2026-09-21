use std::time::SystemTime;

use async_trait::async_trait;
use axum::http::{HeaderMap, StatusCode};
use bytes::Bytes;
use cc_lb_domain::RoutingTrace;
use cc_lb_quota::UnifiedQuotaObservation;
use cc_lb_storage_api::{
    Storage, UpstreamRecord, WarmupAttemptOutcome, WarmupAttemptTrigger, WarmupDispatchKind,
    WarmupPermanentFailureReason, WarmupSkipReason, WarmupTransientFailureReason,
};
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug)]
pub struct RoutePreviewInput {
    pub principal_id: String,
    pub request_id: Option<String>,
    pub headers: HeaderMap,
    pub body_bytes: Bytes,
}

#[derive(Debug, Serialize)]
pub struct RoutePreviewWinner {
    pub upstream_id: Uuid,
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct RoutePreviewOutcome {
    pub trace: RoutingTrace,
    pub winner: Option<RoutePreviewWinner>,
}

#[derive(Debug)]
pub enum RoutePreviewError {
    PrincipalNotFound(String),
    PipelineInstantiationError(String),
}

#[async_trait]
pub trait RoutePreviewPort: Send + Sync {
    async fn preview_route(
        &self,
        input: RoutePreviewInput,
    ) -> Result<RoutePreviewOutcome, RoutePreviewError>;
}

#[derive(Clone, Copy)]
pub struct WarmupAttemptInput<'a> {
    pub storage: &'a dyn Storage,
    pub upstream: &'a UpstreamRecord,
    pub scheduled_for_unix_secs: i64,
    pub trigger: WarmupAttemptTrigger,
    pub replica_id: Option<Uuid>,
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
    PreflightActiveWindow {
        cycle_key: i64,
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
}

#[derive(Debug)]
pub struct RetainedPartialSnapshot {
    pub payload: Vec<u8>,
}

pub trait RetainedPartialPort: Send + Sync {
    fn retained_partial(&self, event_id: &str) -> Option<RetainedPartialSnapshot>;
}
