use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
#[cfg(feature = "postgres")]
use cc_lb_admin::ports::{RetainedPartialPort, RetainedPartialSnapshot};
use cc_lb_admin::ports::{
    RoutePreviewError, RoutePreviewInput, RoutePreviewOutcome, RoutePreviewPort,
    RoutePreviewWinner, SubscriptionQuotaIngestionPort, WarmupAttemptInput,
    WarmupAttemptOutcomeSnapshot, WarmupAttemptResult, WarmupPort,
};
use cc_lb_engine::Lifecycle;
#[cfg(feature = "postgres")]
use cc_lb_engine::PartialRetentionCache;
use cc_lb_engine::lifecycle::{PreviewRouteError, PreviewRouteInput as EnginePreviewRouteInput};
use cc_lb_engine::warmup_attempts::{
    WarmupAttemptExecution, WarmupAttemptExecutionResult, execute_warmup_attempt,
};

pub(crate) struct ServerRoutePreviewPort {
    lifecycle: Arc<Lifecycle>,
}

impl ServerRoutePreviewPort {
    pub(crate) const fn new(lifecycle: Arc<Lifecycle>) -> Self {
        Self { lifecycle }
    }
}

impl RoutePreviewPort for ServerRoutePreviewPort {
    fn preview_route(
        &self,
        input: RoutePreviewInput,
    ) -> Result<RoutePreviewOutcome, RoutePreviewError> {
        let outcome = self
            .lifecycle
            .preview_route(EnginePreviewRouteInput {
                principal_id: input.principal_id,
                request_id: input.request_id,
                headers: input.headers,
                body_bytes: input.body_bytes,
            })
            .map_err(map_preview_error)?;
        let winner = match (outcome.winner_upstream_id, outcome.winner_upstream_name) {
            (Some(upstream_id), Some(name)) => Some(RoutePreviewWinner { upstream_id, name }),
            (None, None) | (Some(_), None) | (None, Some(_)) => None,
        };
        Ok(RoutePreviewOutcome {
            trace: outcome.trace,
            winner,
        })
    }
}

fn map_preview_error(error: PreviewRouteError) -> RoutePreviewError {
    match error {
        PreviewRouteError::PrincipalNotFound(id) => RoutePreviewError::PrincipalNotFound(id),
        PreviewRouteError::PipelineInstantiationError(detail) => {
            RoutePreviewError::PipelineInstantiationError(detail)
        }
    }
}

pub(crate) struct ServerWarmupPort;

#[async_trait]
impl WarmupPort for ServerWarmupPort {
    async fn record_attempt(&self, input: WarmupAttemptInput<'_>) -> WarmupAttemptOutcomeSnapshot {
        let result = match input.result {
            WarmupAttemptResult::Response {
                status,
                observations,
                error_detail,
            } => WarmupAttemptExecutionResult::Response {
                status,
                observations,
                error_detail,
            },
            WarmupAttemptResult::TransientFailure {
                reason,
                http_status,
                error_detail,
            } => WarmupAttemptExecutionResult::TransientFailure {
                reason,
                http_status,
                error_detail,
            },
            WarmupAttemptResult::PermanentFailure {
                reason,
                http_status,
                error_detail,
            } => WarmupAttemptExecutionResult::PermanentFailure {
                reason,
                http_status,
                error_detail,
            },
            WarmupAttemptResult::Skipped {
                reason,
                cycle_key,
                error_detail,
            } => WarmupAttemptExecutionResult::Skipped {
                reason,
                cycle_key,
                error_detail,
            },
            WarmupAttemptResult::PreflightActiveWindow { cycle_key } => {
                WarmupAttemptExecutionResult::PreflightActiveWindow { cycle_key }
            }
        };
        let record = execute_warmup_attempt(WarmupAttemptExecution {
            storage: input.storage,
            upstream: input.upstream,
            scheduled_for_unix_secs: input.scheduled_for_unix_secs,
            trigger: input.trigger,
            replica_id: input.replica_id,
            lease_holder: input.lease_holder,
            expected_cycle_key: input.expected_cycle_key,
            attempted_at_unix_secs: input.attempted_at_unix_secs,
            completed_at_unix_secs: input.completed_at_unix_secs,
            dispatch_kind: input.dispatch_kind,
            result,
        })
        .await;
        WarmupAttemptOutcomeSnapshot {
            outcome: record.outcome,
            cycle_key: record.cycle_key,
            error_detail: record.error_detail,
        }
    }
}

pub(crate) struct ServerSubscriptionQuotaIngestionPort {
    lifecycle: Arc<Lifecycle>,
}

impl ServerSubscriptionQuotaIngestionPort {
    pub(crate) const fn new(lifecycle: Arc<Lifecycle>) -> Self {
        Self { lifecycle }
    }
}

impl SubscriptionQuotaIngestionPort for ServerSubscriptionQuotaIngestionPort {
    fn ingest_subscription_quota_headers(
        &self,
        headers: &axum::http::HeaderMap,
        upstream_id: uuid::Uuid,
        observed_at: SystemTime,
    ) {
        self.lifecycle
            .ingest_subscription_quota_headers(headers, upstream_id, observed_at);
    }
}

#[cfg(feature = "postgres")]
pub(crate) struct ServerRetainedPartialPort {
    cache: PartialRetentionCache,
}

#[cfg(feature = "postgres")]
impl ServerRetainedPartialPort {
    pub(crate) const fn new(cache: PartialRetentionCache) -> Self {
        Self { cache }
    }
}

#[cfg(feature = "postgres")]
impl RetainedPartialPort for ServerRetainedPartialPort {
    fn retained_partial(&self, event_id: &str) -> Option<RetainedPartialSnapshot> {
        self.cache
            .get(event_id)
            .map(|payload| RetainedPartialSnapshot { payload })
    }
}
