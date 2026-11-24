use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use cc_lb_admin::ports::{
    SubscriptionQuotaIngestionPort, WarmupAttemptInput, WarmupAttemptOutcomeSnapshot,
    WarmupAttemptResult, WarmupPort,
};
use cc_lb_engine::Lifecycle;
use cc_lb_event_pipeline::warmup_attempts::{
    WarmupAttemptExecution, WarmupAttemptExecutionResult, execute_warmup_attempt,
};
use cc_lb_storage_api::WarmupAttemptTrigger;

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
        };
        let record = execute_warmup_attempt(WarmupAttemptExecution {
            storage: input.storage,
            upstream: input.upstream,
            lane: input.lane,
            scheduled_for_unix_secs: input.scheduled_for_unix_secs,
            trigger: WarmupAttemptTrigger::Manual,
            replica_id: None,
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

    fn ingest_subscription_quota_observations(
        &self,
        observations: &[cc_lb_quota::UnifiedQuotaObservation],
        upstream_id: uuid::Uuid,
        observed_at: SystemTime,
    ) {
        self.lifecycle.ingest_subscription_quota_observations(
            observations,
            upstream_id,
            observed_at,
        );
    }
}
