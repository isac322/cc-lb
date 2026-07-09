use cc_lb_engine::UnifiedQuotaObservation;
use cc_lb_server::warmup::execute::{
    WarmupAttemptExecution, WarmupAttemptExecutionResult, execute_warmup_attempt,
};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    BackendKind, MetaStore, SubscriptionQuotaSample, SubscriptionQuotaSampleKind,
    SubscriptionQuotaSource, SubscriptionQuotaStatus, SubscriptionQuotaWindow, UpstreamCreate,
    UpstreamStore, UpstreamSubscriptionQuotaStore, UpstreamWarmupAttemptStore,
    WarmupAttemptListFilters, WarmupAttemptOutcome, WarmupAttemptStatus, WarmupAttemptTrigger,
    WarmupDispatchKind, WarmupPermanentFailureReason, WarmupSkipReason, WarmupSuccessReason,
    WarmupTransientFailureReason,
};
use http::StatusCode;
use uuid::Uuid;

#[tokio::test]
async fn warmup_attempt_executor_persists_one_row_for_each_outcome() {
    let storage = cc_lb_storage_sqlite::open_sqlite(
        "sqlite::memory:",
        std::sync::Arc::new(cc_lb_engine::SystemClock),
    )
    .await
    .expect("sqlite opens");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("sqlite initializes");
    let upstream = storage
        .create(UpstreamCreate {
            name: "warmup-attempts-persist".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: true,
            warmup_dialect_plugin: None,
        })
        .await
        .expect("upstream creates");
    storage
        .record_subscription_quota_sample(&previous_quota(upstream.id))
        .await
        .expect("previous quota inserts");

    let fresh_observations = [five_hour_observation(1_800_004_000)];
    let redundant_observations = [five_hour_observation(1_800_001_000)];
    let cases = [
        ExpectedAttempt {
            outcome: WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced),
            expected_idle_secs: Some(2_000),
            dispatch_kind: WarmupDispatchKind::Http,
            result: WarmupAttemptExecutionResult::Response {
                status: StatusCode::OK,
                observations: &fresh_observations,
                error_detail: None,
            },
        },
        ExpectedAttempt {
            outcome: WarmupAttemptOutcome::Success(WarmupSuccessReason::WindowAlreadyActive),
            expected_idle_secs: None,
            dispatch_kind: WarmupDispatchKind::Http,
            result: WarmupAttemptExecutionResult::Response {
                status: StatusCode::OK,
                observations: &redundant_observations,
                error_detail: None,
            },
        },
        ExpectedAttempt {
            outcome: WarmupAttemptOutcome::TransientFailure(
                WarmupTransientFailureReason::NetworkError,
            ),
            expected_idle_secs: None,
            dispatch_kind: WarmupDispatchKind::Http,
            result: WarmupAttemptExecutionResult::TransientFailure {
                reason: WarmupTransientFailureReason::NetworkError,
                http_status: None,
                error_detail: Some("dial tcp failed"),
            },
        },
        ExpectedAttempt {
            outcome: WarmupAttemptOutcome::PermanentFailure(
                WarmupPermanentFailureReason::Forbidden,
            ),
            expected_idle_secs: None,
            dispatch_kind: WarmupDispatchKind::Http,
            result: WarmupAttemptExecutionResult::PermanentFailure {
                reason: WarmupPermanentFailureReason::Forbidden,
                http_status: Some(StatusCode::FORBIDDEN),
                error_detail: None,
            },
        },
        ExpectedAttempt {
            outcome: WarmupAttemptOutcome::Skipped(WarmupSkipReason::UpstreamDisabled),
            expected_idle_secs: None,
            dispatch_kind: WarmupDispatchKind::NotDispatched,
            result: WarmupAttemptExecutionResult::Skipped {
                reason: WarmupSkipReason::UpstreamDisabled,
                cycle_key: None,
                error_detail: None,
            },
        },
    ];

    for (idx, case) in cases.iter().enumerate() {
        let attempted_at_unix_secs = 1_800_002_000 + i64::try_from(idx).expect("idx fits");
        let record = execute_warmup_attempt(WarmupAttemptExecution {
            storage: &storage,
            upstream: &upstream,
            scheduled_for_unix_secs: 1_800_001_000,
            trigger: WarmupAttemptTrigger::Scheduled,
            replica_id: None,
            lease_holder: Some("test-scheduler"),
            expected_cycle_key: Some(1_800_002_000),
            attempted_at_unix_secs,
            completed_at_unix_secs: Some(attempted_at_unix_secs + 1),
            dispatch_kind: case.dispatch_kind,
            result: case.result,
        })
        .await;
        assert_eq!(record.outcome, case.outcome);
        assert_eq!(record.dispatch_kind, Some(case.dispatch_kind));
        assert_eq!(
            record.idle_secs_since_prev_window, case.expected_idle_secs,
            "idle_secs mismatch for outcome={:?}: expected {:?}, got {:?}",
            case.outcome, case.expected_idle_secs, record.idle_secs_since_prev_window
        );

        let attempts = storage
            .list_warmup_attempts_for_upstream(
                upstream.id,
                WarmupAttemptListFilters {
                    limit: Some(10),
                    before: None,
                    status: None,
                },
            )
            .await
            .expect("attempts list");
        assert_eq!(attempts.len(), idx + 1);
        assert_eq!(attempts[0].id, record.id);
    }

    let attempts = storage
        .list_warmup_attempts_for_upstream(
            upstream.id,
            WarmupAttemptListFilters {
                limit: Some(10),
                before: None,
                status: None,
            },
        )
        .await
        .expect("attempts list");
    let mut statuses = attempts
        .iter()
        .map(|attempt| attempt.outcome.status())
        .collect::<Vec<_>>();
    statuses.sort_by_key(status_order);
    assert_eq!(
        statuses,
        [
            WarmupAttemptStatus::Success,
            WarmupAttemptStatus::Success,
            WarmupAttemptStatus::TransientFailure,
            WarmupAttemptStatus::PermanentFailure,
            WarmupAttemptStatus::Skipped,
        ]
    );
}

#[derive(Clone, Copy)]
struct ExpectedAttempt<'a> {
    outcome: WarmupAttemptOutcome,
    expected_idle_secs: Option<i64>,
    dispatch_kind: WarmupDispatchKind,
    result: WarmupAttemptExecutionResult<'a>,
}

fn five_hour_observation(resets_at_unix_secs: u64) -> UnifiedQuotaObservation {
    UnifiedQuotaObservation {
        window: SubscriptionQuotaWindow::FiveHour,
        resets_at_unix_secs: Some(resets_at_unix_secs),
        ..UnifiedQuotaObservation::default()
    }
}

fn previous_quota(upstream_id: Uuid) -> SubscriptionQuotaSample {
    SubscriptionQuotaSample {
        upstream_id,
        window: SubscriptionQuotaWindow::FiveHour,
        source: SubscriptionQuotaSource::Header,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis: 1_800_000_000_000,
        sample_id: Uuid::from_u128(1),
        utilization: Some(0.1),
        status: Some(SubscriptionQuotaStatus::Allowed),
        resets_at_unix_secs: Some(1_800_000_000),
        surpassed_threshold: None,
        representative_claim: None,
        fallback_percentage: None,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
        disabled_reason: None,
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        extra_usage_used_credits: None,
        ingested_at_unix_millis: 1_800_000_000_001,
    }
}

fn status_order(status: &WarmupAttemptStatus) -> u8 {
    match status {
        WarmupAttemptStatus::Success => 0,
        WarmupAttemptStatus::Skipped => 3,
        WarmupAttemptStatus::TransientFailure => 1,
        WarmupAttemptStatus::PermanentFailure => 2,
    }
}
