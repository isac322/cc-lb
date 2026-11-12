//! `dispatch_warmup` lane gates: credential profile and absent quota group.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cc_lb_aead::{AeadService, EncryptedOAuthTokens, GeminiOAuthProfile, OAuthTokenBundle};
use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::{AdaptiveJob, Filter, TaskStatus};
use cc_lb_storage_api::upstream::{UpstreamCreate, UpstreamKind};
use cc_lb_storage_api::{
    OAuthTokensCreate, SubscriptionQuotaSample, SubscriptionQuotaSampleKind,
    SubscriptionQuotaSource, SubscriptionQuotaWindow, UpstreamStore, UpstreamWarmupAttemptStore,
    WarmupAttemptListFilters, WarmupAttemptOutcome, WarmupDispatchKind, WarmupLane,
    WarmupSkipReason,
};
use uuid::Uuid;

use super::support::{FailingPusher, Fixture};

const FIVE_HOURS_SECS: u64 = 5 * 60 * 60;

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs()
}

async fn create_gemini_upstream(fixture: &Fixture, profile: GeminiOAuthProfile) -> Uuid {
    // Same master key as the fixture's dispatch AEAD.
    let aead = AeadService::from_master_key([9; 32]);
    let id = Uuid::new_v4();
    let bundle = OAuthTokenBundle {
        access_token: "warmup-lane-access-token".to_owned(),
        refresh_token: "warmup-lane-refresh-token".to_owned(),
        expires_at_unix_secs: now_unix_secs() + 3_600,
        refresh_token_expires_at_unix_secs: None,
        account_id: Some("warmup-lane-account".to_owned()),
        project_id: Some("warmup-lane-project".to_owned()),
        oauth_profile: profile,
        scopes: Vec::new(),
        never_refresh: false,
    };
    UpstreamStore::create(
        fixture.storage.as_ref(),
        UpstreamCreate {
            id,
            name: format!("warmup-lane-{id}"),
            kind: UpstreamKind::GeminiOauth,
            base_url: Some(url::Url::parse("http://127.0.0.1:9/").expect("base URL parses")),
            api_key_ciphertext: None,
            oauth_tokens: Some(OAuthTokensCreate {
                tokens: EncryptedOAuthTokens::encrypt(&aead, &bundle, id.as_bytes())
                    .expect("encrypt credentials"),
                never_refresh: false,
            }),
            warmup_enabled: true,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("create gemini upstream");
    id
}

async fn pending_warmup_jobs(fixture: &Fixture) -> Vec<(UpstreamWarmupJob, Option<String>)> {
    fixture
        .backend
        .backend
        .list_adaptive_tasks(&Filter {
            status: Some(TaskStatus::Pending),
            page: 1,
            page_size: Some(20),
        })
        .await
        .expect("list adaptive tasks")
        .into_iter()
        .filter_map(|task| match task.args {
            AdaptiveJob::Warmup(job) => Some((job, task.idempotency_key)),
            _ => None,
        })
        .collect()
}

fn absent_sample(
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    now_unix_millis: u64,
) -> SubscriptionQuotaSample {
    SubscriptionQuotaSample {
        upstream_id,
        window,
        source: SubscriptionQuotaSource::Api,
        sample_kind: SubscriptionQuotaSampleKind::Absent,
        observed_at_unix_millis: now_unix_millis,
        sample_id: Uuid::new_v4(),
        utilization: None,
        status: None,
        resets_at_unix_secs: None,
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
        credits: None,
        ingested_at_unix_millis: now_unix_millis,
    }
}

#[tokio::test]
async fn third_party_lane_on_gemini_cli_credential_skips_without_rescheduling() {
    let fixture = Fixture::new().await;
    let upstream_id = create_gemini_upstream(&fixture, GeminiOAuthProfile::GeminiCli).await;
    let dispatch = fixture.dispatch(Arc::new(FailingPusher));
    let cycle_key = now_unix_secs();

    let outcome = dispatch
        .dispatch_warmup(UpstreamWarmupJob::new_for_lane(
            upstream_id,
            cycle_key,
            WarmupLane::ThirdParty,
        ))
        .await
        .expect("third-party warmup dispatch");

    assert_eq!(outcome, JobOutcome::Skip);
    let attempts = fixture
        .storage
        .list_warmup_attempts_for_upstream(upstream_id, WarmupAttemptListFilters::default())
        .await
        .expect("list warmup attempts");
    assert_eq!(attempts.len(), 1, "{attempts:?}");
    assert_eq!(attempts[0].lane, WarmupLane::ThirdParty);
    assert_eq!(
        attempts[0].outcome,
        WarmupAttemptOutcome::Skipped(WarmupSkipReason::QuotaGroupUnavailable)
    );
    assert_eq!(
        attempts[0].dispatch_kind,
        Some(WarmupDispatchKind::NotDispatched)
    );
    assert!(
        pending_warmup_jobs(&fixture).await.is_empty(),
        "unsupported profile must not reschedule the lane"
    );
}

#[tokio::test]
async fn third_party_lane_with_absent_targets_skips_and_rechecks_in_five_hours() {
    let fixture = Fixture::new().await;
    let upstream_id = create_gemini_upstream(&fixture, GeminiOAuthProfile::Antigravity).await;
    let dispatch = fixture.dispatch(Arc::new(FailingPusher));
    let before = now_unix_secs();
    for window in [
        SubscriptionQuotaWindow::ThirdPartyFiveHour,
        SubscriptionQuotaWindow::ThirdPartySevenDay,
    ] {
        dispatch.subscription_quota_cache.upsert_observation(
            upstream_id,
            &absent_sample(upstream_id, window, before * 1_000),
        );
    }

    let outcome = dispatch
        .dispatch_warmup(UpstreamWarmupJob::new_for_lane(
            upstream_id,
            before,
            WarmupLane::ThirdParty,
        ))
        .await
        .expect("third-party warmup dispatch");
    let after = now_unix_secs();

    assert_eq!(outcome, JobOutcome::Skip);
    let attempts = fixture
        .storage
        .list_warmup_attempts_for_upstream(upstream_id, WarmupAttemptListFilters::default())
        .await
        .expect("list warmup attempts");
    assert_eq!(attempts.len(), 1, "{attempts:?}");
    assert_eq!(attempts[0].lane, WarmupLane::ThirdParty);
    assert_eq!(
        attempts[0].outcome,
        WarmupAttemptOutcome::Skipped(WarmupSkipReason::QuotaGroupUnavailable)
    );
    assert_eq!(
        attempts[0].dispatch_kind,
        Some(WarmupDispatchKind::NotDispatched)
    );

    let jobs = pending_warmup_jobs(&fixture).await;
    assert_eq!(jobs.len(), 1, "{jobs:?}");
    let (job, idempotency_key) = &jobs[0];
    assert_eq!(job.upstream_id, upstream_id);
    assert_eq!(job.lane, WarmupLane::ThirdParty);
    assert!(
        (before + FIVE_HOURS_SECS..=after + FIVE_HOURS_SECS).contains(&job.cycle_key),
        "cycle_key={} before={before} after={after}",
        job.cycle_key
    );
    assert_eq!(
        idempotency_key.as_deref(),
        Some(
            format!(
                "adaptive:warmup_third_party:{upstream_id}:{}",
                job.cycle_key
            )
            .as_str()
        )
    );
}
