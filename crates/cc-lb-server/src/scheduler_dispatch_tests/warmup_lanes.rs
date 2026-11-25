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
            vertex: None,
            vertex_credential_type: None,
            vertex_quota_project_id: None,
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
    let view = cc_lb_control::DynamicViewBuilder::from_view(&fixture.dynamic_view().load())
        .subscription_quota_routing_max_staleness_secs(1_800)
        .build();
    fixture.dynamic_view().store(view);
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

#[tokio::test]
async fn worker_role_warmup_preflight_hydrates_from_store_when_cache_is_empty() {
    use cc_lb_storage_api::UpstreamSubscriptionQuotaStore as _;

    let fixture = Fixture::new().await;
    let view = cc_lb_control::DynamicViewBuilder::from_view(&fixture.dynamic_view().load())
        .subscription_quota_routing_max_staleness_secs(1_800)
        .build();
    fixture.dynamic_view().store(view);
    let upstream_id = create_gemini_upstream(&fixture, GeminiOAuthProfile::GeminiCli).await;
    let dispatch = fixture.dispatch(Arc::new(FailingPusher));
    let before = now_unix_secs();
    let before_millis = before * 1_000;
    let resets_at = before + FIVE_HOURS_SECS;

    // Leave in-memory subscription_quota_cache empty (simulating --role worker
    // between reconcile ticks) while durable storage already has an active 5h
    // window sample written by a proxy pod.
    fixture
        .storage
        .record_subscription_quota_samples(&[SubscriptionQuotaSample {
            upstream_id,
            window: SubscriptionQuotaWindow::GeminiFiveHour,
            source: SubscriptionQuotaSource::Api,
            sample_kind: SubscriptionQuotaSampleKind::Sample,
            observed_at_unix_millis: before_millis,
            sample_id: Uuid::new_v4(),
            utilization: Some(0.25),
            status: None,
            resets_at_unix_secs: Some(resets_at),
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
            ingested_at_unix_millis: before_millis,
        }])
        .await
        .expect("record durable 5h quota sample");

    let outcome = dispatch
        .dispatch_warmup(UpstreamWarmupJob::new_for_lane(
            upstream_id,
            before,
            WarmupLane::Primary,
        ))
        .await
        .expect("warmup preflight hydrates from store without firing HTTP");

    assert_eq!(outcome, JobOutcome::Done);
    let attempts = fixture
        .storage
        .list_warmup_attempts_for_upstream(upstream_id, WarmupAttemptListFilters::default())
        .await
        .expect("list warmup attempts");
    assert_eq!(attempts.len(), 1, "{attempts:?}");
    assert_eq!(
        attempts[0].dispatch_kind,
        Some(WarmupDispatchKind::NotDispatched)
    );
    let jobs = pending_warmup_jobs(&fixture).await;
    assert_eq!(jobs.len(), 1, "{jobs:?}");
    assert_eq!(jobs[0].0.cycle_key, resets_at);
}

#[tokio::test]
async fn warmup_preflight_derives_secs_and_millis_from_same_post_hydration_clock() {
    use async_trait::async_trait;
    use cc_lb_clock::TestClock;
    use cc_lb_storage_api::{
        StorageResult, SubscriptionQuotaPollFence, UpstreamSubscriptionQuotaStore,
    };

    struct ClockAdvancingQuotaStore {
        inner: Arc<dyn UpstreamSubscriptionQuotaStore>,
        clock: Arc<TestClock>,
        advance_on_read_secs: u64,
    }

    #[async_trait]
    impl UpstreamSubscriptionQuotaStore for ClockAdvancingQuotaStore {
        async fn record_subscription_quota_samples(
            &self,
            records: &[SubscriptionQuotaSample],
        ) -> StorageResult<()> {
            self.inner.record_subscription_quota_samples(records).await
        }

        async fn record_subscription_quota_poll(
            &self,
            records: &[SubscriptionQuotaSample],
            fence: &SubscriptionQuotaPollFence,
        ) -> StorageResult<bool> {
            self.inner
                .record_subscription_quota_poll(records, fence)
                .await
        }

        async fn record_subscription_quota_fenced_headers(
            &self,
            records: &[SubscriptionQuotaSample],
            fence: &SubscriptionQuotaPollFence,
        ) -> StorageResult<bool> {
            self.inner
                .record_subscription_quota_fenced_headers(records, fence)
                .await
        }

        async fn list_latest_subscription_quota_for_upstreams(
            &self,
            upstream_ids: &[Uuid],
        ) -> StorageResult<Vec<SubscriptionQuotaSample>> {
            let rows = self
                .inner
                .list_latest_subscription_quota_for_upstreams(upstream_ids)
                .await?;
            self.clock.advance_secs(self.advance_on_read_secs);
            Ok(rows)
        }
    }

    fn make_stores(
        storage: &Arc<cc_lb_storage_sqlite::SqliteStorage>,
        quota_store: Arc<dyn UpstreamSubscriptionQuotaStore>,
    ) -> Arc<crate::dynamic_view_builder::Stores> {
        let storage_dyn: Arc<dyn cc_lb_storage_api::Storage> = storage.clone();
        Arc::new(crate::dynamic_view_builder::Stores {
            upstreams: storage_dyn.clone(),
            vertex_state: Arc::new(crate::dynamic_view_builder::VertexState::new(
                storage_dyn.clone(),
            )),
            principals: storage_dyn.clone(),
            plugin_registry: storage_dyn.clone(),
            upstream_rate_limits: storage_dyn.clone(),
            upstream_subscription_quotas: quota_store,
            upstream_subscription_metadata: storage_dyn.clone(),
            organization_metadata: storage_dyn.clone(),
            plan_tiers: storage_dyn.clone(),
            prompt_cache_observations: storage_dyn.clone(),
            anthropic_compatibility_kv: storage_dyn.clone(),
            audit: Some(storage_dyn),
        })
    }

    // Case 1: Clock advances by 10s during DB hydration while DB returns a sample observed at
    // `t0 + 10`. Both post-hydration secs and millis must read `t0 + 10` so the sample is not
    // misclassified as future-dated.
    let fixture = Fixture::new().await;
    let view = cc_lb_control::DynamicViewBuilder::from_view(&fixture.dynamic_view().load())
        .subscription_quota_routing_max_staleness_secs(1_800)
        .build();
    fixture.dynamic_view().store(view);
    let t0 = now_unix_secs();
    let clock = Arc::new(TestClock::new_at_secs(t0));
    let upstream_active = create_gemini_upstream(&fixture, GeminiOAuthProfile::GeminiCli).await;
    let resets_at_active = t0 + FIVE_HOURS_SECS;
    UpstreamSubscriptionQuotaStore::record_subscription_quota_samples(
        fixture.storage.as_ref(),
        &[SubscriptionQuotaSample {
            upstream_id: upstream_active,
            window: SubscriptionQuotaWindow::GeminiFiveHour,
            source: SubscriptionQuotaSource::Api,
            sample_kind: SubscriptionQuotaSampleKind::Sample,
            observed_at_unix_millis: (t0 + 10) * 1_000,
            sample_id: Uuid::new_v4(),
            utilization: Some(0.25),
            status: None,
            resets_at_unix_secs: Some(resets_at_active),
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
            ingested_at_unix_millis: (t0 + 10) * 1_000,
        }],
    )
    .await
    .expect("record post-hydration sample");

    let mut dispatch = fixture.dispatch(Arc::new(FailingPusher));
    dispatch.clock = clock.clone();
    dispatch.stores = make_stores(
        &fixture.storage,
        Arc::new(ClockAdvancingQuotaStore {
            inner: fixture.storage.clone(),
            clock: clock.clone(),
            advance_on_read_secs: 10,
        }),
    );

    let outcome = dispatch
        .dispatch_warmup(UpstreamWarmupJob::new_for_lane(
            upstream_active,
            t0,
            WarmupLane::Primary,
        ))
        .await
        .expect("post-hydration clock must classify sample as active without firing warmup");
    assert_eq!(outcome, JobOutcome::Done);
    let attempts = fixture
        .storage
        .list_warmup_attempts_for_upstream(upstream_active, WarmupAttemptListFilters::default())
        .await
        .expect("list warmup attempts");
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        attempts[0].dispatch_kind,
        Some(WarmupDispatchKind::NotDispatched)
    );

    // Case 2: Reset boundary crossed during DB hydration (`t1 -> t1 + 10 >= reset`). Post-hydration
    // clock must treat the old window as expired and proceed to fire the warmup request.
    let fixture_reset = Fixture::new().await;
    let view = cc_lb_control::DynamicViewBuilder::from_view(&fixture_reset.dynamic_view().load())
        .subscription_quota_routing_max_staleness_secs(1_800)
        .build();
    fixture_reset.dynamic_view().store(view);
    let t1 = now_unix_secs();
    let clock_reset = Arc::new(TestClock::new_at_secs(t1));
    let upstream_reset =
        create_gemini_upstream(&fixture_reset, GeminiOAuthProfile::GeminiCli).await;
    UpstreamSubscriptionQuotaStore::record_subscription_quota_samples(
        fixture_reset.storage.as_ref(),
        &[SubscriptionQuotaSample {
            upstream_id: upstream_reset,
            window: SubscriptionQuotaWindow::GeminiFiveHour,
            source: SubscriptionQuotaSource::Api,
            sample_kind: SubscriptionQuotaSampleKind::Sample,
            observed_at_unix_millis: (t1 - 5) * 1_000,
            sample_id: Uuid::new_v4(),
            utilization: Some(0.25),
            status: None,
            resets_at_unix_secs: Some(t1 + 5),
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
            ingested_at_unix_millis: (t1 - 5) * 1_000,
        }],
    )
    .await
    .expect("record expiring sample");

    let mut dispatch_reset = fixture_reset.dispatch(Arc::new(FailingPusher));
    dispatch_reset.clock = clock_reset.clone();
    dispatch_reset.stores = make_stores(
        &fixture_reset.storage,
        Arc::new(ClockAdvancingQuotaStore {
            inner: fixture_reset.storage.clone(),
            clock: clock_reset,
            advance_on_read_secs: 10,
        }),
    );

    let _ = dispatch_reset
        .dispatch_warmup(UpstreamWarmupJob::new_for_lane(
            upstream_reset,
            t1,
            WarmupLane::Primary,
        ))
        .await;
    let attempts_reset = fixture_reset
        .storage
        .list_warmup_attempts_for_upstream(upstream_reset, WarmupAttemptListFilters::default())
        .await
        .expect("list reset warmup attempts");
    assert_eq!(attempts_reset.len(), 1);
    assert_eq!(
        attempts_reset[0].dispatch_kind,
        Some(WarmupDispatchKind::Http),
        "crossed reset during hydration must dispatch warmup instead of skipping: {attempts_reset:?}"
    );
}

#[tokio::test]
async fn dispatch_manual_warmup_records_manual_attempt_and_meta_result() {
    use cc_lb_scheduler::jobs::warmup::ManualWarmupJob;

    let fixture = Fixture::new().await;
    let upstream_id = create_gemini_upstream(&fixture, GeminiOAuthProfile::GeminiCli).await;
    let dispatch = fixture.dispatch(Arc::new(FailingPusher));
    let nonce = Uuid::new_v4();

    // Disable warmup to exercise the deterministic manual skip path + meta result persistence.
    let current = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
        .await
        .expect("get upstream")
        .expect("upstream exists");
    UpstreamStore::update(
        fixture.storage.as_ref(),
        upstream_id,
        current.revision,
        cc_lb_storage_api::upstream::UpstreamUpdate {
            warmup_enabled: Some(false),
            ..Default::default()
        },
    )
    .await
    .expect("disable warmup");

    let outcome = dispatch
        .dispatch_entity(AdaptiveJob::ManualWarmup(ManualWarmupJob::new(
            upstream_id,
            WarmupLane::Primary,
            "fire-now:test-manual".to_owned(),
            1_700_000_000,
            nonce,
        )))
        .await
        .expect("dispatch_manual_warmup");
    assert_eq!(outcome, JobOutcome::Done);

    let attempts = fixture
        .storage
        .list_warmup_attempts_for_upstream(upstream_id, WarmupAttemptListFilters::default())
        .await
        .expect("list attempts");
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        attempts[0].outcome,
        WarmupAttemptOutcome::Skipped(WarmupSkipReason::UpstreamDisabled)
    );

    let recorded = cc_lb_control::load_manual_warmup_result(
        fixture.storage.as_ref(),
        upstream_id,
        WarmupLane::Primary,
        nonce,
    )
    .await
    .expect("load result")
    .expect("manual warmup result persisted")
    .into_settled()
    .expect("settled manual warmup result");
    assert_eq!(recorded.nonce, nonce);
    assert_eq!(
        recorded.outcome,
        WarmupAttemptOutcome::Skipped(WarmupSkipReason::UpstreamDisabled)
    );
}

#[tokio::test]
async fn dispatch_manual_warmup_skips_expired_unstarted_job_before_dispatch() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::routing::post;
    use cc_lb_scheduler::jobs::warmup::ManualWarmupJob;

    let hit_count = Arc::new(AtomicUsize::new(0));
    let hit_count_handler = Arc::clone(&hit_count);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock origin");
    let addr = listener.local_addr().expect("mock addr");
    let server = tokio::spawn(async move {
        let router = axum::Router::new().route(
            "/v1/messages",
            post(move || {
                let hit_count = Arc::clone(&hit_count_handler);
                async move {
                    hit_count.fetch_add(1, Ordering::SeqCst);
                    axum::http::StatusCode::OK
                }
            }),
        );
        axum::serve(listener, router).await.ok();
    });

    let fixture = Fixture::new().await;
    let aead = AeadService::from_master_key([9; 32]);
    let upstream_id = Uuid::new_v4();
    let bundle = OAuthTokenBundle {
        access_token: "manual-warmup-expired-token".to_owned(),
        refresh_token: "manual-warmup-expired-refresh".to_owned(),
        expires_at_unix_secs: now_unix_secs() + 3_600,
        refresh_token_expires_at_unix_secs: None,
        account_id: Some("acct".to_owned()),
        project_id: None,
        oauth_profile: GeminiOAuthProfile::default(),
        scopes: Vec::new(),
        never_refresh: false,
    };
    UpstreamStore::create(
        fixture.storage.as_ref(),
        UpstreamCreate {
            vertex: None,
            vertex_credential_type: None,
            vertex_quota_project_id: None,
            id: upstream_id,
            name: "manual-expired-anthropic".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: Some(url::Url::parse(&format!("http://{addr}/")).expect("mock URL")),
            api_key_ciphertext: None,
            oauth_tokens: Some(OAuthTokensCreate {
                tokens: EncryptedOAuthTokens::encrypt(&aead, &bundle, upstream_id.as_bytes())
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
    .expect("create anthropic upstream");

    let dispatch = fixture.dispatch(Arc::new(FailingPusher));
    let nonce = Uuid::new_v4();
    let expired_at = now_unix_secs().saturating_sub(1);
    let outcome = dispatch
        .dispatch_entity(AdaptiveJob::ManualWarmup(
            ManualWarmupJob::new(
                upstream_id,
                WarmupLane::Primary,
                "fire-now:expired".to_owned(),
                1_700_000_000,
                nonce,
            )
            .with_expires_at(expired_at),
        ))
        .await
        .expect("dispatch expired manual warmup");

    server.abort();
    assert_eq!(outcome, JobOutcome::Skip);
    assert_eq!(
        hit_count.load(Ordering::SeqCst),
        0,
        "expired unstarted manual warmup must not dispatch upstream HTTP requests"
    );
    let attempts = fixture
        .storage
        .list_warmup_attempts_for_upstream(upstream_id, WarmupAttemptListFilters::default())
        .await
        .expect("list attempts");
    assert!(
        attempts.is_empty(),
        "expired unstarted manual warmup must not record an execution attempt: {attempts:?}"
    );
    let bridge_result = cc_lb_control::load_manual_warmup_result(
        fixture.storage.as_ref(),
        upstream_id,
        WarmupLane::Primary,
        nonce,
    )
    .await
    .expect("load result")
    .expect("expired unstarted manual warmup must record ExpiredBeforeStart bridge result");
    assert!(
        matches!(
            bridge_result,
            cc_lb_control::ManualWarmupBridgeResult::ExpiredBeforeStart {
                nonce: recorded_nonce,
                ..
            } if recorded_nonce == nonce
        ),
        "unexpected bridge result: {bridge_result:?}"
    );
}

async fn run_manual_warmup_429_quota_exhausted_case(close_quota_sink: bool) -> usize {
    use axum::http::{HeaderMap as AxumHeaderMap, HeaderValue, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::post;
    use cc_lb_scheduler::jobs::warmup::ManualWarmupJob;

    let reset_ts = (now_unix_secs() + 100_000).to_string();
    let reset_header = reset_ts.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock 429 origin");
    let addr = listener.local_addr().expect("mock addr");
    let server = tokio::spawn(async move {
        let router = axum::Router::new().route(
            "/v1/messages",
            post(move || {
                let reset_header = reset_header.clone();
                async move {
                    let mut headers = AxumHeaderMap::new();
                    headers.insert(
                        "anthropic-ratelimit-unified-7d-utilization",
                        HeaderValue::from_static("1.00"),
                    );
                    headers.insert(
                        "anthropic-ratelimit-unified-7d-status",
                        HeaderValue::from_static("rejected"),
                    );
                    headers.insert(
                        "anthropic-ratelimit-unified-7d-reset",
                        HeaderValue::from_str(&reset_header).expect("reset header"),
                    );
                    headers.insert(
                        "anthropic-ratelimit-unified-representative-claim",
                        HeaderValue::from_static("seven_day"),
                    );
                    (
                        StatusCode::TOO_MANY_REQUESTS,
                        headers,
                        r#"{"error":{"type":"rate_limit_error","message":"weekly limit exhausted"}}"#,
                    )
                        .into_response()
                }
            }),
        );
        axum::serve(listener, router).await.ok();
    });

    let fixture = Fixture::new().await;
    let aead = AeadService::from_master_key([9; 32]);
    let upstream_id = Uuid::new_v4();
    let bundle = OAuthTokenBundle {
        access_token: "manual-warmup-429-token".to_owned(),
        refresh_token: "manual-warmup-429-refresh".to_owned(),
        expires_at_unix_secs: now_unix_secs() + 3_600,
        refresh_token_expires_at_unix_secs: None,
        account_id: Some("acct".to_owned()),
        project_id: None,
        oauth_profile: GeminiOAuthProfile::default(),
        scopes: Vec::new(),
        never_refresh: false,
    };
    UpstreamStore::create(
        fixture.storage.as_ref(),
        UpstreamCreate {
            vertex: None,
            vertex_credential_type: None,
            vertex_quota_project_id: None,
            id: upstream_id,
            name: "manual-429-anthropic".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: Some(url::Url::parse(&format!("http://{addr}/")).expect("mock URL")),
            api_key_ciphertext: None,
            oauth_tokens: Some(OAuthTokensCreate {
                tokens: EncryptedOAuthTokens::encrypt(&aead, &bundle, upstream_id.as_bytes())
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
    .expect("create anthropic upstream");

    let mut dispatch = fixture.dispatch(Arc::new(FailingPusher));
    let (subscription_quota_sink, quota_rx) = cc_lb_engine::SubscriptionQuotaSink::new();
    let mut quota_rx = if close_quota_sink {
        drop(quota_rx);
        None
    } else {
        Some(quota_rx)
    };
    dispatch.subscription_quota_sink = subscription_quota_sink;
    let nonce = Uuid::new_v4();
    let outcome = dispatch
        .dispatch_entity(AdaptiveJob::ManualWarmup(ManualWarmupJob::new(
            upstream_id,
            WarmupLane::Primary,
            "fire-now:429-exhausted".to_owned(),
            1_700_000_000,
            nonce,
        )))
        .await
        .expect("dispatch 429 manual warmup");

    server.abort();
    assert_eq!(outcome, JobOutcome::Done);

    let attempts = fixture
        .storage
        .list_warmup_attempts_for_upstream(upstream_id, WarmupAttemptListFilters::default())
        .await
        .expect("list attempts");
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        attempts[0].outcome,
        WarmupAttemptOutcome::Skipped(WarmupSkipReason::SevenDayQuotaExhausted)
    );

    let manual_result = cc_lb_control::take_manual_warmup_result(
        fixture.storage.as_ref(),
        upstream_id,
        WarmupLane::Primary,
        nonce,
    )
    .await
    .expect("read manual warmup result")
    .expect("manual warmup result must be persisted even when quota sink fails")
    .into_settled()
    .expect("settled manual warmup result");
    assert_eq!(
        manual_result.outcome,
        WarmupAttemptOutcome::Skipped(WarmupSkipReason::SevenDayQuotaExhausted)
    );

    let snapshots = dispatch.subscription_quota_cache.snapshot_for_upstream(
        upstream_id,
        now_unix_secs() * 1_000,
        1_800,
    );
    let seven_day = snapshots
        .iter()
        .find(|snap| snap.window == SubscriptionQuotaWindow::SevenDay.as_str())
        .expect("429 manual warmup must publish 7d quota observation");
    assert_eq!(
        seven_day.status.as_deref(),
        Some(cc_lb_storage_api::SubscriptionQuotaStatus::Rejected.as_str())
    );
    assert_eq!(seven_day.utilization, Some(1.0));

    let mut queued = Vec::new();
    if let Some(ref mut rx) = quota_rx {
        while let Ok(sample) = rx.try_recv() {
            queued.push(sample);
        }
    }
    queued
        .into_iter()
        .filter(|s| s.upstream_id == upstream_id && s.window == SubscriptionQuotaWindow::SevenDay)
        .count()
}

#[tokio::test]
async fn dispatch_manual_warmup_records_observations_on_429_quota_exhausted() {
    let matching_samples = run_manual_warmup_429_quota_exhausted_case(false).await;
    assert_eq!(
        matching_samples, 1,
        "429 manual warmup must enqueue 7d quota sample to sink"
    );
}

#[tokio::test]
async fn dispatch_manual_warmup_preserves_result_and_cache_when_quota_sink_is_closed() {
    let matching_samples = run_manual_warmup_429_quota_exhausted_case(true).await;
    assert_eq!(matching_samples, 0);
}

#[tokio::test]
async fn apalis_housekeeping_prunes_expired_split_bridge_results_when_request_retention_disabled() {
    use cc_lb_storage_api::MetaStore as _;

    let fixture = Fixture::new().await;
    let cfg = cc_lb_config::Config {
        request_event_retention_days: 0,
        ..cc_lb_config::Config::default()
    };
    let pusher = Arc::new(FailingPusher);
    let dispatch = fixture.dispatch_with_config(pusher, cfg);

    let now = now_unix_secs();
    let early_upstream = Uuid::parse_str("00000000-0000-4000-8000-000000000001").expect("uuid");
    let late_upstream = Uuid::parse_str("ffffffff-ffff-4000-8000-000000000001").expect("uuid");

    let mut live_keys = Vec::new();
    for idx in 0..270_u128 {
        let nonce = Uuid::from_u128(0x0000_0000_0000_4000_8000_0000_0000_0001 + idx);
        let key = cc_lb_control::manual_warmup_result_meta_key(
            early_upstream,
            WarmupLane::Primary,
            nonce,
        );
        let payload = serde_json::to_string(&cc_lb_control::ManualWarmupBridgeResult::Settled(
            cc_lb_control::ManualWarmupJobResult {
                nonce,
                outcome: WarmupAttemptOutcome::Success(
                    cc_lb_storage_api::WarmupSuccessReason::CycleAdvanced,
                ),
                cycle_key: Some(1),
                error_detail: None,
                expires_at_unix_secs: now.saturating_add(600),
            },
        ))
        .expect("serialize live");
        fixture
            .storage
            .put_meta_value(&key, &payload)
            .await
            .expect("seed live");
        live_keys.push(key);
    }

    let expired_nonce = Uuid::parse_str("ffffffff-ffff-4000-8000-000000000099").expect("uuid");
    let expired_key = cc_lb_control::manual_warmup_result_meta_key(
        late_upstream,
        WarmupLane::Primary,
        expired_nonce,
    );
    let expired_payload = serde_json::to_string(&cc_lb_control::ManualWarmupBridgeResult::Settled(
        cc_lb_control::ManualWarmupJobResult {
            nonce: expired_nonce,
            outcome: WarmupAttemptOutcome::Success(
                cc_lb_storage_api::WarmupSuccessReason::CycleAdvanced,
            ),
            cycle_key: Some(2),
            error_detail: None,
            expires_at_unix_secs: now.saturating_sub(60),
        },
    ))
    .expect("serialize expired");
    fixture
        .storage
        .put_meta_value(&expired_key, &expired_payload)
        .await
        .expect("seed expired");

    let outcome = dispatch
        .dispatch_singleton(cc_lb_scheduler::worker::CronJob::ApalisHousekeeping(
            Default::default(),
        ))
        .await
        .expect("dispatch apalis housekeeping");
    assert_eq!(outcome, JobOutcome::Done);

    assert_eq!(
        fixture
            .storage
            .get_meta_value(&expired_key)
            .await
            .expect("get expired"),
        None,
        "expired bridge result past first page must be pruned even when request_event_retention_days = 0"
    );
    assert!(
        fixture
            .storage
            .get_meta_value(&live_keys[0])
            .await
            .expect("get live")
            .is_some(),
        "live bridge result must remain"
    );
}

#[tokio::test]
async fn apalis_housekeeping_reaps_stale_running_jobs_before_bounded_bridge_prune_and_enqueues_continuation()
 {
    use cc_lb_clock::TestClock;
    use cc_lb_scheduler::jobs::apalis_housekeeping::ApalisHousekeepingJob;
    use cc_lb_scheduler::worker::{CronJob, SchedulerBackend};
    use cc_lb_storage_api::MetaStore as _;

    let fixture = Fixture::new().await;
    let scheduler_pool = match &fixture.backend.backend {
        SchedulerBackend::Sqlite(backend) => backend.pool(),
        #[cfg(feature = "storage-postgres")]
        SchedulerBackend::Postgres(_) => panic!("fixture must open SQLite scheduler storage"),
    };

    let now = 1_800_000_000_u64;
    let stale_lock_at = i64::try_from(now - 300).expect("lock_at fits i64");
    let now_i64 = i64::try_from(now).expect("now fits i64");

    sqlx::query(
        "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen, started_at)
         VALUES ('dead-worker', 'cron', 'default', '', ?1, ?1)",
    )
    .bind(now_i64)
    .execute(scheduler_pool)
    .await
    .expect("seed worker row for lock_by foreign key");

    sqlx::query(
        "INSERT INTO Jobs (job, id, job_type, status, run_at, lock_by, lock_at, attempts, last_result)
         VALUES (?1, 'stale-running-1', 'cron', 'Running', ?2, 'dead-worker', ?3, 0, NULL)",
    )
    .bind(vec![0_u8])
    .bind(now_i64)
    .bind(stale_lock_at)
    .execute(scheduler_pool)
    .await
    .expect("seed first stale Running job");

    let upstream_id = Uuid::parse_str("00000000-0000-4000-8000-000000000001").expect("uuid");
    let mut expired_keys = Vec::with_capacity(70);
    for idx in 0..70_u128 {
        let nonce = Uuid::from_u128(0x0000_0000_0000_4000_8000_0000_0000_0001 + idx);
        let key =
            cc_lb_control::manual_warmup_result_meta_key(upstream_id, WarmupLane::Primary, nonce);
        let payload = serde_json::to_string(&cc_lb_control::ManualWarmupBridgeResult::Settled(
            cc_lb_control::ManualWarmupJobResult {
                nonce,
                outcome: WarmupAttemptOutcome::Success(
                    cc_lb_storage_api::WarmupSuccessReason::CycleAdvanced,
                ),
                cycle_key: Some(1),
                error_detail: None,
                expires_at_unix_secs: now - 60,
            },
        ))
        .expect("serialize expired");
        fixture
            .storage
            .put_meta_value(&key, &payload)
            .await
            .expect("seed expired bridge entry");
        expired_keys.push(key);
    }

    let mut dispatch = fixture.dispatch_with_config(
        Arc::new(FailingPusher),
        cc_lb_config::Config {
            request_event_retention_days: 0,
            ..cc_lb_config::Config::default()
        },
    );
    dispatch.clock = Arc::new(TestClock::new_at_secs(now));

    let outcome = dispatch
        .dispatch_singleton(CronJob::ApalisHousekeeping(ApalisHousekeepingJob::default()))
        .await
        .expect("dispatch regular housekeeping");
    assert_eq!(outcome, JobOutcome::Done);

    // (a) Ordinary stale Running recovery ran BEFORE the bounded bridge sweep.
    let (status_1, attempts_1): (String, i64) =
        sqlx::query_as("SELECT status, attempts FROM Jobs WHERE id = 'stale-running-1'")
            .fetch_one(scheduler_pool)
            .await
            .expect("read reaped job");
    assert_eq!(status_1, "Pending");
    assert_eq!(attempts_1, 1);

    // (b) Bounded sweep deleted exactly 64 expired keys and left 6 remaining.
    let mut remaining_after_first = 0_usize;
    for key in &expired_keys {
        if fixture
            .storage
            .get_meta_value(key)
            .await
            .expect("check expired key")
            .is_some()
        {
            remaining_after_first += 1;
        }
    }
    assert_eq!(remaining_after_first, 6);

    // (c) Continuation cron job with split_bridge_prune_cursor was enqueued.
    let continuations: Vec<(Vec<u8>, String)> = sqlx::query_as(
        "SELECT job, idempotency_key FROM Jobs
         WHERE job_type = 'cron'
           AND status = 'Pending'
           AND idempotency_key LIKE 'split_bridge_prune_continuation:%'",
    )
    .fetch_all(scheduler_pool)
    .await
    .expect("query continuation cron jobs");
    assert_eq!(
        continuations.len(),
        1,
        "expected one continuation cron task"
    );
    let continuation_job: CronJob =
        serde_json::from_slice(&continuations[0].0).expect("decode continuation CronJob");
    match &continuation_job {
        CronJob::ApalisHousekeeping(job) => {
            assert!(
                job.split_bridge_prune_cursor.is_some(),
                "continuation job must carry split_bridge_prune_cursor"
            );
        }
        other => panic!("unexpected continuation job: {other:?}"),
    }

    // Seed a second stale Running job and delete the queued continuation row so we can
    // assert no second continuation is enqueued and ordinary recovery does not run.
    sqlx::query(
        "DELETE FROM Jobs WHERE job_type = 'cron' AND idempotency_key LIKE 'split_bridge_prune_continuation:%'",
    )
    .execute(scheduler_pool)
    .await
    .expect("clear pending continuation row before direct dispatch");
    sqlx::query(
        "INSERT INTO Jobs (job, id, job_type, status, run_at, lock_by, lock_at, attempts, last_result)
         VALUES (?1, 'stale-running-2', 'cron', 'Running', ?2, 'dead-worker', ?3, 0, NULL)",
    )
    .bind(vec![0_u8])
    .bind(now_i64)
    .bind(stale_lock_at)
    .execute(scheduler_pool)
    .await
    .expect("seed second stale Running job");

    let cont_outcome = dispatch
        .dispatch_singleton(continuation_job)
        .await
        .expect("dispatch split-bridge continuation");
    assert_eq!(cont_outcome, JobOutcome::Done);

    // (d) All 6 remaining expired keys were deleted.
    for key in &expired_keys {
        assert_eq!(
            fixture
                .storage
                .get_meta_value(key)
                .await
                .expect("check expired key after continuation"),
            None
        );
    }

    // (e) No further continuation was enqueued.
    let remaining_continuations: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM Jobs WHERE job_type = 'cron' AND idempotency_key LIKE 'split_bridge_prune_continuation:%'",
    )
    .fetch_one(scheduler_pool)
    .await
    .expect("count remaining continuation rows");
    assert_eq!(remaining_continuations, 0);

    // (f) Continuation pass did NOT run ordinary stale-lock recovery.
    let (status_2, attempts_2): (String, i64) =
        sqlx::query_as("SELECT status, attempts FROM Jobs WHERE id = 'stale-running-2'")
            .fetch_one(scheduler_pool)
            .await
            .expect("read untouched second stale job");
    assert_eq!(status_2, "Running");
    assert_eq!(attempts_2, 0);
}
