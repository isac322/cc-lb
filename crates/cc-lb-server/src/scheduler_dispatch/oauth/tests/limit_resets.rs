use super::*;
use cc_lb_admin::{LimitResetUsageRefresh, LimitResetUsageRefreshPort};
use cc_lb_control::anthropic_metadata::cedar_ember_epoch_meta_key;
use cc_lb_control::openai_limit_resets::{
    OpenAiLimitResetPollRecord, openai_limit_reset_epoch_fence, openai_limit_reset_epoch_meta_key,
    openai_limit_reset_meta_key,
};
use cc_lb_domain::{Principal, PrincipalKind, UpstreamCandidate};
use cc_lb_engine::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
use cc_lb_routing::{FilterPlugin, RoutingContext};
use cc_lb_storage_api::UpstreamSubscriptionQuotaStore;
use serde_json::{Value, json};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicU16},
};

struct ProviderState {
    usage: Mutex<Value>,
    usage_status: AtomicU16,
    inventory_status: AtomicU16,
    delay_next_usage: AtomicBool,
    delay_reset_usage: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    usage_calls: AtomicUsize,
    inventory_calls: AtomicUsize,
    reset_percent: AtomicUsize,
    reset_usage_status: AtomicU16,
    consume_calls: AtomicUsize,
}

struct Provider {
    state: Arc<ProviderState>,
    base: Url,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Provider {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Provider {
    async fn new(kind: UpstreamKind, percent: u64) -> Self {
        let state = Arc::new(ProviderState {
            usage: Mutex::new(usage(kind, percent)),
            usage_status: AtomicU16::new(200),
            inventory_status: AtomicU16::new(200),
            delay_next_usage: AtomicBool::new(false),
            delay_reset_usage: AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            usage_calls: AtomicUsize::new(0),
            inventory_calls: AtomicUsize::new(0),
            reset_percent: AtomicUsize::new(12),
            reset_usage_status: AtomicU16::new(200),
            consume_calls: AtomicUsize::new(0),
        });
        let provider = state.clone();
        let app = axum::Router::new().fallback(move |request: axum::extract::Request| {
            let state = provider.clone();
            async move {
                let inventory = request.uri().path().ends_with("/rate-limit-reset-credits");
                let status;
                if request.uri().path().ends_with("/rate-limit-reset-credits/consume") {
                    assert_eq!(request.method(), http::Method::POST);
                    let bytes = axum::body::to_bytes(request.into_body(), 4096).await.unwrap();
                    let payload: Value = serde_json::from_slice(&bytes).unwrap();
                    assert_eq!(payload["credit_id"], "opaque-credit");
                    assert!(Uuid::parse_str(payload["redeem_request_id"].as_str().unwrap()).is_ok());
                    state.consume_calls.fetch_add(1, Ordering::SeqCst);
                    if state.delay_reset_usage.load(Ordering::SeqCst) {
                        state.delay_next_usage.store(true, Ordering::SeqCst);
                    }
                    *state.usage.lock().unwrap() = usage(kind, state.reset_percent.load(Ordering::SeqCst) as u64);
                    state.usage_status.store(state.reset_usage_status.load(Ordering::SeqCst), Ordering::SeqCst);
                    return (http::StatusCode::OK, axum::Json(json!({"code": "reset", "windows_reset": 1})));
                }
                let body;
                if inventory {
                    state.inventory_calls.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(request.headers()["chatgpt-account-id"], "synthetic-workspace");
                    assert_eq!(request.headers()["originator"], "codex_cli_rs");
                    assert_eq!(request.headers()["user-agent"], "codex-cli");
                    assert_eq!(request.headers()["authorization"], "Bearer synthetic-display-token");
                    status = state.inventory_status.load(Ordering::SeqCst);
                    body = json!({"credits": [{
                        "id": "opaque-credit", "reset_type": "codex_rate_limits", "status": "available",
                        "is_supported_by_plan": true, "granted_at": "2026-11-01T00:00:00Z"
                    }], "available_count": 2});
                } else {
                    state.usage_calls.fetch_add(1, Ordering::SeqCst);
                    status = state.usage_status.load(Ordering::SeqCst);
                    body = state.usage.lock().expect("usage body").clone();
                    if state.delay_next_usage.swap(false, Ordering::SeqCst) {
                        state.entered.notify_one();
                        state.release.notified().await;
                    }
                }
                (http::StatusCode::from_u16(status).expect("HTTP status"), axum::Json(body))
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind provider");
        let base = Url::parse(&format!(
            "http://{}/backend-api/codex",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let server =
            tokio::spawn(async move { axum::serve(listener, app).await.expect("serve provider") });
        Self {
            state,
            base,
            server,
        }
    }

    async fn upstream(&self, fixture: &DispatchFixture, kind: UpstreamKind) -> UpstreamRecord {
        provider_metadata::create_provider(fixture, kind, self.base.clone(), Default::default())
            .await
    }
}

fn usage(kind: UpstreamKind, percent: u64) -> Value {
    if kind == UpstreamKind::AnthropicOauth {
        json!({"seven_day": {"utilization": percent, "resets_at": 1_700_500_000}})
    } else {
        json!({
            "account_id": "synthetic-workspace", "plan_type": "pro",
            "rate_limit": {"primary_window": {"used_percent": percent, "limit_window_seconds": 604800, "reset_at": 1_700_500_000}},
            "rate_limit_reset_credits": {"available_count": 2, "applicable_available_count": 2}
        })
    }
}

fn epoch_key(kind: UpstreamKind, id: Uuid) -> String {
    if kind == UpstreamKind::AnthropicOauth {
        cedar_ember_epoch_meta_key(id)
    } else {
        openai_limit_reset_epoch_meta_key(id)
    }
}

fn selected(fixture: &DispatchFixture, upstream: &UpstreamRecord) -> Uuid {
    let candidate = |id, kind, quotas| UpstreamCandidate {
        upstream_id: id,
        name: "reset-routing".to_owned(),
        kind,
        subscription_quotas: quotas,
        observed_at_unix_secs: 1_700_000_000,
        cache_score: None,
        base_url: None,
        plan_capacity_ratio: None,
        organization_type: None,
        rate_limit_tier: None,
        seat_tier: None,
    };
    let key = Uuid::from_u128(1);
    let kind = if upstream.kind == UpstreamKind::AnthropicOauth {
        cc_lb_domain::UpstreamKind::AnthropicOauth
    } else {
        cc_lb_domain::UpstreamKind::OpenaiOauth
    };
    let key_kind = if upstream.kind == UpstreamKind::AnthropicOauth {
        cc_lb_domain::UpstreamKind::AnthropicApiKey
    } else {
        cc_lb_domain::UpstreamKind::OpenaiApiKey
    };
    let snapshots = fixture
        .dispatch
        .subscription_quota_cache
        .snapshot_for_upstream(upstream.id, 1_700_000_000_000, 1_800);
    let context = RoutingContext {
        request_id: "after-coupon".to_owned(),
        thread_id: None,
        requested_service_tier: None,
        downstream_headers: Default::default(),
        method: http::Method::POST,
        path: "/v1/responses".to_owned(),
        query: None,
        body_bytes: bytes::Bytes::new(),
        canonical_model_id: if upstream.kind == UpstreamKind::AnthropicOauth {
            "claude-sonnet-4-5"
        } else {
            "gpt-5"
        }
        .to_owned(),
        cache_pricing: Default::default(),
    };
    SubscriptionPreferenceFilter::new()
        .filter(
            &context,
            &Principal {
                id: "reset-test".to_owned(),
                kind: PrincipalKind::InternalKey,
            },
            &[
                candidate(upstream.id, kind, snapshots),
                candidate(key, key_kind, Vec::new()),
            ],
        )
        .expect("routing filter")
        .kept_upstream_ids[0]
}

async fn refresh(fixture: &DispatchFixture, upstream: &UpstreamRecord) -> LimitResetUsageRefresh {
    fixture
        .dispatch
        .refresh_upstream_usage(upstream.id, &CancellationToken::new())
        .await
}

#[tokio::test]
async fn reset_refresh_changes_real_routing_selection_without_cron_or_warmup() {
    for kind in [UpstreamKind::AnthropicOauth, UpstreamKind::OpenaiOauth] {
        let provider = Provider::new(kind, 100).await;
        let fixture = DispatchFixture::new().await;
        let upstream = provider.upstream(&fixture, kind).await;
        assert_eq!(
            refresh(&fixture, &upstream).await,
            LimitResetUsageRefresh::Refreshed
        );
        assert_eq!(selected(&fixture, &upstream), Uuid::from_u128(1));
        *provider.state.usage.lock().unwrap() = usage(kind, 12);
        fixture
            .storage
            .put_meta_value(&epoch_key(kind, upstream.id), &Uuid::new_v4().to_string())
            .await
            .unwrap();
        assert_eq!(
            refresh(&fixture, &upstream).await,
            LimitResetUsageRefresh::Refreshed
        );
        assert_eq!(selected(&fixture, &upstream), upstream.id);
        assert_eq!(provider.state.usage_calls.load(Ordering::SeqCst), 2);
        assert_eq!(fixture.refresh_entry_calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn successful_refresh_with_real_exhaustion_does_not_invent_availability() {
    let provider = Provider::new(UpstreamKind::OpenaiOauth, 100).await;
    let fixture = DispatchFixture::new().await;
    let upstream = provider.upstream(&fixture, UpstreamKind::OpenaiOauth).await;
    assert_eq!(
        refresh(&fixture, &upstream).await,
        LimitResetUsageRefresh::Refreshed
    );
    assert_eq!(selected(&fixture, &upstream), Uuid::from_u128(1));
    fixture
        .storage
        .put_meta_value(
            &epoch_key(upstream.kind, upstream.id),
            &Uuid::new_v4().to_string(),
        )
        .await
        .unwrap();
    assert_eq!(
        refresh(&fixture, &upstream).await,
        LimitResetUsageRefresh::Refreshed
    );
    assert_eq!(selected(&fixture, &upstream), Uuid::from_u128(1));
}

#[tokio::test]
async fn failed_refresh_keeps_previous_quota_and_routing_admission() {
    let provider = Provider::new(UpstreamKind::OpenaiOauth, 100).await;
    let fixture = DispatchFixture::new().await;
    let upstream = provider.upstream(&fixture, UpstreamKind::OpenaiOauth).await;
    assert_eq!(
        refresh(&fixture, &upstream).await,
        LimitResetUsageRefresh::Refreshed
    );
    *provider.state.usage.lock().unwrap() = usage(upstream.kind, 0);
    for status in [429, 503] {
        provider.state.usage_status.store(status, Ordering::SeqCst);
        assert_eq!(
            refresh(&fixture, &upstream).await,
            LimitResetUsageRefresh::Failed
        );
        assert_eq!(selected(&fixture, &upstream), Uuid::from_u128(1));
    }
}

#[tokio::test]
async fn delayed_pre_reset_usage_cannot_overwrite_post_reset_quota_or_inventory() {
    for kind in [UpstreamKind::AnthropicOauth, UpstreamKind::OpenaiOauth] {
        let provider = Provider::new(kind, 100).await;
        let fixture = DispatchFixture::new().await;
        let upstream = provider.upstream(&fixture, kind).await;
        provider
            .state
            .delay_next_usage
            .store(true, Ordering::SeqCst);
        let dispatch = fixture.dispatch.clone();
        let pending = tokio::spawn(async move { dispatch.poll_usage(upstream.id, None).await });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            provider.state.entered.notified(),
        )
        .await
        .expect("delayed pre-reset usage request starts");
        let epoch = Uuid::new_v4();
        fixture
            .storage
            .put_meta_value(&epoch_key(kind, upstream.id), &epoch.to_string())
            .await
            .unwrap();
        *provider.state.usage.lock().unwrap() = usage(kind, 15);
        assert_eq!(
            refresh(&fixture, &upstream).await,
            LimitResetUsageRefresh::Refreshed
        );
        assert_eq!(selected(&fixture, &upstream), upstream.id);
        provider.state.release.notify_one();
        assert!(pending.await.unwrap().is_err());
        assert_eq!(selected(&fixture, &upstream), upstream.id);
        if kind == UpstreamKind::OpenaiOauth {
            let record = inventory(&fixture, upstream.id).await;
            assert_eq!(record.epoch, Some(epoch));
        }
    }
}

#[tokio::test]
async fn credential_rotation_rejects_in_flight_usage_before_cache_publication() {
    for kind in [UpstreamKind::AnthropicOauth, UpstreamKind::OpenaiOauth] {
        let provider = Provider::new(kind, 100).await;
        let fixture = DispatchFixture::new().await;
        let upstream = provider.upstream(&fixture, kind).await;
        assert_eq!(
            refresh(&fixture, &upstream).await,
            LimitResetUsageRefresh::Refreshed
        );
        *provider.state.usage.lock().unwrap() = usage(kind, 0);
        provider
            .state
            .delay_next_usage
            .store(true, Ordering::SeqCst);
        let dispatch = fixture.dispatch.clone();
        let pending = tokio::spawn(async move { dispatch.poll_usage(upstream.id, None).await });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            provider.state.entered.notified(),
        )
        .await
        .expect("usage request starts before credential rotation");
        let mut bundle = upstream
            .oauth_credentials
            .as_ref()
            .unwrap()
            .decrypt(fixture.aead.as_ref(), upstream.id.as_bytes())
            .unwrap();
        bundle.access_token = "rotated-token".to_owned();
        let tokens =
            EncryptedOAuthTokens::encrypt(fixture.aead.as_ref(), &bundle, upstream.id.as_bytes())
                .unwrap();
        UpstreamStore::store_oauth_tokens(
            fixture.storage.as_ref(),
            upstream.id,
            upstream.revision,
            tokens,
            true,
        )
        .await
        .unwrap();
        provider.state.release.notify_one();
        assert!(pending.await.unwrap().is_err());
        assert_eq!(selected(&fixture, &upstream), Uuid::from_u128(1));
    }
}

async fn inventory(fixture: &DispatchFixture, id: Uuid) -> OpenAiLimitResetPollRecord {
    serde_json::from_str(
        &fixture
            .storage
            .get_meta_value(&openai_limit_reset_meta_key(id))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn inventory_failure_is_unknown_not_zero_and_does_not_block_usage_publication() {
    let provider = Provider::new(UpstreamKind::OpenaiOauth, 100).await;
    let fixture = DispatchFixture::new().await;
    let upstream = provider.upstream(&fixture, UpstreamKind::OpenaiOauth).await;
    assert_eq!(
        refresh(&fixture, &upstream).await,
        LimitResetUsageRefresh::Refreshed
    );
    let record = inventory(&fixture, upstream.id).await;
    assert_eq!(
        record.oauth_token_generation,
        upstream.oauth_token_generation
    );
    assert_eq!(
        record.credential_fingerprint,
        upstream.oauth_credential_fingerprint().unwrap()
    );
    assert_eq!(record.account_id, "synthetic-workspace");
    let value = record.inventory.unwrap();
    assert_eq!(value.available_count, 2);
    assert_eq!(value.credits.len(), 1);
    assert_eq!(value.applicable_available_count, Some(2));
    let manual = value.manual_redemption.unwrap();
    assert!(manual.eligible);
    assert_eq!(manual.windows.len(), 1);
    assert_eq!(manual.windows[0].window, "7d");
    assert_eq!(manual.windows[0].duration_seconds, Some(604800));
    assert_eq!(manual.windows[0].resets_at_unix_secs, Some(1_700_500_000));
    assert_eq!(
        provider.state.usage_calls.load(Ordering::SeqCst),
        1,
        "inventory reuses the existing usage response"
    );
    provider.state.inventory_status.store(503, Ordering::SeqCst);
    *provider.state.usage.lock().unwrap() = usage(upstream.kind, 10);
    assert_eq!(
        refresh(&fixture, &upstream).await,
        LimitResetUsageRefresh::Refreshed
    );
    assert_eq!(selected(&fixture, &upstream), upstream.id);
    assert!(inventory(&fixture, upstream.id).await.inventory.is_none());
}

#[tokio::test]
async fn live_reset_fence_blocks_usage_and_abandoned_openai_fence_recovers_before_get() {
    let provider = Provider::new(UpstreamKind::OpenaiOauth, 10).await;
    let fixture = DispatchFixture::new().await;
    let upstream = provider.upstream(&fixture, UpstreamKind::OpenaiOauth).await;
    let key = openai_limit_reset_epoch_meta_key(upstream.id);
    fixture
        .storage
        .put_meta_value(
            &key,
            &openai_limit_reset_epoch_fence(Uuid::new_v4(), 1_700_000_000_000),
        )
        .await
        .unwrap();
    assert_eq!(
        refresh(&fixture, &upstream).await,
        LimitResetUsageRefresh::Failed
    );
    assert_eq!(provider.state.usage_calls.load(Ordering::SeqCst), 0);
    fixture
        .storage
        .put_meta_value(
            &key,
            &openai_limit_reset_epoch_fence(Uuid::new_v4(), 1_699_999_000_000),
        )
        .await
        .unwrap();
    assert_eq!(
        refresh(&fixture, &upstream).await,
        LimitResetUsageRefresh::Refreshed
    );
    assert!(inventory(&fixture, upstream.id).await.epoch.is_some());
    assert_eq!(provider.state.usage_calls.load(Ordering::SeqCst), 1);
}

struct TestAdminAuth;

#[async_trait]
impl cc_lb_admin::auth::AdminAuthProvider for TestAdminAuth {
    fn id(&self) -> &str {
        "reset-integration"
    }
    fn is_static_token(&self) -> bool {
        true
    }
    async fn authenticate(&self, _: &http::HeaderMap) -> cc_lb_admin::auth::ProviderOutcome {
        cc_lb_admin::auth::ProviderOutcome::Verified(cc_lb_admin::auth::AdminIdentity {
            authority: "test".to_owned(),
            subject: "reset-integration".to_owned(),
            kind: cc_lb_admin::auth::AdminActorKind::BreakGlass,
            provider_id: self.id().to_owned(),
            email: None,
            display_name: None,
            groups: Vec::new(),
            expires_at_unix_secs: None,
        })
    }
}

fn admin_router(
    fixture: &DispatchFixture,
    port: Arc<dyn LimitResetUsageRefreshPort>,
) -> axum::Router {
    cc_lb_admin::router(cc_lb_admin::AdminState {
        build: crate::build_meta::BuildMeta::current().admin_build_info(),
        config_path: None,
        startup_config_overrides: Default::default(),
        storage: Some(fixture.storage.clone()),
        key_store: Some(fixture.dispatch.key_store.clone()),
        aead: fixture.aead.clone(),
        limit_engine: fixture.dispatch.limit_engine.clone(),
        lifecycle: None,
        lazy_refresher: None,
        runtime: Some(fixture.dispatch.runtime.clone()),
        data_dir: None,
        warmup_dialect_dispatcher: None,
        limit_reset_usage_refresh: Some(port),
        dynamic_view: fixture.dispatch.dynamic_view.clone(),
        config: fixture.dispatch.config.clone(),
        dynamic_view_rebinder: None,
        scheduler: None,
        admin_auth: Arc::new(cc_lb_admin::auth::AdminAuthenticator::new(vec![Arc::new(
            TestAdminAuth,
        )])),
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock: fixture.clock.clone(),
        anthropic_org_api: fixture.dispatch.anthropic_org_api.clone(),
    })
}

fn claim_request(id: Uuid) -> http::Request<axum::body::Body> {
    http::Request::post(format!("/admin/v1/upstreams/{id}/limit-resets/claim"))
        .header("authorization", "Bearer test")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({
                "account_id": "synthetic-workspace", "organization_id": null,
                "grant_id": "opaque-credit", "request_id": Uuid::new_v4().to_string()
            })
            .to_string(),
        ))
        .unwrap()
}

#[tokio::test]
async fn admin_claim_awaits_actual_usage_publication_before_next_routing_selection() {
    use tower::ServiceExt;

    for (after_reset_percent, after_reset_status, expected_refresh, routes_to_subscription) in [
        (12, 200, "refreshed", true),
        (100, 200, "refreshed", false),
        (12, 503, "failed", false),
    ] {
        let provider = Provider::new(UpstreamKind::OpenaiOauth, 100).await;
        provider
            .state
            .reset_percent
            .store(after_reset_percent, Ordering::SeqCst);
        provider
            .state
            .reset_usage_status
            .store(after_reset_status, Ordering::SeqCst);
        let fixture = DispatchFixture::new().await;
        let upstream = provider.upstream(&fixture, UpstreamKind::OpenaiOauth).await;
        assert_eq!(
            refresh(&fixture, &upstream).await,
            LimitResetUsageRefresh::Refreshed
        );
        assert_eq!(selected(&fixture, &upstream), Uuid::from_u128(1));

        let response = admin_router(&fixture, Arc::new(fixture.dispatch.clone()))
            .oneshot(claim_request(upstream.id))
            .await
            .unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["result"], "reset");
        assert_eq!(body["usage_refresh"], expected_refresh);
        assert_eq!(body["windows_reset"], 1);
        assert_eq!(
            selected(&fixture, &upstream),
            if routes_to_subscription {
                upstream.id
            } else {
                Uuid::from_u128(1)
            }
        );
        assert_eq!(provider.state.consume_calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.state.usage_calls.load(Ordering::SeqCst), 3);
    }
}

struct ObservedRefresh {
    dispatch: SchedulerDispatch,
    finished: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl LimitResetUsageRefreshPort for ObservedRefresh {
    async fn refresh_upstream_usage(
        &self,
        id: Uuid,
        cancel: &CancellationToken,
    ) -> LimitResetUsageRefresh {
        let result = self.dispatch.refresh_upstream_usage(id, cancel).await;
        self.finished.notify_one();
        result
    }
}

#[tokio::test]
async fn dropped_admin_request_does_not_cancel_committed_claim_usage_refresh() {
    use tower::ServiceExt;

    let provider = Provider::new(UpstreamKind::OpenaiOauth, 100).await;
    let fixture = DispatchFixture::new().await;
    let upstream = provider.upstream(&fixture, UpstreamKind::OpenaiOauth).await;
    assert_eq!(
        refresh(&fixture, &upstream).await,
        LimitResetUsageRefresh::Refreshed
    );
    assert_eq!(selected(&fixture, &upstream), Uuid::from_u128(1));
    provider
        .state
        .delay_reset_usage
        .store(true, Ordering::SeqCst);
    let finished = Arc::new(tokio::sync::Notify::new());
    let app = admin_router(
        &fixture,
        Arc::new(ObservedRefresh {
            dispatch: fixture.dispatch.clone(),
            finished: finished.clone(),
        }),
    );
    let request = tokio::spawn(async move { app.oneshot(claim_request(upstream.id)).await });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        provider.state.entered.notified(),
    )
    .await
    .expect("claim continuation starts its actual usage request");
    request.abort();
    provider.state.release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(5), finished.notified())
        .await
        .expect("continuation finishes after request drop");
    assert_eq!(selected(&fixture, &upstream), upstream.id);
    assert_eq!(provider.state.consume_calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.state.usage_calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn mismatched_usage_identity_cannot_publish_quota_or_merge_coupon_applicability() {
    let provider = Provider::new(UpstreamKind::OpenaiOauth, 100).await;
    let fixture = DispatchFixture::new().await;
    let upstream = provider.upstream(&fixture, UpstreamKind::OpenaiOauth).await;
    assert_eq!(
        refresh(&fixture, &upstream).await,
        LimitResetUsageRefresh::Refreshed
    );
    let mut wrong_account = usage(upstream.kind, 0);
    wrong_account["account_id"] = json!("different-account");
    *provider.state.usage.lock().unwrap() = wrong_account;
    assert_eq!(
        refresh(&fixture, &upstream).await,
        LimitResetUsageRefresh::Failed
    );
    assert_eq!(selected(&fixture, &upstream), Uuid::from_u128(1));
    assert_eq!(provider.state.inventory_calls.load(Ordering::SeqCst), 1);
    let mut mismatched_total = usage(upstream.kind, 20);
    mismatched_total["rate_limit_reset_credits"]["available_count"] = json!(3);
    *provider.state.usage.lock().unwrap() = mismatched_total;
    assert_eq!(
        refresh(&fixture, &upstream).await,
        LimitResetUsageRefresh::Refreshed
    );
    assert_eq!(
        inventory(&fixture, upstream.id)
            .await
            .inventory
            .unwrap()
            .applicable_available_count,
        None
    );
}

#[tokio::test]
async fn refresh_missing_upstream_is_not_applicable() {
    let fixture = DispatchFixture::new().await;
    assert_eq!(
        fixture
            .dispatch
            .refresh_upstream_usage(Uuid::new_v4(), &CancellationToken::new())
            .await,
        LimitResetUsageRefresh::NotApplicable,
    );
}

#[tokio::test]
async fn reset_inventory_eligibility_uses_this_poll_main_usage_without_an_extra_usage_get() {
    let provider = Provider::new(UpstreamKind::OpenaiOauth, 20).await;
    let fixture = DispatchFixture::new().await;
    let upstream = provider.upstream(&fixture, UpstreamKind::OpenaiOauth).await;
    let mut body = usage(upstream.kind, 20);
    body["ordinary_usage_allowed"] = json!(false);
    body["additional_rate_limits"] = json!([{"rate_limit":{"primary_window":{
        "limit_window_seconds":604800,"used_percent":100,"reset_at":1_700_500_000
    }}}]);
    *provider.state.usage.lock().unwrap() = body;
    assert_eq!(
        refresh(&fixture, &upstream).await,
        LimitResetUsageRefresh::Refreshed
    );
    let manual = inventory(&fixture, upstream.id)
        .await
        .inventory
        .unwrap()
        .manual_redemption
        .unwrap();
    assert!(!manual.eligible);
    assert_eq!(manual.reason.as_deref(), Some("below_redemption_threshold"));
    assert_eq!(manual.windows.len(), 1);
    assert_eq!(
        manual.windows[0].used_percent,
        Some(serde_json::Number::from(20))
    );
    assert_eq!(provider.state.usage_calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.state.inventory_calls.load(Ordering::SeqCst), 1);
    let mut body = usage(upstream.kind, 21);
    body["plan_type"] = json!("unverified_business");
    *provider.state.usage.lock().unwrap() = body;
    assert_eq!(
        refresh(&fixture, &upstream).await,
        LimitResetUsageRefresh::Refreshed
    );
    let manual = inventory(&fixture, upstream.id)
        .await
        .inventory
        .unwrap()
        .manual_redemption
        .unwrap();
    assert!(manual.eligible);
    assert!(
        manual.reason.is_none(),
        "native main usage establishes coverage without a plan catalog"
    );
    assert_eq!(provider.state.usage_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn admin_claim_refuses_when_cached_reset_eligibility_no_longer_matches_live_usage() {
    use tower::ServiceExt;
    let provider = Provider::new(UpstreamKind::OpenaiOauth, 100).await;
    let fixture = DispatchFixture::new().await;
    let upstream = provider.upstream(&fixture, UpstreamKind::OpenaiOauth).await;
    assert_eq!(
        refresh(&fixture, &upstream).await,
        LimitResetUsageRefresh::Refreshed
    );
    assert!(
        inventory(&fixture, upstream.id)
            .await
            .inventory
            .unwrap()
            .manual_redemption
            .unwrap()
            .eligible
    );
    *provider.state.usage.lock().unwrap() = usage(upstream.kind, 20);
    let response = admin_router(&fixture, Arc::new(fixture.dispatch.clone()))
        .oneshot(claim_request(upstream.id))
        .await
        .unwrap();
    assert_eq!(response.status(), http::StatusCode::CONFLICT);
    assert_eq!(provider.state.consume_calls.load(Ordering::SeqCst), 0);
    assert_eq!(provider.state.usage_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        selected(&fixture, &upstream),
        Uuid::from_u128(1),
        "refused claim never optimistically clears the existing routing sample"
    );
    assert!(
        fixture
            .storage
            .get_meta_value(&openai_limit_reset_epoch_meta_key(upstream.id))
            .await
            .unwrap()
            .is_none()
    );
}

/// Stops the poll on its third clock read after inventory was fetched:
/// classifier freshness, inventory observation, then quota publication time.
/// The last read is immediately after the final durable epoch validation.
struct AfterEpochReadClock {
    provider: Arc<ProviderState>,
    reads_after_inventory: AtomicUsize,
    entered: tokio::sync::Notify,
    release: (Mutex<bool>, std::sync::Condvar),
    paused_once: AtomicBool,
    seconds: std::sync::atomic::AtomicU64,
}

impl cc_lb_clock::Clock for AfterEpochReadClock {
    fn now(&self) -> std::time::SystemTime {
        if self.provider.inventory_calls.load(Ordering::SeqCst) > 0
            && !self.paused_once.load(Ordering::SeqCst)
            && self.reads_after_inventory.fetch_add(1, Ordering::SeqCst) == 2
            && !self.paused_once.swap(true, Ordering::SeqCst)
        {
            self.entered.notify_one();
            let released = self.release.0.lock().unwrap();
            let (released, _) = self
                .release
                .1
                .wait_timeout_while(released, std::time::Duration::from_secs(10), |released| {
                    !*released
                })
                .unwrap();
            assert!(
                *released,
                "test must release the poll paused after its epoch read"
            );
        }
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(self.seconds.load(Ordering::SeqCst))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poll_paused_after_final_epoch_read_cannot_restore_pre_reset_exhaustion() {
    let provider = Provider::new(UpstreamKind::OpenaiOauth, 100).await;
    let mut fixture = DispatchFixture::new().await;
    let upstream = provider.upstream(&fixture, UpstreamKind::OpenaiOauth).await;
    let clock = Arc::new(AfterEpochReadClock {
        provider: provider.state.clone(),
        reads_after_inventory: AtomicUsize::new(0),
        entered: tokio::sync::Notify::new(),
        release: (Mutex::new(false), std::sync::Condvar::new()),
        paused_once: AtomicBool::new(false),
        seconds: std::sync::atomic::AtomicU64::new(1_700_000_000),
    });
    fixture.dispatch.clock = clock.clone();
    let dispatch = fixture.dispatch.clone();
    let old_poll = tokio::spawn(async move { dispatch.poll_usage(upstream.id, None).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), clock.entered.notified())
        .await
        .expect("poll pauses after final epoch read and before quota publication");
    fixture
        .storage
        .put_meta_value(
            &openai_limit_reset_epoch_meta_key(upstream.id),
            &Uuid::new_v4().to_string(),
        )
        .await
        .unwrap();
    *provider.state.usage.lock().unwrap() = usage(upstream.kind, 12);
    let refreshed = refresh(&fixture, &upstream).await;
    let selected_after_refresh = selected(&fixture, &upstream);
    // A timestamp acquired after resumption must not make the stale body newer.
    clock.seconds.store(1_700_000_001, Ordering::SeqCst);
    *clock.release.0.lock().unwrap() = true;
    clock.release.1.notify_one();
    let _old_outcome = old_poll.await.unwrap();
    assert_eq!(refreshed, LimitResetUsageRefresh::Refreshed);
    assert_eq!(selected_after_refresh, upstream.id);
    assert_eq!(
        selected(&fixture, &upstream),
        upstream.id,
        "a pre-reset body validated before settlement must not overwrite refreshed routing quota"
    );
    let durable = fixture
        .storage
        .list_latest_subscription_quota_for_upstreams(&[upstream.id])
        .await
        .unwrap();
    assert_eq!(
        durable
            .iter()
            .find(|sample| sample.window == cc_lb_storage_api::SubscriptionQuotaWindow::SevenDay)
            .unwrap()
            .utilization,
        Some(0.12),
        "durable latest must retain the post-reset observation"
    );
}

#[tokio::test]
async fn monthly_main_without_ordinary_credits_keeps_coupon_inventory_without_claiming_quota_refresh()
 {
    let provider = Provider::new(UpstreamKind::OpenaiOauth, 100).await;
    let fixture = DispatchFixture::new().await;
    let upstream = provider.upstream(&fixture, UpstreamKind::OpenaiOauth).await;
    let mut monthly = usage(upstream.kind, 100);
    monthly["plan_type"] = json!("go");
    monthly["rate_limit"]["primary_window"]["limit_window_seconds"] = json!(2_592_000);
    monthly["credits"] = Value::Null;
    *provider.state.usage.lock().unwrap() = monthly;
    let result = refresh(&fixture, &upstream).await;
    let raw = fixture
        .storage
        .get_meta_value(&openai_limit_reset_meta_key(upstream.id))
        .await
        .unwrap();
    assert!(
        raw.is_some(),
        "valid monthly usage must not discard the already-fetched banked reset inventory"
    );
    let record: OpenAiLimitResetPollRecord = serde_json::from_str(&raw.unwrap()).unwrap();
    let manual = record.inventory.unwrap().manual_redemption.unwrap();
    assert!(manual.eligible);
    assert_eq!(manual.windows[0].window, "monthly");
    assert_eq!(manual.windows[0].duration_seconds, Some(2_592_000));
    assert_ne!(
        result,
        LimitResetUsageRefresh::Refreshed,
        "inventory publication alone is not affected-quota routing publication"
    );
    assert!(
        fixture
            .dispatch
            .subscription_quota_cache
            .snapshot_for_upstream(upstream.id, 1_700_000_000_000, 1_800)
            .iter()
            .all(|snapshot| snapshot.observed_at_unix_millis.is_none())
    );
    assert_eq!(provider.state.usage_calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.state.inventory_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn monthly_main_with_ordinary_credits_does_not_report_affected_quota_refreshed() {
    let provider = Provider::new(UpstreamKind::OpenaiOauth, 100).await;
    let fixture = DispatchFixture::new().await;
    let upstream = provider.upstream(&fixture, UpstreamKind::OpenaiOauth).await;
    let mut monthly = usage(upstream.kind, 100);
    monthly["rate_limit"]["primary_window"]["limit_window_seconds"] = json!(2_592_000);
    monthly["credits"] = json!({"has_credits":true,"balance":"1.00"});
    *provider.state.usage.lock().unwrap() = monthly;
    let result = refresh(&fixture, &upstream).await;
    assert_ne!(
        result,
        LimitResetUsageRefresh::Refreshed,
        "publishing ordinary credits does not refresh the reset's affected main quota"
    );
    let record = inventory(&fixture, upstream.id).await;
    assert!(
        record
            .inventory
            .unwrap()
            .manual_redemption
            .unwrap()
            .eligible
    );
    let snapshots = fixture
        .dispatch
        .subscription_quota_cache
        .snapshot_for_upstream(upstream.id, 1_700_000_000_000, 1_800);
    assert!(
        snapshots
            .iter()
            .filter(|snapshot| snapshot.window == "5h" || snapshot.window == "7d")
            .all(|snapshot| snapshot.observed_at_unix_millis.is_none())
    );
}
