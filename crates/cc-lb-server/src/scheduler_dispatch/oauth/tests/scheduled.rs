use super::*;

use cc_lb_clock::Clock;
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::retry::JobOutcome;
use http::StatusCode;

async fn token_server(
    status: StatusCode,
    body: &'static str,
) -> (Url, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let recorded = calls.clone();
    let app = axum::Router::new().route(
        "/oauth/token",
        axum::routing::post(move || {
            recorded.fetch_add(1, Ordering::SeqCst);
            async move { (status, [("content-type", "application/json")], body) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind token server");
    let endpoint = Url::parse(&format!(
        "http://{}/oauth/token",
        listener.local_addr().expect("token server address")
    ))
    .expect("token URL");
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve token endpoint");
    });
    (endpoint, calls, task)
}

fn set_token_endpoint(fixture: &mut DispatchFixture, endpoint: Url) {
    let mut config = fixture.dispatch.oauth_cfg.as_ref().clone();
    config.token_url = endpoint;
    fixture.dispatch.oauth_cfg = Arc::new(config);
    fixture.dispatch.replica_id = Some(Uuid::new_v4());
}

#[tokio::test]
async fn terminal_refresh_stops_dispatch_watchdog_and_replacement_dispatch() {
    for (status, body, reason) in [
        (
            StatusCode::UNAUTHORIZED,
            "provider response must not be persisted",
            "status_401",
        ),
        (
            StatusCode::BAD_REQUEST,
            r#"{"error":"invalid_grant","error_description":"private provider detail"}"#,
            "status_400",
        ),
    ] {
        let (endpoint, calls, server) = token_server(status, body).await;
        let mut fixture = DispatchFixture::new().await;
        set_token_endpoint(&mut fixture, endpoint);
        let upstream = fixture.oauth_upstream(false).await;
        let job = OAuthRefreshJob::new(upstream.id);

        assert_eq!(
            fixture
                .dispatch
                .dispatch_oauth_refresh(job.clone())
                .await
                .expect("terminal response"),
            JobOutcome::Skip
        );
        let failed = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
            .await
            .expect("load failure")
            .expect("upstream exists");
        assert_eq!(failed.last_apply_error.as_deref(), Some(reason));
        assert_eq!(
            failed.oauth_token_generation,
            upstream.oauth_token_generation
        );
        assert!(
            !fixture
                .dispatch
                .list_oauth_refresh_watchdog_upstream_ids()
                .await
                .expect("watchdog candidates")
                .contains(&upstream.id)
        );
        assert_eq!(
            fixture
                .dispatch
                .dispatch_oauth_refresh(job.clone())
                .await
                .expect("repeat dispatch"),
            JobOutcome::Skip
        );

        let database_url = format!(
            "sqlite://{}",
            fixture._dir.path().join("cc-lb.sqlite").display()
        );
        let reopened = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url, fixture.clock.clone())
                .await
                .expect("replacement storage"),
        );
        let mut replacement = fixture.dispatch.clone();
        replacement.storage = reopened;
        assert_eq!(
            replacement
                .dispatch_oauth_refresh(job)
                .await
                .expect("replacement dispatch"),
            JobOutcome::Skip
        );
        assert!(
            !replacement
                .list_oauth_refresh_watchdog_upstream_ids()
                .await
                .expect("replacement watchdog candidates")
                .contains(&upstream.id)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let tasks = fixture
            .dispatch
            .backend
            .list_adaptive_tasks(&Filter {
                status: Some(TaskStatus::Pending),
                page: 1,
                page_size: Some(100),
            })
            .await
            .expect("pending scheduled tasks");
        assert!(tasks.iter().all(|task| !matches!(
            task.args,
            AdaptiveJob::OAuthRefresh(_) | AdaptiveJob::MetadataRefresh(_)
        )));
        server.abort();
    }
}

#[tokio::test]
async fn known_expired_refresh_deadline_stops_before_provider_and_usage_refresh() {
    let (endpoint, calls, server) = token_server(
        StatusCode::OK,
        r#"{"access_token":"new-access","expires_in":3600}"#,
    )
    .await;
    let mut fixture = DispatchFixture::new().await;
    set_token_endpoint(&mut fixture, endpoint);
    let upstream = fixture.oauth_upstream(false).await;
    let mut bundle = upstream
        .oauth_credentials
        .as_ref()
        .expect("credentials")
        .decrypt(fixture.aead.as_ref(), upstream.id.as_bytes())
        .expect("decrypt");
    bundle.refresh_token_expires_at_unix_secs = Some(cc_lb_clock::unix_secs(fixture.clock.now()));
    let encrypted =
        EncryptedOAuthTokens::encrypt(fixture.aead.as_ref(), &bundle, upstream.id.as_bytes())
            .expect("encrypt expired refresh token");
    let mut expired = UpstreamStore::store_oauth_tokens(
        fixture.storage.as_ref(),
        upstream.id,
        upstream.revision,
        encrypted,
        false,
    )
    .await
    .expect("store expiry");

    fixture
        .dispatch
        .ensure_fresh_usage_token(&mut expired)
        .await
        .expect("proactive expiry gate");
    assert_eq!(
        expired.last_apply_error.as_deref(),
        Some("refresh_token_expired")
    );
    assert_eq!(fixture.refresh_entry_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture
            .dispatch
            .dispatch_oauth_refresh(OAuthRefreshJob::new(upstream.id))
            .await
            .expect("scheduled expiry gate"),
        JobOutcome::Skip
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    server.abort();
}

#[tokio::test]
async fn scheduled_known_expiry_is_recorded_without_provider_request() {
    let (endpoint, calls, server) = token_server(StatusCode::UNAUTHORIZED, "").await;
    let mut fixture = DispatchFixture::new().await;
    set_token_endpoint(&mut fixture, endpoint);
    let upstream = fixture.oauth_upstream(false).await;
    let mut bundle = upstream
        .oauth_credentials
        .as_ref()
        .expect("credentials")
        .decrypt(fixture.aead.as_ref(), upstream.id.as_bytes())
        .expect("decrypt");
    bundle.refresh_token_expires_at_unix_secs = Some(1);
    let encrypted =
        EncryptedOAuthTokens::encrypt(fixture.aead.as_ref(), &bundle, upstream.id.as_bytes())
            .expect("encrypt expired refresh token");
    UpstreamStore::store_oauth_tokens(
        fixture.storage.as_ref(),
        upstream.id,
        upstream.revision,
        encrypted,
        false,
    )
    .await
    .expect("store expiry");
    assert_eq!(
        fixture
            .dispatch
            .dispatch_oauth_refresh(OAuthRefreshJob::new(upstream.id))
            .await
            .expect("scheduled expiry gate"),
        JobOutcome::Skip
    );
    let stored = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
        .await
        .expect("load expiry state")
        .expect("upstream exists");
    assert_eq!(
        stored.last_apply_error.as_deref(),
        Some("refresh_token_expired")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    server.abort();
}

#[tokio::test]
async fn transient_refresh_responses_keep_existing_retry_policy() {
    for (status, body) in [
        (StatusCode::BAD_REQUEST, r#"{"error":"invalid_request"}"#),
        (StatusCode::BAD_REQUEST, "unstructured error"),
        (
            StatusCode::TOO_MANY_REQUESTS,
            r#"{"error":"invalid_grant"}"#,
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"error":"invalid_grant"}"#,
        ),
    ] {
        let (endpoint, calls, server) = token_server(status, body).await;
        let mut fixture = DispatchFixture::new().await;
        set_token_endpoint(&mut fixture, endpoint);
        let upstream = fixture.oauth_upstream(false).await;
        for _ in 0..2 {
            assert_eq!(
                fixture
                    .dispatch
                    .dispatch_oauth_refresh(OAuthRefreshJob::new(upstream.id))
                    .await
                    .expect("retryable response"),
                JobOutcome::Retry {
                    delay: Some(std::time::Duration::from_secs(30)),
                }
            );
        }
        let stored = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
            .await
            .expect("load transient state")
            .expect("upstream exists");
        assert_eq!(stored.last_apply_error, None);
        assert!(
            fixture
                .dispatch
                .list_oauth_refresh_watchdog_upstream_ids()
                .await
                .expect("retryable watchdog candidates")
                .contains(&upstream.id)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        server.abort();
    }
}

#[tokio::test]
async fn disabled_valid_oauth_credential_refreshes_and_schedules_next_cycle() {
    let (endpoint, calls, server) = token_server(
        StatusCode::OK,
        r#"{"access_token":"new-access","refresh_token":"new-refresh","expires_in":3600}"#,
    )
    .await;
    let mut fixture = DispatchFixture::new().await;
    set_token_endpoint(&mut fixture, endpoint);
    let upstream = fixture.oauth_upstream(false).await;
    UpstreamStore::update(
        fixture.storage.as_ref(),
        upstream.id,
        upstream.revision,
        UpstreamUpdate {
            enabled: Some(false),
            ..UpstreamUpdate::default()
        },
    )
    .await
    .expect("disable routing");

    assert_eq!(
        fixture
            .dispatch
            .dispatch_oauth_refresh(OAuthRefreshJob::new(upstream.id))
            .await
            .expect("disabled valid refresh"),
        JobOutcome::Done
    );
    let stored = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
        .await
        .expect("load refreshed state")
        .expect("upstream exists");
    assert!(!stored.enabled);
    assert_eq!(
        stored.oauth_token_generation,
        upstream.oauth_token_generation + 1
    );
    let bundle = stored
        .oauth_credentials
        .as_ref()
        .expect("credentials")
        .decrypt(fixture.aead.as_ref(), upstream.id.as_bytes())
        .expect("decrypt updated token");
    assert_eq!(bundle.access_token, "new-access");
    let tasks = fixture
        .dispatch
        .backend
        .list_adaptive_tasks(&Filter {
            status: Some(TaskStatus::Pending),
            page: 1,
            page_size: Some(100),
        })
        .await
        .expect("scheduled tasks");
    assert!(
        tasks
            .iter()
            .any(|task| matches!(task.args, AdaptiveJob::OAuthRefresh(_)))
    );
    assert!(
        tasks
            .iter()
            .any(|task| matches!(task.args, AdaptiveJob::MetadataRefresh(_)))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn stale_terminal_provider_response_cannot_poison_reconnected_credential() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let observed = entered.clone();
    let resumed = release.clone();
    let app = axum::Router::new().route(
        "/oauth/token",
        axum::routing::post(move || {
            let observed = observed.clone();
            let resumed = resumed.clone();
            async move {
                observed.notify_one();
                resumed.notified().await;
                (StatusCode::BAD_REQUEST, r#"{"error":"invalid_grant"}"#)
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind token server");
    let endpoint = Url::parse(&format!(
        "http://{}/oauth/token",
        listener.local_addr().expect("address")
    ))
    .expect("token endpoint");
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve token endpoint");
    });
    let mut fixture = DispatchFixture::new().await;
    set_token_endpoint(&mut fixture, endpoint);
    let upstream = fixture.oauth_upstream(false).await;
    let dispatch = fixture.dispatch.clone();
    let attempt = tokio::spawn(async move {
        dispatch
            .dispatch_oauth_refresh(OAuthRefreshJob::new(upstream.id))
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .expect("provider request started");
    let bundle = OAuthTokenBundle {
        access_token: "reconnected-access".to_owned(),
        refresh_token: "reconnected-refresh".to_owned(),
        expires_at_unix_secs: cc_lb_clock::unix_secs(fixture.clock.now()) + 3600,
        refresh_token_expires_at_unix_secs: None,
        account_id: None,
        project_id: None,
        oauth_profile: Default::default(),
        scopes: vec!["messages".to_owned()],
        never_refresh: false,
    };
    let encrypted =
        EncryptedOAuthTokens::encrypt(fixture.aead.as_ref(), &bundle, upstream.id.as_bytes())
            .expect("encrypt reconnect");
    let reconnected = UpstreamStore::store_oauth_tokens(
        fixture.storage.as_ref(),
        upstream.id,
        upstream.revision,
        encrypted,
        false,
    )
    .await
    .expect("reconnect token write");
    release.notify_one();
    assert_eq!(
        attempt
            .await
            .expect("refresh task")
            .expect("stale terminal skipped"),
        JobOutcome::Skip
    );
    let stored = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
        .await
        .expect("load raced state")
        .expect("upstream exists");
    assert_eq!(stored.last_apply_error, None);
    assert_eq!(
        stored.oauth_token_generation,
        reconnected.oauth_token_generation
    );
    assert!(
        fixture
            .dispatch
            .list_oauth_refresh_watchdog_upstream_ids()
            .await
            .expect("reconnected watchdog candidates")
            .contains(&upstream.id)
    );
    server.abort();
}

#[tokio::test]
async fn network_refresh_failure_keeps_existing_retry_policy() {
    let mut fixture = DispatchFixture::new().await;
    fixture.dispatch.replica_id = Some(Uuid::new_v4());
    let upstream = fixture.oauth_upstream(false).await;
    assert_eq!(
        fixture
            .dispatch
            .dispatch_oauth_refresh(OAuthRefreshJob::new(upstream.id))
            .await
            .expect("network failure is retryable"),
        JobOutcome::Retry {
            delay: Some(std::time::Duration::from_secs(30)),
        }
    );
    let stored = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
        .await
        .expect("load network failure state")
        .expect("upstream exists");
    assert_eq!(stored.last_apply_error, None);
}

#[tokio::test]
async fn in_flight_usage_success_cannot_clear_terminal_refresh_failure() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let observed = entered.clone();
    let resumed = release.clone();
    let app = axum::Router::new().route(
        "/api/oauth/usage",
        axum::routing::get(move || {
            let observed = observed.clone();
            let resumed = resumed.clone();
            async move {
                observed.notify_one();
                resumed.notified().await;
                axum::Json(serde_json::json!({
                    "five_hour": {"utilization": 20.0, "resets_at": "2026-09-30T20:00:00Z"}
                }))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind usage endpoint");
    let base_url = Url::parse(&format!(
        "http://{}/",
        listener.local_addr().expect("usage address")
    ))
    .expect("usage base URL");
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve usage endpoint");
    });
    let fixture = DispatchFixture::new().await;
    let upstream = fixture.oauth_upstream(true).await;
    UpstreamStore::update(
        fixture.storage.as_ref(),
        upstream.id,
        upstream.revision,
        UpstreamUpdate {
            base_url: Some(Some(base_url)),
            ..UpstreamUpdate::default()
        },
    )
    .await
    .expect("set usage endpoint");
    let dispatch = fixture.dispatch.clone();
    let poll = tokio::spawn(async move { dispatch.poll_usage(upstream.id, None).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .expect("usage request started");
    UpstreamStore::set_status(
        fixture.storage.as_ref(),
        upstream.id,
        UpstreamStatusUpdate {
            last_apply_error: Some(Some("status_401".to_owned())),
            expected_oauth_token_generation: Some(upstream.oauth_token_generation),
            ..UpstreamStatusUpdate::default()
        },
    )
    .await
    .expect("terminal refresh failure lands during usage poll");
    release.notify_one();
    assert!(matches!(
        poll.await
            .expect("usage poll task")
            .expect("usage response"),
        cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsagePollObservation::Success { .. }
    ));
    let stored = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
        .await
        .expect("load terminal state")
        .expect("upstream exists");
    assert_eq!(stored.last_apply_error.as_deref(), Some("status_401"));
    server.abort();
}

#[tokio::test]
async fn refresh_failure_status_does_not_wait_for_unfinished_provider_body() {
    use futures_util::StreamExt;

    for status in [
        StatusCode::UNAUTHORIZED,
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::SERVICE_UNAVAILABLE,
    ] {
        let app = axum::Router::new().route(
            "/oauth/token",
            axum::routing::post(move || async move {
                axum::response::Response::builder()
                    .status(status)
                    .body(axum::body::Body::from_stream(
                        futures_util::stream::once(async {
                            Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"x"))
                        })
                        .chain(futures_util::stream::pending()),
                    ))
                    .expect("unfinished token response")
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind unfinished token endpoint");
        let endpoint = Url::parse(&format!(
            "http://{}/oauth/token",
            listener.local_addr().expect("token endpoint address")
        ))
        .expect("token endpoint URL");
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve token endpoint");
        });
        let config = cc_lb_config::AnthropicOAuthConfig {
            client_id: "test-client".to_owned(),
            auth_url: endpoint.clone(),
            token_url: endpoint.clone(),
            redirect_uri: endpoint,
            scopes: vec!["messages".to_owned()],
        };
        let client = crate::scheduler_dispatch::http::json_http_client();
        let cancel = CancellationToken::new();
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            crate::scheduler_dispatch::http::request_refresh(
                &client,
                &config,
                &cc_lb_config::Config::default(),
                cc_lb_storage_api::upstream::UpstreamKind::AnthropicOauth,
                cc_lb_oauth_protocol::GeminiOAuthProfile::GeminiCli,
                &cancel,
                "refresh-token",
            ),
        )
        .await
        .expect("failure status returns while response body is unfinished")
        .expect_err("non-success token endpoint response");
        match error {
            crate::scheduler_dispatch::http::RefreshRequestError::Endpoint {
                status: observed,
                ..
            } => assert_eq!(observed, status),
            other => panic!("expected typed provider status, got {other:?}"),
        }
        server.abort();
    }
}

struct CompleteRefreshClaimGuard {
    storage: Arc<dyn UpstreamStore>,
    aead: Arc<AeadService>,
    replacement_access_token: String,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl LazyRefreshClaimGuard for CompleteRefreshClaimGuard {
    async fn begin_refresh(
        &self,
        upstream_id: Uuid,
        _holder: &str,
        _expires_at_unix_secs: u64,
        now_unix_secs: u64,
    ) -> StorageResult<LazyRefreshClaim> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let upstream = self
            .storage
            .get_by_id(upstream_id)
            .await?
            .expect("upstream exists for claim guard");
        let mut bundle = upstream
            .oauth_credentials
            .as_ref()
            .expect("oauth credentials exist")
            .decrypt(self.aead.as_ref(), upstream_id.as_bytes())
            .expect("decrypt oauth bundle");
        bundle.access_token = self.replacement_access_token.clone();
        bundle.expires_at_unix_secs = now_unix_secs + 3600;
        let encrypted =
            EncryptedOAuthTokens::encrypt(self.aead.as_ref(), &bundle, upstream_id.as_bytes())
                .expect("encrypt replacement oauth bundle");
        self.storage
            .complete_refresh(upstream_id, Uuid::new_v4(), encrypted)
            .await?;
        Ok(LazyRefreshClaim::Enqueued {
            idempotency_key: format!("poll-test-{upstream_id}"),
        })
    }

    async fn complete_and_bump_generation(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _generation: u64,
    ) -> StorageResult<bool> {
        Ok(true)
    }

    async fn release_if_holder(&self, _upstream_id: Uuid, _holder: &str) -> StorageResult<bool> {
        Ok(true)
    }
}

struct AcquiredClaimGuard {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl LazyRefreshClaimGuard for AcquiredClaimGuard {
    async fn begin_refresh(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _expires_at_unix_secs: u64,
        _now_unix_secs: u64,
    ) -> StorageResult<LazyRefreshClaim> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(LazyRefreshClaim::Acquired)
    }

    async fn complete_and_bump_generation(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _generation: u64,
    ) -> StorageResult<bool> {
        Ok(true)
    }

    async fn release_if_holder(&self, _upstream_id: Uuid, _holder: &str) -> StorageResult<bool> {
        Ok(true)
    }
}

fn install_lazy_refresher(
    fixture: &mut DispatchFixture,
    claim_guard: Arc<dyn LazyRefreshClaimGuard>,
) {
    fixture.dispatch.lazy_refresher = Some(Arc::new(LazyRefresher::new_with_claim_guard(
        LazyRefresherDeps {
            google_config: fixture.dispatch.config.clone(),
            stores: fixture.dispatch.stores.clone(),
            aead: fixture.aead.clone(),
            oauth_cfg: fixture.dispatch.oauth_cfg.clone(),
            clock: fixture.dispatch.clock.clone(),
        },
        Uuid::new_v4(),
        None,
        CancellationToken::new(),
        claim_guard,
        fixture.dispatch.backend.clone(),
    )));
}

fn configure_loopback_google(
    fixture: &mut DispatchFixture,
    base_url: &Url,
    token_url: Option<&Url>,
) {
    let mut cfg = fixture.dispatch.config.as_ref().clone();
    let userinfo_url = base_url.join("userinfo").expect("userinfo url");
    let mut gemini = cfg.oauth.gemini.clone().unwrap_or_default();
    gemini.userinfo_url = userinfo_url.clone();
    gemini.code_assist_url = base_url.clone();
    cfg.oauth.gemini = Some(gemini);

    let mut antigravity = cfg.oauth.antigravity.clone().unwrap_or_default();
    antigravity.client_id = "antigravity-test-client".to_owned();
    antigravity.client_secret = "antigravity-test-secret".to_owned();
    antigravity.userinfo_url = userinfo_url;
    antigravity.code_assist_url = base_url.clone();
    if let Some(url) = token_url {
        antigravity.token_url = url.clone();
    }
    cfg.oauth.antigravity = Some(antigravity);
    fixture.dispatch.config = Arc::new(cfg);
}

async fn create_usage_poll_upstream(
    fixture: &DispatchFixture,
    kind: UpstreamKind,
    profile: cc_lb_oauth_protocol::GeminiOAuthProfile,
    base_url: Url,
    access_token: &str,
) -> UpstreamRecord {
    let upstream_id = Uuid::new_v4();
    let created = UpstreamStore::create(
        fixture.storage.as_ref(),
        UpstreamCreate {
            vertex: None,
            vertex_credential_type: None,
            vertex_quota_project_id: None,
            id: upstream_id,
            name: format!("usage-poll-{kind:?}-{upstream_id}"),
            kind,
            base_url: Some(base_url),
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("create usage upstream");
    let now = cc_lb_clock::unix_secs(fixture.clock.now());
    let encrypted = EncryptedOAuthTokens::encrypt(
        fixture.aead.as_ref(),
        &OAuthTokenBundle {
            access_token: access_token.to_owned(),
            refresh_token: "poll-refresh-token".to_owned(),
            expires_at_unix_secs: now + 3600,
            refresh_token_expires_at_unix_secs: None,
            account_id: (kind == UpstreamKind::OpenaiOauth)
                .then(|| "usage-poll-account".to_owned()),
            project_id: (kind == UpstreamKind::GeminiOauth)
                .then(|| "usage-poll-project".to_owned()),
            oauth_profile: profile,
            scopes: vec!["messages".to_owned()],
            never_refresh: false,
        },
        created.id.as_bytes(),
    )
    .expect("encrypt usage bundle");
    UpstreamStore::store_oauth_tokens(
        fixture.storage.as_ref(),
        created.id,
        created.revision,
        encrypted,
        false,
    )
    .await
    .expect("store usage tokens")
}

fn success_usage_body(kind: UpstreamKind, path: &str) -> serde_json::Value {
    match kind {
        UpstreamKind::AnthropicOauth => serde_json::json!({
            "five_hour": {"utilization": 15.0, "resets_at": "2026-09-24T20:00:00Z"}
        }),
        UpstreamKind::OpenaiOauth => serde_json::json!({
            "rate_limit": {
                "primary_window": {
                    "limit_window_seconds": 18000,
                    "used_percent": 12.0,
                    "reset_at": 1_700_018_000
                }
            }
        }),
        UpstreamKind::GeminiOauth => {
            if path.contains("retrieveUserQuotaSummary") {
                serde_json::json!({
                    "groups": [
                        {
                            "displayName": "Gemini",
                            "buckets": [{
                                "modelId": "gemini-2.5-flash",
                                "remainingFraction": 0.8,
                                "resetTime": "2026-09-24T20:00:00Z",
                                "period": "5h"
                            }]
                        },
                        {
                            "displayName": "Claude",
                            "buckets": [{
                                "modelId": "claude-sonnet-4",
                                "remainingFraction": 0.9,
                                "resetTime": "2026-09-24T20:00:00Z",
                                "period": "5h"
                            }]
                        }
                    ]
                })
            } else {
                serde_json::json!({
                    "buckets": [{
                        "modelId": "gemini-2.5-flash",
                        "remainingFraction": 0.8,
                        "resetTime": "2026-09-24T20:00:00Z"
                    }]
                })
            }
        }
        other => panic!("unexpected usage upstream kind {other:?}"),
    }
}

async fn spawn_usage_sequence_server(
    kind: UpstreamKind,
    statuses: Vec<StatusCode>,
    gemini_fallback_only: bool,
) -> (
    Url,
    Arc<std::sync::Mutex<Vec<(String, Option<String>)>>>,
    tokio::task::JoinHandle<()>,
) {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let queue = Arc::new(std::sync::Mutex::new(std::collections::VecDeque::from(
        statuses,
    )));
    let recorded = seen.clone();
    let app = axum::Router::new().fallback(move |request: axum::extract::Request| {
        let recorded = recorded.clone();
        let queue = queue.clone();
        async move {
            let path = request.uri().path().to_owned();
            if path == "/userinfo" {
                return (StatusCode::OK, axum::Json(serde_json::json!({})));
            }
            let auth = request
                .headers()
                .get(http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            recorded
                .lock()
                .expect("usage seen lock")
                .push((path.clone(), auth));
            if gemini_fallback_only && path.contains("retrieveUserQuotaSummary") {
                return (
                    StatusCode::OK,
                    axum::Json(serde_json::json!({ "groups": [] })),
                );
            }
            let status = queue
                .lock()
                .expect("usage queue lock")
                .pop_front()
                .unwrap_or(StatusCode::OK);
            let body = if status.is_success() {
                success_usage_body(kind, &path)
            } else {
                serde_json::json!({ "error": "rejected" })
            };
            (status, axum::Json(body))
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind usage server");
    let url = Url::parse(&format!(
        "http://{}/",
        listener.local_addr().expect("usage server addr")
    ))
    .expect("usage server URL");
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve usage server");
    });
    (url, seen, server)
}

#[tokio::test]
async fn poll_usage_post_refresh_unauthorized_persists_status_401_and_skips_next_poll() {
    use cc_lb_oauth_protocol::GeminiOAuthProfile;
    use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsagePollObservation;

    for (kind, profile, gemini_fallback_only) in [
        (
            UpstreamKind::OpenaiOauth,
            GeminiOAuthProfile::GeminiCli,
            false,
        ),
        (
            UpstreamKind::GeminiOauth,
            GeminiOAuthProfile::GeminiCli,
            false,
        ),
        (
            UpstreamKind::GeminiOauth,
            GeminiOAuthProfile::Antigravity,
            true,
        ),
        (
            UpstreamKind::AnthropicOauth,
            GeminiOAuthProfile::GeminiCli,
            false,
        ),
    ] {
        let (base_url, seen, server) = spawn_usage_sequence_server(
            kind,
            vec![StatusCode::UNAUTHORIZED, StatusCode::UNAUTHORIZED],
            gemini_fallback_only,
        )
        .await;
        let mut fixture = DispatchFixture::new().await;
        configure_loopback_google(&mut fixture, &base_url, None);
        let refresh_calls = Arc::new(AtomicUsize::new(0));
        let claim_guard = Arc::new(CompleteRefreshClaimGuard {
            storage: fixture.storage.clone(),
            aead: fixture.aead.clone(),
            replacement_access_token: "refreshed-usage-token".to_owned(),
            calls: refresh_calls.clone(),
        });
        install_lazy_refresher(&mut fixture, claim_guard);
        let upstream =
            create_usage_poll_upstream(&fixture, kind, profile, base_url, "stale-usage-token")
                .await;

        let observation = fixture
            .dispatch
            .poll_usage(upstream.id, None)
            .await
            .expect("poll finishes");
        assert!(
            matches!(
                observation,
                OAuthUsagePollObservation::StatusFailure { status: 401, .. }
            ),
            "{kind:?}/{profile:?}: expected StatusFailure(401), got {observation:?}"
        );
        assert_eq!(refresh_calls.load(Ordering::SeqCst), 1);

        let recorded = seen.lock().expect("seen lock").clone();
        let auths: Vec<_> = recorded
            .iter()
            .filter_map(|(_, auth)| auth.as_deref())
            .collect();
        assert!(
            auths.contains(&"Bearer stale-usage-token")
                && auths.contains(&"Bearer refreshed-usage-token"),
            "{kind:?}: expected stale then refreshed auth headers, got {recorded:?}"
        );

        let stored = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
            .await
            .expect("upstream fetch")
            .expect("upstream exists");
        assert_eq!(stored.last_apply_error.as_deref(), Some("status_401"));
        assert_eq!(
            stored.oauth_token_generation,
            upstream.oauth_token_generation + 1
        );

        let second = fixture
            .dispatch
            .poll_usage(upstream.id, None)
            .await
            .expect("second poll skips");
        assert!(matches!(second, OAuthUsagePollObservation::Skip));
        assert_eq!(refresh_calls.load(Ordering::SeqCst), 1);
        server.abort();
    }
}

#[tokio::test]
async fn poll_usage_post_refresh_200_and_403_leave_last_apply_error_clear() {
    use cc_lb_oauth_protocol::GeminiOAuthProfile;
    use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsagePollObservation;

    for (kind, profile) in [
        (UpstreamKind::OpenaiOauth, GeminiOAuthProfile::GeminiCli),
        (UpstreamKind::GeminiOauth, GeminiOAuthProfile::Antigravity),
        (UpstreamKind::AnthropicOauth, GeminiOAuthProfile::GeminiCli),
    ] {
        for retry_status in [StatusCode::OK, StatusCode::FORBIDDEN] {
            let statuses =
                if kind == UpstreamKind::GeminiOauth && retry_status == StatusCode::FORBIDDEN {
                    vec![
                        StatusCode::UNAUTHORIZED,
                        StatusCode::FORBIDDEN,
                        StatusCode::FORBIDDEN,
                    ]
                } else {
                    vec![StatusCode::UNAUTHORIZED, retry_status]
                };
            let (base_url, _, server) = spawn_usage_sequence_server(kind, statuses, false).await;
            let mut fixture = DispatchFixture::new().await;
            configure_loopback_google(&mut fixture, &base_url, None);
            let refresh_calls = Arc::new(AtomicUsize::new(0));
            let claim_guard = Arc::new(CompleteRefreshClaimGuard {
                storage: fixture.storage.clone(),
                aead: fixture.aead.clone(),
                replacement_access_token: "refreshed-usage-token".to_owned(),
                calls: refresh_calls.clone(),
            });
            install_lazy_refresher(&mut fixture, claim_guard);
            let upstream =
                create_usage_poll_upstream(&fixture, kind, profile, base_url, "stale-usage-token")
                    .await;

            let observation = fixture
                .dispatch
                .poll_usage(upstream.id, None)
                .await
                .expect("poll finishes");
            if retry_status == StatusCode::OK {
                assert!(
                    matches!(observation, OAuthUsagePollObservation::Success { .. }),
                    "{kind:?}: expected Success, got {observation:?}"
                );
            } else {
                assert!(
                    matches!(
                        observation,
                        OAuthUsagePollObservation::StatusFailure { status: 403, .. }
                    ),
                    "{kind:?}: expected StatusFailure(403), got {observation:?}"
                );
            }
            assert_eq!(refresh_calls.load(Ordering::SeqCst), 1);
            let stored = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
                .await
                .expect("upstream fetch")
                .expect("upstream exists");
            assert_eq!(stored.last_apply_error, None);
            server.abort();
        }
    }
}

#[tokio::test]
async fn poll_usage_post_refresh_generation_guard_ignores_concurrent_reconnect() {
    use cc_lb_oauth_protocol::GeminiOAuthProfile;

    for (kind, profile) in [
        (UpstreamKind::OpenaiOauth, GeminiOAuthProfile::GeminiCli),
        (UpstreamKind::GeminiOauth, GeminiOAuthProfile::Antigravity),
        (UpstreamKind::AnthropicOauth, GeminiOAuthProfile::GeminiCli),
    ] {
        let mut fixture = DispatchFixture::new().await;
        let base_placeholder = Url::parse("http://127.0.0.1:9/").expect("placeholder url");
        let upstream = create_usage_poll_upstream(
            &fixture,
            kind,
            profile,
            base_placeholder,
            "stale-usage-token",
        )
        .await;
        let upstream_id = upstream.id;
        let storage_for_hook = fixture.storage.clone();
        let aead_for_hook = fixture.aead.clone();
        let now = cc_lb_clock::unix_secs(fixture.clock.now());
        let calls = Arc::new(AtomicUsize::new(0));
        let hook_calls = calls.clone();
        let app = axum::Router::new().fallback(move || {
            let hook_calls = hook_calls.clone();
            let storage = storage_for_hook.clone();
            let aead = aead_for_hook.clone();
            async move {
                let attempt = hook_calls.fetch_add(1, Ordering::SeqCst);
                if attempt == 1 {
                    let current = UpstreamStore::get_by_id(storage.as_ref(), upstream_id)
                        .await
                        .expect("read upstream")
                        .expect("upstream exists");
                    let encrypted = EncryptedOAuthTokens::encrypt(
                        aead.as_ref(),
                        &OAuthTokenBundle {
                            access_token: "reconnected-usage-token".to_owned(),
                            refresh_token: "reconnected-refresh".to_owned(),
                            expires_at_unix_secs: now + 7200,
                            refresh_token_expires_at_unix_secs: None,
                            account_id: Some("usage-poll-account".to_owned()),
                            project_id: Some("usage-poll-project".to_owned()),
                            oauth_profile: profile,
                            scopes: vec!["messages".to_owned()],
                            never_refresh: false,
                        },
                        upstream_id.as_bytes(),
                    )
                    .expect("encrypt replacement");
                    UpstreamStore::store_oauth_tokens(
                        storage.as_ref(),
                        upstream_id,
                        current.revision,
                        encrypted,
                        false,
                    )
                    .await
                    .expect("reconnect commits before retry 401 arrives");
                }
                (
                    StatusCode::UNAUTHORIZED,
                    axum::Json(serde_json::json!({"error":"rejected"})),
                )
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind race usage server");
        let base_url = Url::parse(&format!(
            "http://{}/",
            listener.local_addr().expect("race usage addr")
        ))
        .expect("race usage URL");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve race usage");
        });

        UpstreamStore::update(
            fixture.storage.as_ref(),
            upstream_id,
            upstream.revision,
            UpstreamUpdate {
                base_url: Some(Some(base_url.clone())),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("set base_url");
        configure_loopback_google(&mut fixture, &base_url, None);
        let claim_guard = Arc::new(CompleteRefreshClaimGuard {
            storage: fixture.storage.clone(),
            aead: fixture.aead.clone(),
            replacement_access_token: "refreshed-usage-token".to_owned(),
            calls: Arc::new(AtomicUsize::new(0)),
        });
        install_lazy_refresher(&mut fixture, claim_guard);

        let _ = fixture
            .dispatch
            .poll_usage(upstream_id, None)
            .await
            .expect("poll finishes");
        let latest = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await
            .expect("upstream fetch")
            .expect("upstream exists");
        assert_eq!(latest.oauth_token_generation, 3);
        assert_eq!(latest.last_apply_error, None);
        server.abort();
    }
}

#[tokio::test]
async fn poll_usage_post_refresh_401_via_http_token_exchange_records_status_401() {
    use cc_lb_oauth_protocol::GeminiOAuthProfile;
    use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsagePollObservation;

    for (kind, profile) in [
        (UpstreamKind::AnthropicOauth, GeminiOAuthProfile::GeminiCli),
        (UpstreamKind::GeminiOauth, GeminiOAuthProfile::Antigravity),
    ] {
        let (token_url, token_calls, token_srv) = token_server(
            StatusCode::OK,
            r#"{"access_token":"http-refreshed-usage-token","token_type":"Bearer","expires_in":3600}"#,
        )
        .await;
        let (usage_url, seen, usage_srv) = spawn_usage_sequence_server(
            kind,
            vec![StatusCode::UNAUTHORIZED, StatusCode::UNAUTHORIZED],
            false,
        )
        .await;
        let mut fixture = DispatchFixture::new().await;
        set_token_endpoint(&mut fixture, token_url.clone());
        configure_loopback_google(&mut fixture, &usage_url, Some(&token_url));
        let claim_calls = Arc::new(AtomicUsize::new(0));
        install_lazy_refresher(
            &mut fixture,
            Arc::new(AcquiredClaimGuard {
                calls: claim_calls.clone(),
            }),
        );
        let upstream =
            create_usage_poll_upstream(&fixture, kind, profile, usage_url, "initial-usage-token")
                .await;

        let observation = fixture
            .dispatch
            .poll_usage(upstream.id, None)
            .await
            .expect("poll finishes");
        assert!(matches!(
            observation,
            OAuthUsagePollObservation::StatusFailure { status: 401, .. }
        ));
        assert_eq!(claim_calls.load(Ordering::SeqCst), 1);
        assert_eq!(token_calls.load(Ordering::SeqCst), 1);
        let recorded = seen.lock().expect("seen lock").clone();
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0].1.as_deref(), Some("Bearer initial-usage-token"));
        assert_eq!(
            recorded[1].1.as_deref(),
            Some("Bearer http-refreshed-usage-token")
        );
        let stored = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
            .await
            .expect("upstream fetch")
            .expect("upstream exists");
        assert_eq!(stored.last_apply_error.as_deref(), Some("status_401"));

        usage_srv.abort();
        token_srv.abort();
    }
}

#[tokio::test]
async fn scheduled_oauth_refresh_retries_without_calling_provider_while_lease_is_held() {
    use cc_lb_storage_api::MetaStore;

    let (endpoint, calls, server) = token_server(
        StatusCode::OK,
        r#"{"access_token":"leased-access","refresh_token":"leased-refresh","token_type":"Bearer","expires_in":3600}"#,
    )
    .await;
    let mut fixture = DispatchFixture::new().await;
    set_token_endpoint(&mut fixture, endpoint);
    let upstream = fixture.oauth_upstream(false).await;
    let now = cc_lb_clock::unix_secs(fixture.clock.now());
    let lease = crate::refresh::try_acquire_oauth_refresh_lease(
        fixture.storage.as_ref(),
        upstream.id,
        upstream.oauth_token_generation,
        now,
    )
    .await
    .expect("lease lookup")
    .expect("acquire external lease");

    let contended = fixture
        .dispatch
        .dispatch_oauth_refresh(OAuthRefreshJob::new(upstream.id))
        .await
        .expect("contended dispatch returns Retry");
    assert!(matches!(contended, JobOutcome::Retry { .. }));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "scheduled dispatch must not contact /oauth/token while fallback lease is live"
    );

    crate::refresh::release_oauth_refresh_lease(
        fixture.storage.as_ref(),
        upstream.id,
        &lease.token,
        upstream.oauth_token_generation,
    )
    .await
    .expect("release lease");

    let completed = fixture
        .dispatch
        .dispatch_oauth_refresh(OAuthRefreshJob::new(upstream.id))
        .await
        .expect("uncontended dispatch completes");
    assert_eq!(completed, JobOutcome::Done);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let refreshed = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
        .await
        .expect("load refreshed")
        .expect("upstream exists");
    assert_eq!(
        refreshed.oauth_token_generation,
        upstream.oauth_token_generation + 1
    );
    let expected_idle = format!("idle:{}", refreshed.oauth_token_generation);
    let lease_value = fixture
        .storage
        .get_meta_value(&format!("oauth_refresh_lease:v1:{}", upstream.id))
        .await
        .expect("read released lease");
    assert_eq!(lease_value.as_deref(), Some(expected_idle.as_str()));

    server.abort();
}
