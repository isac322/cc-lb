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
        let expected_retry_delay = if status.is_server_error() {
            std::time::Duration::from_secs(crate::refresh::OAUTH_REFRESH_LEASE_TTL_SECS + 1)
        } else {
            std::time::Duration::from_secs(30)
        };
        assert_eq!(
            fixture
                .dispatch
                .dispatch_oauth_refresh(OAuthRefreshJob::new(upstream.id))
                .await
                .expect("retryable response"),
            JobOutcome::Retry {
                delay: Some(expected_retry_delay),
            }
        );
        if status.is_server_error() {
            // Ambiguous 5xx responses keep the held lease until TTL expiry and defer retries past it.
            assert_eq!(
                fixture
                    .dispatch
                    .dispatch_oauth_refresh(OAuthRefreshJob::new(upstream.id))
                    .await
                    .expect("held 5xx lease retries after expiry without calling provider"),
                JobOutcome::Retry {
                    delay: Some(expected_retry_delay),
                }
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            fixture
                .clock
                .advance_secs(crate::refresh::OAUTH_REFRESH_LEASE_TTL_SECS + 1);
        }
        assert_eq!(
            fixture
                .dispatch
                .dispatch_oauth_refresh(OAuthRefreshJob::new(upstream.id))
                .await
                .expect("retryable response after rejection or lease expiry"),
            JobOutcome::Retry {
                delay: Some(expected_retry_delay),
            }
        );
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
            delay: Some(std::time::Duration::from_secs(
                crate::refresh::OAUTH_REFRESH_LEASE_TTL_SECS + 1,
            )),
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

#[tokio::test]
async fn obsolete_oauth_refresh_job_skips_after_fallback_rotates_token_and_preserves_successor() {
    let (endpoint, calls, server) = token_server(
        StatusCode::OK,
        r#"{"access_token":"late-access","refresh_token":"late-refresh","token_type":"Bearer","expires_in":3600}"#,
    )
    .await;
    let mut fixture = DispatchFixture::new().await;
    set_token_endpoint(&mut fixture, endpoint);
    let upstream = fixture.oauth_upstream(false).await;
    let original_generation = upstream.oauth_token_generation;
    let obsolete_job = OAuthRefreshJob::for_credential(upstream.id, original_generation, 1);

    // Simulate lazy fallback advancing N -> N+1 with a newer expiry before the
    // previously pending worker job resumes and executes.
    let replacement = EncryptedOAuthTokens::encrypt(
        fixture.dispatch.aead.as_ref(),
        &OAuthTokenBundle {
            oauth_profile: Default::default(),
            access_token: "fallback-access".to_owned(),
            refresh_token: "fallback-refresh".to_owned(),
            expires_at_unix_secs: 9_999_999_999,
            refresh_token_expires_at_unix_secs: None,
            account_id: None,
            project_id: None,
            scopes: vec!["messages".to_owned()],
            never_refresh: false,
        },
        upstream.id.as_bytes(),
    )
    .expect("encrypt replacement");
    let rotated = UpstreamStore::complete_refresh(
        fixture.storage.as_ref(),
        upstream.id,
        Uuid::new_v4(),
        replacement,
    )
    .await
    .expect("fallback rotates credential");
    assert_eq!(rotated.oauth_token_generation, original_generation + 1);

    let outcome = fixture
        .dispatch
        .dispatch_oauth_refresh(obsolete_job)
        .await
        .expect("obsolete job dispatch");
    assert_eq!(outcome, JobOutcome::Skip);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "obsolete job must never exchange the replacement credential",
    );

    let current = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
        .await
        .expect("reload upstream")
        .expect("upstream present");
    assert_eq!(
        current.oauth_token_generation,
        original_generation + 1,
        "obsolete job must leave the replacement generation untouched",
    );
    server.abort();
}

#[tokio::test]
async fn scheduled_oauth_refresh_rejects_stale_lease_after_generation_advances() {
    use cc_lb_storage_api::MetaStore;

    let (endpoint, calls, server) = token_server(
        StatusCode::OK,
        r#"{"access_token":"stale-access","refresh_token":"stale-refresh","token_type":"Bearer","expires_in":3600}"#,
    )
    .await;
    let mut fixture = DispatchFixture::new().await;
    set_token_endpoint(&mut fixture, endpoint);
    let upstream = fixture.oauth_upstream(false).await;
    let old_gen = upstream.oauth_token_generation;
    let now = cc_lb_clock::unix_secs(fixture.clock.now());
    let lease_key = crate::refresh::oauth_refresh_lease_meta_key(upstream.id);

    // Another runner committed N+1 and published `idle:{N+1}`.
    let advanced_idle = format!("idle:{}", old_gen + 1);
    fixture
        .storage
        .put_meta_value(&lease_key, &advanced_idle)
        .await
        .expect("seed idle:N+1");

    // A stale lease claim for `old_gen` must be rejected and leave `idle:{N+1}` intact.
    let stale_attempt = crate::refresh::try_acquire_oauth_refresh_lease(
        fixture.storage.as_ref(),
        upstream.id,
        old_gen,
        now,
    )
    .await
    .expect("stale lease attempt");
    assert!(
        stale_attempt.is_none(),
        "stale lease generation N must not overwrite idle:(N+1)",
    );
    assert_eq!(
        fixture
            .storage
            .get_meta_value(&lease_key)
            .await
            .expect("read meta")
            .as_deref(),
        Some(advanced_idle.as_str())
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    // Dispatching a legacy/generation-unbound job after `idle:{N+1}` also fails
    // to steal ownership for generation `old_gen` and skips once generation > old_gen,
    // OR completes only if uncontended at N+1. Specifically verify an N-bound job
    // skips cleanly with 0 token calls even if metadata says `idle:N+1`.
    let replacement = EncryptedOAuthTokens::encrypt(
        fixture.dispatch.aead.as_ref(),
        &OAuthTokenBundle {
            oauth_profile: Default::default(),
            access_token: "fresh-access".to_owned(),
            refresh_token: "fresh-refresh".to_owned(),
            expires_at_unix_secs: 9_999_999_999,
            refresh_token_expires_at_unix_secs: None,
            account_id: None,
            project_id: None,
            scopes: vec!["messages".to_owned()],
            never_refresh: false,
        },
        upstream.id.as_bytes(),
    )
    .expect("encrypt fresh");
    UpstreamStore::complete_refresh(
        fixture.storage.as_ref(),
        upstream.id,
        Uuid::new_v4(),
        replacement,
    )
    .await
    .expect("commit generation N+1");

    let stale_job = OAuthRefreshJob::with_expected_generation(upstream.id, old_gen);
    let outcome = fixture
        .dispatch
        .dispatch_oauth_refresh(stale_job)
        .await
        .expect("stale job dispatch");
    assert_eq!(outcome, JobOutcome::Skip);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture
            .storage
            .get_meta_value(&lease_key)
            .await
            .expect("read meta after skip")
            .as_deref(),
        Some(advanced_idle.as_str())
    );
    server.abort();
}

#[tokio::test]
async fn legacy_untagged_oauth_refresh_job_retires_and_reconciles_after_fallback_rotates_token() {
    let (endpoint, calls, server) = token_server(
        StatusCode::OK,
        r#"{"access_token":"legacy-late","refresh_token":"legacy-refresh","token_type":"Bearer","expires_in":3600}"#,
    )
    .await;
    let mut fixture = DispatchFixture::new().await;
    set_token_endpoint(&mut fixture, endpoint);
    let upstream = fixture.oauth_upstream(false).await;
    let now = cc_lb_clock::unix_secs(fixture.clock.now());
    let new_expiry = now + 7_200;

    // Simulate lazy fallback having rotated N -> N+1 to a fresh token (due at new_expiry - 300),
    // WITHOUT having enqueued the N+1 successor yet (or while a persisted pre-upgrade untagged job
    // without `expected_generation` / `expected_expires_at_unix_secs` remained Pending).
    let replacement = EncryptedOAuthTokens::encrypt(
        fixture.dispatch.aead.as_ref(),
        &OAuthTokenBundle {
            oauth_profile: Default::default(),
            access_token: "fallback-fresh-access".to_owned(),
            refresh_token: "fallback-fresh-refresh".to_owned(),
            expires_at_unix_secs: new_expiry,
            refresh_token_expires_at_unix_secs: None,
            account_id: None,
            project_id: None,
            scopes: vec!["messages".to_owned()],
            never_refresh: false,
        },
        upstream.id.as_bytes(),
    )
    .expect("encrypt fresh fallback token");
    let rotated = UpstreamStore::complete_refresh(
        fixture.storage.as_ref(),
        upstream.id,
        Uuid::new_v4(),
        replacement,
    )
    .await
    .expect("fallback rotates token");
    let new_gen = rotated.oauth_token_generation;

    // Deserialize a persisted pre-upgrade payload with no expected fields.
    let raw_legacy = serde_json::json!({
        "upstream_id": upstream.id,
        "traceparent": null
    });
    let legacy_job: OAuthRefreshJob =
        serde_json::from_value(raw_legacy).expect("deserialize legacy untagged job");
    assert_eq!(legacy_job.expected_generation, None);
    assert_eq!(legacy_job.expected_expires_at_unix_secs, None);

    let outcome = fixture
        .dispatch
        .dispatch_oauth_refresh(legacy_job.clone())
        .await
        .expect("legacy job retires cleanly");
    assert_eq!(outcome, JobOutcome::Skip);
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    // Canonical successor for N+1 was reconciled!
    let expected_key = legacy_job.generation_idempotency_key(new_gen, new_expiry);
    let pending = fixture
        .dispatch
        .backend
        .list_adaptive_tasks(&Filter {
            status: Some(TaskStatus::Pending),
            page: 1,
            page_size: Some(50),
        })
        .await
        .expect("list pending");
    assert!(
        pending.iter().any(|task| {
            task.idempotency_key.as_deref() == Some(expected_key.as_str())
                && matches!(
                    &task.args,
                    AdaptiveJob::OAuthRefresh(j)
                        if j.expected_generation == Some(new_gen)
                            && j.expected_expires_at_unix_secs == Some(new_expiry)
                )
        }),
        "legacy retirement must reconcile canonical generation-tagged successor"
    );

    // Even when a credential later becomes due, an active canonical generation-tagged job
    // still causes a legacy untagged job to retire without calling `/oauth/token`.
    fixture.clock.advance_secs(7_100);
    let second_outcome = fixture
        .dispatch
        .dispatch_oauth_refresh(legacy_job)
        .await
        .expect("legacy job covered by tagged successor retries/retires");
    assert_eq!(second_outcome, JobOutcome::Skip);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    server.abort();
}

#[tokio::test]
async fn superseded_oauth_refresh_job_repairs_missing_metadata_and_successor_without_provider_call()
{
    let (endpoint, calls, server) = token_server(
        StatusCode::OK,
        r#"{"access_token":"never-called","refresh_token":"never-called","token_type":"Bearer","expires_in":3600}"#,
    )
    .await;
    let mut fixture = DispatchFixture::new().await;
    set_token_endpoint(&mut fixture, endpoint);
    let upstream = fixture.oauth_upstream(false).await;
    let old_gen = upstream.oauth_token_generation;
    let now = cc_lb_clock::unix_secs(fixture.clock.now());
    let new_expiry = now + 5_000;

    // Commit generation N+1 as if token persistence succeeded right before metadata/successor
    // enqueue failed. Neither MetadataRefreshJob(N+1) nor OAuthRefreshJob(N+1) exists yet.
    let replacement = EncryptedOAuthTokens::encrypt(
        fixture.dispatch.aead.as_ref(),
        &OAuthTokenBundle {
            oauth_profile: Default::default(),
            access_token: "committed-access".to_owned(),
            refresh_token: "committed-refresh".to_owned(),
            expires_at_unix_secs: new_expiry,
            refresh_token_expires_at_unix_secs: None,
            account_id: None,
            project_id: None,
            scopes: vec!["messages".to_owned()],
            never_refresh: false,
        },
        upstream.id.as_bytes(),
    )
    .expect("encrypt committed tokens");
    let committed = UpstreamStore::complete_refresh(
        fixture.storage.as_ref(),
        upstream.id,
        Uuid::new_v4(),
        replacement,
    )
    .await
    .expect("commit generation N+1");
    let new_gen = committed.oauth_token_generation;

    let superseded_job = OAuthRefreshJob::for_credential(upstream.id, old_gen, 1);
    let outcome = fixture
        .dispatch
        .dispatch_oauth_refresh(superseded_job)
        .await
        .expect("superseded job repairs post-commit work");
    assert_eq!(outcome, JobOutcome::Skip);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "repairing missing post-commit work must not exchange the token again",
    );

    let pending = fixture
        .dispatch
        .backend
        .list_adaptive_tasks(&Filter {
            status: Some(TaskStatus::Pending),
            page: 1,
            page_size: Some(50),
        })
        .await
        .expect("list pending after repair");
    let expected_oauth_key =
        OAuthRefreshJob::new(upstream.id).generation_idempotency_key(new_gen, new_expiry);
    assert!(
        pending.iter().any(|task| {
            task.idempotency_key.as_deref() == Some(expected_oauth_key.as_str())
                && matches!(
                    &task.args,
                    AdaptiveJob::OAuthRefresh(j)
                        if j.expected_generation == Some(new_gen)
                            && j.expected_expires_at_unix_secs == Some(new_expiry)
                )
        }),
        "missing successor OAuthRefreshJob must be repaired"
    );
    assert!(
        pending.iter().any(|task| matches!(
            &task.args,
            AdaptiveJob::MetadataRefresh(m)
                if m.upstream_id == upstream.id && m.credential_generation == new_gen
        )),
        "missing MetadataRefreshJob for Anthropic OAuth generation N+1 must be repaired"
    );
    server.abort();
}

#[tokio::test]
async fn oauth_refresh_watchdog_reconciles_anthropic_metadata_even_when_refresh_job_already_exists()
{
    use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
    use cc_lb_scheduler::jobs::watchdog::OAuthRefreshWatchdogJob;
    use cc_lb_scheduler::worker::{CronJob, SchedulerBackend};

    let fixture = DispatchFixture::new().await;
    let upstream = fixture.oauth_upstream(false).await;
    let generation = upstream.oauth_token_generation;
    let expires_at =
        crate::scheduler_dispatch::http::decrypt_bundle(&upstream, fixture.dispatch.aead.as_ref())
            .expect("decrypt bundle")
            .expires_at_unix_secs;

    // Seed the canonical OAuthRefreshJob for the current generation BEFORE running the watchdog,
    // leaving MetadataRefreshJob missing (as if metadata enqueue failed on the prior run).
    let refresh_job = OAuthRefreshJob::for_credential(upstream.id, generation, expires_at);
    let refresh_key = refresh_job.generation_idempotency_key(generation, expires_at);
    fixture
        .dispatch
        .backend
        .push_adaptive_task(SchedulerPushTask {
            args: AdaptiveJob::OAuthRefresh(refresh_job),
            idempotency_key: Some(refresh_key),
            run_at_unix_secs: Some(OAuthRefreshJob::run_at_for_expires_at(expires_at)),
            max_attempts: None,
        })
        .await
        .expect("seed active current-generation refresh job");

    let tick1 = cc_lb_clock::unix_secs(fixture.clock.now());
    let outcome1 = fixture
        .dispatch
        .dispatch_singleton(CronJob::OAuthRefreshWatchdog(OAuthRefreshWatchdogJob::new(
            tick1,
        )))
        .await
        .expect("watchdog reconciles metadata even when refresh job exists");
    assert_eq!(outcome1, JobOutcome::Done);

    let metadata_job = MetadataRefreshJob::new(upstream.id, generation);
    let canonical_meta_key = metadata_job.idempotency_key();
    let pending = fixture
        .dispatch
        .backend
        .list_adaptive_tasks(&Filter {
            status: Some(TaskStatus::Pending),
            page: 1,
            page_size: Some(50),
        })
        .await
        .expect("list pending after first watchdog tick");
    assert!(
        pending.iter().any(|task| {
            task.idempotency_key.as_deref() == Some(canonical_meta_key.as_str())
                && matches!(
                    &task.args,
                    AdaptiveJob::MetadataRefresh(m)
                        if m.upstream_id == upstream.id && m.credential_generation == generation
                )
        }),
        "watchdog must enqueue missing MetadataRefreshJob even though OAuthRefreshJob is already active"
    );

    #[allow(irrefutable_let_patterns)]
    let SchedulerBackend::Sqlite(sqlite) = &fixture.dispatch.backend else {
        panic!("expected sqlite backend");
    };

    // Once MetadataRefreshJob is Done, a subsequent watchdog tick must NOT duplicate it.
    sqlx::query("UPDATE Jobs SET status = 'Done', done_at = ?1 WHERE idempotency_key = ?2")
        .bind(i64::try_from(tick1).expect("tick1 fits i64"))
        .bind(&canonical_meta_key)
        .execute(sqlite.pool())
        .await
        .expect("mark canonical metadata job Done");

    let outcome2 = fixture
        .dispatch
        .dispatch_singleton(CronJob::OAuthRefreshWatchdog(OAuthRefreshWatchdogJob::new(
            tick1 + 100,
        )))
        .await
        .expect("watchdog after Done metadata");
    assert_eq!(outcome2, JobOutcome::Done);

    let meta_rows_after_done: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE idempotency_key LIKE ?1")
            .bind(format!("{canonical_meta_key}%"))
            .fetch_one(sqlite.pool())
            .await
            .expect("count metadata rows after Done");
    assert_eq!(meta_rows_after_done, 1);

    // If the canonical MetadataRefreshJob is Killed/exhausted, the watchdog seeds a distinct
    // recovery metadata job for the current generation.
    sqlx::query("UPDATE Jobs SET status = 'Killed' WHERE idempotency_key = ?1")
        .bind(&canonical_meta_key)
        .execute(sqlite.pool())
        .await
        .expect("mark canonical metadata job Killed");

    let tick3 = tick1 + 200;
    let outcome3 = fixture
        .dispatch
        .dispatch_singleton(CronJob::OAuthRefreshWatchdog(OAuthRefreshWatchdogJob::new(
            tick3,
        )))
        .await
        .expect("watchdog recovers killed metadata job");
    assert_eq!(outcome3, JobOutcome::Done);

    let recovery_meta_key = format!("{canonical_meta_key}:watchdog:{tick3}");
    let recovery_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE idempotency_key = ?1")
            .bind(&recovery_meta_key)
            .fetch_one(sqlite.pool())
            .await
            .expect("count recovery metadata job");
    assert_eq!(recovery_count, 1);
}

async fn spawn_controlled_token_server(
    allow_response: tokio::sync::watch::Receiver<bool>,
    response_status: StatusCode,
    response_body: &'static str,
) -> (
    Url,
    Arc<AtomicUsize>,
    tokio::sync::watch::Receiver<bool>,
    tokio::sync::watch::Receiver<bool>,
    tokio::task::JoinHandle<()>,
) {
    let calls = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = tokio::sync::watch::channel(false);
    let (sent_tx, sent_rx) = tokio::sync::watch::channel(false);
    let recorded = Arc::clone(&calls);
    let app = axum::Router::new().route(
        "/oauth/token",
        axum::routing::post(move || {
            let recorded = Arc::clone(&recorded);
            let entered_tx = entered_tx.clone();
            let sent_tx = sent_tx.clone();
            let mut allow = allow_response.clone();
            async move {
                recorded.fetch_add(1, Ordering::SeqCst);
                entered_tx.send_replace(true);
                let _ = allow.wait_for(|&go| go).await;
                sent_tx.send_replace(true);
                (
                    response_status,
                    [("content-type", "application/json")],
                    response_body,
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind controlled token server");
    let endpoint = Url::parse(&format!(
        "http://{}/oauth/token",
        listener.local_addr().expect("token server address")
    ))
    .expect("token URL");
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve controlled token endpoint");
    });
    (endpoint, calls, entered_rx, sent_rx, task)
}

#[tokio::test]
async fn scheduled_oauth_refresh_timeout_after_provider_request_preserves_ownership_and_persists_rotation()
 {
    use tokio::sync::watch;

    const OK_ROTATED_BODY: &str = r#"{"access_token":"rotated-access","refresh_token":"rotated-refresh","token_type":"Bearer","expires_in":3600}"#;

    // Sub-case A: `spawn_leased_oauth_refresh` continues owning lease + completing fenced
    // rotation if the caller drops or times out while `/oauth/token` response is in flight.
    let (allow_tx1, allow_rx1) = watch::channel(false);
    let (endpoint1, calls1, mut entered_rx1, _sent_rx1, server1) =
        spawn_controlled_token_server(allow_rx1, StatusCode::OK, OK_ROTATED_BODY).await;
    let mut fixture1 = DispatchFixture::new().await;
    set_token_endpoint(&mut fixture1, endpoint1);
    let upstream1 = fixture1.oauth_upstream(false).await;
    let starting_gen1 = upstream1.oauth_token_generation;
    let lease_key1 = crate::refresh::oauth_refresh_lease_meta_key(upstream1.id);
    let now1 = cc_lb_clock::unix_secs(fixture1.clock.now());
    let lease1 = crate::refresh::try_acquire_oauth_refresh_lease_cancellation_safe(
        fixture1.storage.clone(),
        fixture1.storage.clone(),
        upstream1.id,
        starting_gen1,
        now1,
    )
    .await
    .expect("lease acquire succeeds")
    .expect("lease 1 is free");

    let exchange_handle1 = fixture1.dispatch.spawn_leased_oauth_refresh(
        OAuthRefreshJob::new(upstream1.id),
        lease1,
        fixture1.dispatch.replica_id.expect("replica id set"),
        starting_gen1,
        None,
    );
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        entered_rx1.wait_for(|&entered| entered),
    )
    .await
    .expect("provider request enters within 2s")
    .expect("entered watch stays open");

    let peer_outcome1 = fixture1
        .dispatch
        .dispatch_oauth_refresh(OAuthRefreshJob::new(upstream1.id))
        .await
        .expect("peer observes held lease and retries");
    assert!(matches!(peer_outcome1, JobOutcome::Retry { .. }));
    assert_eq!(calls1.load(Ordering::SeqCst), 1);
    let held_value1 = fixture1
        .storage
        .get_meta_value(&lease_key1)
        .await
        .expect("read meta")
        .expect("lease row exists");
    assert!(held_value1.starts_with(&format!("held:{starting_gen1}:")));

    allow_tx1.send_replace(true);
    let completed_outcome1 =
        tokio::time::timeout(std::time::Duration::from_secs(2), exchange_handle1)
            .await
            .expect("detached scheduled refresh completes within 2s")
            .expect("spawned task joins")
            .expect("detached scheduled refresh returns Done");
    assert_eq!(completed_outcome1, JobOutcome::Done);
    assert_eq!(calls1.load(Ordering::SeqCst), 1);

    let stored1 = UpstreamStore::get_by_id(fixture1.storage.as_ref(), upstream1.id)
        .await
        .expect("load rotated upstream")
        .expect("upstream exists");
    assert_eq!(stored1.oauth_token_generation, starting_gen1 + 1);
    let decrypted1 = stored1
        .oauth_credentials
        .as_ref()
        .expect("encrypted credentials present")
        .decrypt(fixture1.dispatch.aead.as_ref(), upstream1.id.as_bytes())
        .expect("decrypt rotated credentials");
    assert_eq!(decrypted1.access_token, "rotated-access");
    assert_eq!(decrypted1.refresh_token, "rotated-refresh");
    assert_eq!(
        fixture1
            .storage
            .get_meta_value(&lease_key1)
            .await
            .expect("read released meta")
            .as_deref(),
        Some(format!("idle:{}", starting_gen1 + 1).as_str())
    );
    server1.abort();

    // Sub-case B: Dropping `dispatch_oauth_refresh` mid-provider-request (simulating worker
    // ENTITY_TIMEOUT) leaves the lease held and blocks concurrent retries from re-sending.
    let (allow_tx2, allow_rx2) = watch::channel(false);
    let (endpoint2, calls2, mut entered_rx2, mut sent_rx2, server2) =
        spawn_controlled_token_server(allow_rx2, StatusCode::OK, OK_ROTATED_BODY).await;
    let mut fixture2 = DispatchFixture::new().await;
    set_token_endpoint(&mut fixture2, endpoint2);
    let upstream2 = fixture2.oauth_upstream(false).await;
    let starting_gen2 = upstream2.oauth_token_generation;
    let lease_key2 = crate::refresh::oauth_refresh_lease_meta_key(upstream2.id);

    tokio::select! {
        biased;
        outcome = fixture2.dispatch.dispatch_oauth_refresh(OAuthRefreshJob::new(upstream2.id)) => {
            panic!("paused dispatch should not finish before allow_response: {outcome:?}");
        }
        wait_res = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            entered_rx2.wait_for(|&entered| entered),
        ) => {
            wait_res
                .expect("provider request enters within 2s")
                .expect("entered watch stays open");
        }
    }

    let peer_outcome2 = fixture2
        .dispatch
        .dispatch_oauth_refresh(OAuthRefreshJob::new(upstream2.id))
        .await
        .expect("concurrent retry sees exclusive lease still held");
    assert!(matches!(peer_outcome2, JobOutcome::Retry { .. }));
    assert_eq!(calls2.load(Ordering::SeqCst), 1);
    let held_value2 = fixture2
        .storage
        .get_meta_value(&lease_key2)
        .await
        .expect("read meta")
        .expect("lease row exists");
    assert!(held_value2.starts_with(&format!("held:{starting_gen2}:")));

    allow_tx2.send_replace(true);
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        sent_rx2.wait_for(|&sent| sent),
    )
    .await
    .expect("provider response sent within 2s")
    .expect("sent watch stays open");
    drop(server2);
}

#[tokio::test]
async fn scheduled_oauth_refresh_retains_lease_when_shutdown_cancels_after_provider_accepts() {
    use tokio::sync::watch;

    // Sub-case A: Shutdown cancels `dispatch.cancel` while provider `/oauth/token` response is
    // still pending -> ambiguous exchange retains `held:...` lease until TTL expiry so a peer
    // replica cannot re-send the rotating refresh token.
    let (_allow_tx1, allow_rx1) = watch::channel(false);
    let (endpoint1, calls1, mut entered_rx1, _sent_rx1, server1) =
        spawn_controlled_token_server(allow_rx1, StatusCode::OK, r#"{"access_token":"rotated"}"#)
            .await;
    let mut fixture1 = DispatchFixture::new().await;
    set_token_endpoint(&mut fixture1, endpoint1);
    let upstream1 = fixture1.oauth_upstream(false).await;
    let starting_gen1 = upstream1.oauth_token_generation;
    let lease_key1 = crate::refresh::oauth_refresh_lease_meta_key(upstream1.id);
    let now1 = cc_lb_clock::unix_secs(fixture1.clock.now());
    let lease1 = crate::refresh::try_acquire_oauth_refresh_lease_cancellation_safe(
        fixture1.storage.clone(),
        fixture1.storage.clone(),
        upstream1.id,
        starting_gen1,
        now1,
    )
    .await
    .expect("lease acquire succeeds")
    .expect("lease 1 is free");

    let exchange_handle1 = fixture1.dispatch.spawn_leased_oauth_refresh(
        OAuthRefreshJob::new(upstream1.id),
        lease1,
        fixture1.dispatch.replica_id.expect("replica id set"),
        starting_gen1,
        None,
    );
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        entered_rx1.wait_for(|&entered| entered),
    )
    .await
    .expect("provider request enters within 2s")
    .expect("entered watch stays open");

    let held_before1 = fixture1
        .storage
        .get_meta_value(&lease_key1)
        .await
        .expect("read held meta")
        .expect("lease row exists");
    assert!(held_before1.starts_with(&format!("held:{starting_gen1}:")));

    fixture1.dispatch.cancel.cancel();

    let cancelled_outcome1 =
        tokio::time::timeout(std::time::Duration::from_secs(2), exchange_handle1)
            .await
            .expect("cancelled scheduled refresh completes within 2s")
            .expect("spawned task joins")
            .expect("cancelled refresh returns Retry instead of hard failing");
    assert_eq!(
        cancelled_outcome1,
        JobOutcome::Retry {
            delay: Some(std::time::Duration::from_secs(
                crate::refresh::OAUTH_REFRESH_LEASE_TTL_SECS + 1,
            )),
        }
    );
    assert_eq!(
        fixture1
            .storage
            .get_meta_value(&lease_key1)
            .await
            .expect("read retained meta")
            .as_deref(),
        Some(held_before1.as_str())
    );

    let second_claim = crate::refresh::try_acquire_oauth_refresh_lease_cancellation_safe(
        fixture1.storage.clone(),
        fixture1.storage.clone(),
        upstream1.id,
        starting_gen1,
        now1,
    )
    .await
    .expect("second claimant check succeeds");
    assert!(second_claim.is_none());
    assert_eq!(calls1.load(Ordering::SeqCst), 1);
    server1.abort();

    // Sub-case B: Gateway timeout (`504 Gateway Timeout`) leaves downstream uncertain whether
    // upstream rotated the refresh token, so the lease stays held until TTL expiry.
    let (_allow_tx2, allow_rx2) = watch::channel(true);
    let (endpoint2, calls2, _entered_rx2, mut sent_rx2, server2) = spawn_controlled_token_server(
        allow_rx2,
        StatusCode::GATEWAY_TIMEOUT,
        r#"{"error":"gateway_timeout"}"#,
    )
    .await;
    let mut fixture2 = DispatchFixture::new().await;
    set_token_endpoint(&mut fixture2, endpoint2);
    let upstream2 = fixture2.oauth_upstream(false).await;
    let starting_gen2 = upstream2.oauth_token_generation;
    let lease_key2 = crate::refresh::oauth_refresh_lease_meta_key(upstream2.id);
    let now2 = cc_lb_clock::unix_secs(fixture2.clock.now());
    let lease2 = crate::refresh::try_acquire_oauth_refresh_lease_cancellation_safe(
        fixture2.storage.clone(),
        fixture2.storage.clone(),
        upstream2.id,
        starting_gen2,
        now2,
    )
    .await
    .expect("lease acquire succeeds")
    .expect("lease 2 is free");

    let outcome2 = fixture2
        .dispatch
        .spawn_leased_oauth_refresh(
            OAuthRefreshJob::new(upstream2.id),
            lease2,
            fixture2.dispatch.replica_id.expect("replica id set"),
            starting_gen2,
            None,
        )
        .await
        .expect("spawned task joins")
        .expect("504 response maps to Retry");
    assert_eq!(
        outcome2,
        JobOutcome::Retry {
            delay: Some(std::time::Duration::from_secs(
                crate::refresh::OAUTH_REFRESH_LEASE_TTL_SECS + 1,
            )),
        }
    );
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        sent_rx2.wait_for(|&sent| sent),
    )
    .await
    .expect("504 response sent within 2s")
    .expect("sent watch stays open");
    assert_eq!(calls2.load(Ordering::SeqCst), 1);
    let held_after_504 = fixture2
        .storage
        .get_meta_value(&lease_key2)
        .await
        .expect("read retained 504 meta")
        .expect("504 lease row exists");
    assert!(held_after_504.starts_with(&format!("held:{starting_gen2}:")));
    assert!(
        crate::refresh::try_acquire_oauth_refresh_lease_cancellation_safe(
            fixture2.storage.clone(),
            fixture2.storage.clone(),
            upstream2.id,
            starting_gen2,
            now2,
        )
        .await
        .expect("check retained 504 claim")
        .is_none()
    );
    assert!(
        crate::refresh::try_acquire_oauth_refresh_lease_cancellation_safe(
            fixture2.storage.clone(),
            fixture2.storage.clone(),
            upstream2.id,
            starting_gen2,
            now2 + crate::refresh::OAUTH_REFRESH_LEASE_TTL_SECS + 1,
        )
        .await
        .expect("check claim after 504 TTL expiry")
        .is_some()
    );
    server2.abort();

    // Sub-case C: Conclusive OAuth 4xx rejections (`429`, `401`) prove no rotation occurred and
    // immediately release the lease back to `idle:{starting_gen}`.
    for (status, body, expected_outcome, expected_error) in [
        (
            StatusCode::TOO_MANY_REQUESTS,
            r#"{"error":"rate_limited"}"#,
            JobOutcome::Retry {
                delay: Some(std::time::Duration::from_secs(30)),
            },
            None,
        ),
        (
            StatusCode::UNAUTHORIZED,
            r#"{"error":"invalid_grant"}"#,
            JobOutcome::Skip,
            Some("status_401"),
        ),
    ] {
        let (_allow_tx, allow_rx) = watch::channel(true);
        let (endpoint, calls, _entered_rx, mut sent_rx, server) =
            spawn_controlled_token_server(allow_rx, status, body).await;
        let mut fixture = DispatchFixture::new().await;
        set_token_endpoint(&mut fixture, endpoint);
        let upstream = fixture.oauth_upstream(false).await;
        let starting_gen = upstream.oauth_token_generation;
        let lease_key = crate::refresh::oauth_refresh_lease_meta_key(upstream.id);
        let now = cc_lb_clock::unix_secs(fixture.clock.now());
        let lease = crate::refresh::try_acquire_oauth_refresh_lease_cancellation_safe(
            fixture.storage.clone(),
            fixture.storage.clone(),
            upstream.id,
            starting_gen,
            now,
        )
        .await
        .expect("lease acquire succeeds")
        .expect("lease is free");

        let outcome = fixture
            .dispatch
            .spawn_leased_oauth_refresh(
                OAuthRefreshJob::new(upstream.id),
                lease,
                fixture.dispatch.replica_id.expect("replica id set"),
                starting_gen,
                None,
            )
            .await
            .expect("spawned task joins")
            .expect("4xx response yields expected outcome");
        assert_eq!(outcome, expected_outcome);
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            sent_rx.wait_for(|&sent| sent),
        )
        .await
        .expect("4xx response sent within 2s")
        .expect("sent watch stays open");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture
                .storage
                .get_meta_value(&lease_key)
                .await
                .expect("read released meta")
                .as_deref(),
            Some(format!("idle:{starting_gen}").as_str())
        );
        assert!(
            crate::refresh::try_acquire_oauth_refresh_lease_cancellation_safe(
                fixture.storage.clone(),
                fixture.storage.clone(),
                upstream.id,
                starting_gen,
                now,
            )
            .await
            .expect("check immediate reacquisition")
            .is_some()
        );
        let stored = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
            .await
            .expect("load upstream")
            .expect("upstream exists");
        assert_eq!(stored.last_apply_error.as_deref(), expected_error);
        server.abort();
    }
}

#[tokio::test]
async fn scheduled_oauth_refresh_uses_persisted_lease_expiry_after_lost_renewal_ack() {
    use cc_lb_storage_api::MetaStore;
    use tokio::sync::watch;

    let (allow_tx, allow_rx) = watch::channel(false);
    let (endpoint, calls, mut entered_rx, _sent_rx, server) = spawn_controlled_token_server(
        allow_rx,
        StatusCode::GATEWAY_TIMEOUT,
        r#"{"error":"gateway_timeout"}"#,
    )
    .await;
    let mut fixture = DispatchFixture::new().await;
    set_token_endpoint(&mut fixture, endpoint);
    let upstream = fixture.oauth_upstream(false).await;
    let starting_gen = upstream.oauth_token_generation;
    let lease_key = crate::refresh::oauth_refresh_lease_meta_key(upstream.id);
    let now = cc_lb_clock::unix_secs(fixture.clock.now());
    let lease = crate::refresh::try_acquire_oauth_refresh_lease_cancellation_safe(
        fixture.storage.clone(),
        fixture.storage.clone(),
        upstream.id,
        starting_gen,
        now,
    )
    .await
    .expect("lease acquire succeeds")
    .expect("lease is free");

    let exchange_handle = fixture.dispatch.spawn_leased_oauth_refresh(
        OAuthRefreshJob::new(upstream.id),
        lease,
        fixture.dispatch.replica_id.expect("replica id set"),
        starting_gen,
        None,
    );
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        entered_rx.wait_for(|&entered| entered),
    )
    .await
    .expect("provider request enters within 2s")
    .expect("entered watch stays open");

    // Simulate a renewal committed in storage at now+115 whose acknowledgement and probe both
    // failed, leaving the local heartbeat with an earlier now+90 expiry.
    let initial_held = fixture
        .storage
        .get_meta_value(&lease_key)
        .await
        .expect("read initial lease")
        .expect("initial lease present");
    let parts: Vec<&str> = initial_held.split(':').collect();
    assert_eq!(parts.len(), 4);
    let renewed_expiry = now + 115;
    let renewed_held = format!("held:{}:{}:{renewed_expiry}", parts[1], parts[2]);
    fixture
        .storage
        .put_meta_value(&lease_key, &renewed_held)
        .await
        .expect("persist renewed expiry in storage");

    allow_tx.send_replace(true);
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), exchange_handle)
        .await
        .expect("scheduled refresh completes within 2s")
        .expect("spawned task joins")
        .expect("504 response maps to Retry");
    assert_eq!(
        outcome,
        JobOutcome::Retry {
            delay: Some(std::time::Duration::from_secs(116)),
        }
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .storage
            .get_meta_value(&lease_key)
            .await
            .expect("read retained renewed meta")
            .as_deref(),
        Some(renewed_held.as_str())
    );
    server.abort();
}

#[tokio::test]
async fn scheduled_antigravity_missing_config_releases_refresh_lease_immediately() {
    use cc_lb_oauth_protocol::GeminiOAuthProfile;
    use cc_lb_storage_api::MetaStore;

    let mut fixture = DispatchFixture::new().await;
    fixture.dispatch.replica_id = Some(Uuid::new_v4());
    let upstream_id = Uuid::new_v4();
    let created = UpstreamStore::create(
        fixture.storage.as_ref(),
        UpstreamCreate {
            vertex: None,
            vertex_credential_type: None,
            vertex_quota_project_id: None,
            id: upstream_id,
            name: format!("antigravity-missing-cfg-{upstream_id}"),
            kind: UpstreamKind::GeminiOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("create antigravity upstream");
    let encrypted = EncryptedOAuthTokens::encrypt(
        fixture.aead.as_ref(),
        &OAuthTokenBundle {
            access_token: "antigravity-old-access".to_owned(),
            refresh_token: "antigravity-refresh-token".to_owned(),
            expires_at_unix_secs: 1,
            refresh_token_expires_at_unix_secs: None,
            account_id: None,
            project_id: Some("antigravity-project".to_owned()),
            oauth_profile: GeminiOAuthProfile::Antigravity,
            scopes: Vec::new(),
            never_refresh: false,
        },
        created.id.as_bytes(),
    )
    .expect("encrypt antigravity bundle");
    let upstream = UpstreamStore::store_oauth_tokens(
        fixture.storage.as_ref(),
        created.id,
        created.revision,
        encrypted,
        false,
    )
    .await
    .expect("store antigravity tokens");
    let starting_gen = upstream.oauth_token_generation;
    let lease_key = crate::refresh::oauth_refresh_lease_meta_key(upstream.id);

    let denied_calls = Arc::new(AtomicUsize::new(0));
    let direct_err = crate::scheduler_dispatch::http::with_denied_token_refresh(
        denied_calls.clone(),
        fixture.dispatch.refresh_upstream(upstream.clone()),
    )
    .await
    .expect_err("missing antigravity config must fail before transport");
    assert!(
        direct_err
            .to_string()
            .contains("oauth_client_not_configured"),
        "expected oauth_client_not_configured error, got {direct_err}"
    );
    assert_eq!(
        denied_calls.load(Ordering::SeqCst),
        0,
        "pre-send missing config failure must not enter HTTP transport"
    );

    let outcome = fixture
        .dispatch
        .dispatch_oauth_refresh(OAuthRefreshJob::new(upstream.id))
        .await
        .expect("missing antigravity config returns retry");
    assert_eq!(
        outcome,
        JobOutcome::Retry {
            delay: Some(std::time::Duration::from_secs(30)),
        }
    );
    assert_eq!(
        fixture
            .storage
            .get_meta_value(&lease_key)
            .await
            .expect("read lease meta")
            .as_deref(),
        Some(format!("idle:{starting_gen}").as_str()),
        "pre-send missing config error must release the refresh lease immediately"
    );

    let now = cc_lb_clock::unix_secs(fixture.clock.now());
    let reacquired = crate::refresh::try_acquire_oauth_refresh_lease_cancellation_safe(
        fixture.storage.clone(),
        fixture.storage.clone(),
        upstream.id,
        starting_gen,
        now,
    )
    .await
    .expect("immediate reacquisition check")
    .expect("lease must be immediately reacquirable at the same timestamp");
    crate::refresh::release_oauth_refresh_lease(
        fixture.storage.as_ref(),
        upstream.id,
        &reacquired.token,
        starting_gen,
    )
    .await
    .expect("release reacquired lease");

    let (token_url, calls, server) = token_server(
        StatusCode::OK,
        r#"{"access_token":"antigravity-new-access","refresh_token":"antigravity-new-refresh","token_type":"Bearer","expires_in":3600}"#,
    )
    .await;
    configure_loopback_google(&mut fixture, &token_url, Some(&token_url));
    let completed = fixture
        .dispatch
        .dispatch_oauth_refresh(OAuthRefreshJob::new(upstream.id))
        .await
        .expect("configured antigravity refresh succeeds immediately");
    assert_eq!(completed, JobOutcome::Done);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let refreshed = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
        .await
        .expect("load refreshed upstream")
        .expect("upstream exists");
    assert_eq!(refreshed.oauth_token_generation, starting_gen + 1);
    server.abort();
}
