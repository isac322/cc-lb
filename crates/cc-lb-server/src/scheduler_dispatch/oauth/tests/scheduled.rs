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
                    delay: std::time::Duration::from_secs(30),
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
    UpstreamStore::set_enabled(
        fixture.storage.as_ref(),
        upstream.id,
        upstream.revision,
        false,
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
            delay: std::time::Duration::from_secs(30),
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
    UpstreamStore::update_spec(
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
