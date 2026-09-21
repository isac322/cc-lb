// allow: SIZE_OK — single end-to-end replacement-worker OAuth refresh proxy scenario

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_worker_refreshes_selected_oauth_upstream_during_message_request() {
    // CI latency budget, not a correctness bound: this drives a full proxy ->
    // lazy-refresh-enqueue -> scheduler-dispatch async chain that normally
    // finishes in well under a second, but under llvm-cov instrumentation plus
    // a co-scheduled heavy build on the shared runner it overran even a 30s
    // ceiling and flaked (the dispatch still happens; only the wait was too
    // short). Scale by CC_LB_TEST_READY_TIMEOUT_SECS (240 in active CI), the
    // repo's convention for these tests, instead of a hardcoded value.
    let qa_timeout = std::env::var("CC_LB_TEST_READY_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(30));

    let message_script = MessageScript::new();
    let refresh_pause = OAuthRefreshPause::new();
    let fixture = Fixture::new_without_scheduler(AppConfig {
        oauth_refresh_pause: Some(refresh_pause.clone()),
        message_script: Some(message_script.clone()),
        ..AppConfig::default()
    })
    .await;
    fixture.create_principal("oauth-principal").await;
    let key_store = Arc::new(KeyStore::new(fixture.storage.clone()));
    let (_key_record, key_secret) = key_store
        .create(
            "oauth-principal",
            CreateParams {
                label: "scheduler-restart-oauth-proxy".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: Vec::new(),
            },
        )
        .await
        .expect("managed key created");
    let mut stored_keys = key_store.list_all().await.expect("managed keys listed");
    let stored_key_count = stored_keys.len();
    let (stored_principal_id, key_id, _) = stored_keys.pop().expect("managed key exists");

    let (dispatch_tx, mut dispatch_rx) = mpsc::channel(2);
    let job_a = Uuid::new_v4();
    fixture
        .scheduler_backend
        .push_job(AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(job_a)))
        .await
        .expect("generation 1 job A enqueued");
    let generation_one_worker = build_adaptive_worker(
        &fixture.scheduler_backend,
        SchedulerCtx::new(
            cc_lb_config::SchedulerConfig::default(),
            Arc::new({
                let dispatch_tx = dispatch_tx.clone();
                move |job| {
                    let dispatch_tx = dispatch_tx.clone();
                    Box::pin(async move {
                        let upstream_id = match job {
                            AdaptiveJob::OAuthRefresh(job) => job.upstream_id,
                            AdaptiveJob::Warmup(_)
                            | AdaptiveJob::MetadataRefresh(_)
                            | AdaptiveJob::CacheKeepalive(_) => {
                                panic!("unexpected generation 1 adaptive job")
                            }
                        };
                        dispatch_tx
                            .send(upstream_id)
                            .await
                            .expect("dispatch receiver remains open");
                        Ok(JobOutcome::Done)
                    })
                }
            }),
            Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
            fixture.clock.clone(),
        ),
    )
    .expect("generation 1 worker builds");
    let generation_one_cancel = CancellationToken::new();
    let generation_one_task =
        tokio::spawn(generation_one_worker.run_until_cancelled(generation_one_cancel.clone()));
    let dispatched_a = timeout(qa_timeout, dispatch_rx.recv())
        .await
        .expect("generation 1 dispatch timed out")
        .expect("generation 1 dispatch channel closed");
    generation_one_cancel.cancel();
    generation_one_task
        .await
        .expect("generation 1 worker joins")
        .expect("generation 1 worker exits cleanly");
    match &fixture.scheduler_backend {
        #[cfg(feature = "sqlite")]
        SchedulerBackend::Sqlite(sqlite) => {
            sqlite
                .pool()
                .acquire()
                .await
                .expect("scheduler connection acquires")
                .close()
                .await
                .expect("scheduler connection closes");
        }
        #[cfg(feature = "postgres")]
        SchedulerBackend::Postgres(_) => panic!("test requires a SQLite scheduler backend"),
    }

    let initial_tokens = initial_tokens(&fixture.fake_base).await;
    let initial_access_token = initial_tokens.access_token.clone();
    let upstream_id = fixture
        .create_oauth_upstream_with_tokens(
            "oauth-target",
            now_secs(fixture.clock.as_ref()).saturating_sub(1),
            initial_tokens,
            Some(Url::parse(&fixture.fake_base).expect("fake url")),
        )
        .await;
    let initial_generation =
        UpstreamStore::read_oauth_token_generation(fixture.storage.as_ref(), upstream_id)
            .await
            .expect("initial token generation read")
            .expect("initial token generation exists");

    let lazy_cancel = CancellationToken::new();
    let lazy = Arc::new(LazyRefresher::new(LazyRefresherParams {
        deps: LazyRefresherDeps {
            stores: fixture.stores.clone(),
            aead: fixture.aead.clone(),
            oauth_cfg: fixture.oauth_cfg.clone(),
            clock: fixture.clock.clone(),
        },
        replica_id: Uuid::new_v4(),
        metadata_hook: None,
        cancel: lazy_cancel.clone(),
        apalis_handle: fixture.scheduler_backend.clone(),
    }));
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let view = build_dynamic_view(
        fixture.stores.as_ref(),
        fixture.oauth_cfg.as_ref(),
        fixture.aead.clone(),
        Some(lazy),
        0,
        &runtime,
        fixture._dir.path(),
        Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
        30,
        None,
        None,
        1800,
        fixture.clock.clone(),
    )
    .await
    .expect("dynamic view builds");
    let lifecycle = Lifecycle::new_with_dynamic_view(
        Arc::new(BuiltinAuthn::new(key_store.clone(), fixture.clock.clone())),
        Arc::new(DynamicViewHolder::new(view)),
        cc_lb_engine::make_default_dispatcher(50),
        LifecycleConfig::default(),
        fixture.clock.clone(),
    );

    let generation_two_backend = fixture.scheduler_backend.clone();
    let generation_two_worker = build_adaptive_worker(
        &fixture.scheduler_backend,
        SchedulerCtx::new(
            cc_lb_config::SchedulerConfig::default(),
            Arc::new({
                let dispatch_tx = dispatch_tx.clone();
                let storage = fixture.storage.clone();
                let aead = fixture.aead.clone();
                let oauth_cfg = fixture.oauth_cfg.clone();
                let clock = fixture.clock.clone();
                move |job| {
                    let dispatch_tx = dispatch_tx.clone();
                    let backend = generation_two_backend.clone();
                    let storage = storage.clone();
                    let aead = aead.clone();
                    let oauth_cfg = oauth_cfg.clone();
                    let clock = clock.clone();
                    Box::pin(async move {
                        match &job {
                            AdaptiveJob::OAuthRefresh(refresh_job) => {
                                dispatch_tx
                                    .send(refresh_job.upstream_id)
                                    .await
                                    .expect("dispatch receiver remains open");
                            }
                            AdaptiveJob::Warmup(_)
                            | AdaptiveJob::MetadataRefresh(_)
                            | AdaptiveJob::CacheKeepalive(_) => {}
                        }
                        dispatch_oauth_refresh_job(backend, storage, aead, oauth_cfg, clock, job)
                            .await
                    })
                }
            }),
            Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
            fixture.clock.clone(),
        ),
    )
    .expect("generation 2 worker builds");
    let generation_two_cancel = CancellationToken::new();
    let mut generation_two_task =
        tokio::spawn(generation_two_worker.run_until_cancelled(generation_two_cancel.clone()));
    let request = Request::builder()
        .method(Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", key_secret.expose())
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .body(Bytes::from_static(
            br#"{"model":"claude-3-5-sonnet-20241022","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}"#,
        ))
        .expect("request builds");
    let mut request_task =
        tokio::spawn(async move { timeout(qa_timeout, lifecycle.handle(request)).await });

    let dispatched_b = tokio::select! {
        worker_result = &mut generation_two_task => {
            panic!("generation 2 worker exited before dispatch: {worker_result:?}");
        }
        request_result = &mut request_task => {
            match request_result {
                Err(error) => panic!(
                    "proxy request task failed before generation 2 dispatch: {error}"
                ),
                Ok(Err(_elapsed)) => panic!(
                    "proxy request timed out before generation 2 dispatch"
                ),
                Ok(Ok(Err(error))) => panic!(
                    "proxy request failed before generation 2 dispatch: {error}"
                ),
                Ok(Ok(Ok(response))) => panic!(
                    "proxy request completed with status {} before generation 2 dispatch",
                    response.status()
                ),
            }
        }
        result = timeout(qa_timeout, dispatch_rx.recv()) => {
            result
                .expect("generation 2 dispatch timed out")
                .expect("generation 2 dispatch channel closed")
        }
    };
    timeout(qa_timeout, refresh_pause.wait_until_entered())
        .await
        .expect("OAuth token endpoint was not reached");
    refresh_pause.release();
    let response = request_task
        .await
        .expect("proxy request task joins")
        .expect("proxy request timed out")
        .expect("lifecycle response");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();

    let response_body = String::from_utf8_lossy(&body).into_owned();
    let refresh_history_count = refresh_history_len(&fixture.fake_base).await;
    let refreshed_generation =
        UpstreamStore::read_oauth_token_generation(fixture.storage.as_ref(), upstream_id)
            .await
            .expect("refreshed token generation read");
    let refreshed_record = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
        .await
        .expect("refreshed upstream read")
        .expect("refreshed upstream exists");
    let refreshed_tokens = refreshed_record
        .oauth_credentials
        .as_ref()
        .expect("refreshed OAuth credentials exist")
        .decrypt(fixture.aead.as_ref(), upstream_id.as_bytes())
        .expect("refreshed OAuth credentials decrypt");
    let refreshed_access_token = refreshed_tokens.access_token;
    let recorded_requests = message_script.requests();
    let recorded_request_count = recorded_requests.len();
    let recorded_authorization = recorded_requests
        .first()
        .and_then(|request| request.headers.get("authorization"))
        .cloned();
    let expected_authorization = format!("Bearer {refreshed_access_token}");

    key_store
        .revoke("oauth-principal", &key_id)
        .await
        .expect("managed key revoked");
    let revoked_key = key_store
        .get("oauth-principal", &key_id)
        .await
        .expect("revoked key read")
        .expect("revoked key remains auditable");
    let revoked_key_status = revoked_key.status;
    lazy_cancel.cancel();
    generation_two_cancel.cancel();
    generation_two_task
        .await
        .expect("generation 2 worker joins")
        .expect("generation 2 worker exits cleanly");

    assert_eq!(stored_key_count, 1, "test creates exactly one managed key");
    assert_eq!(stored_principal_id, "oauth-principal");
    assert_eq!(dispatched_a, job_a);
    assert_ne!(job_a, upstream_id, "jobs A and B are distinct");
    assert_eq!(dispatched_b, upstream_id);
    assert_eq!(status, StatusCode::OK, "{response_body}");
    assert_eq!(refresh_history_count, 1);
    assert_eq!(refreshed_generation, Some(initial_generation + 1));
    assert!(
        refreshed_access_token != initial_access_token,
        "refreshed access token must differ from initial"
    );
    assert_eq!(
        recorded_request_count, 1,
        "sole fake upstream receives one request"
    );
    assert!(
        recorded_authorization
            .as_deref()
            .is_some_and(|authorization| authorization == expected_authorization),
        "upstream authorization must use refreshed bearer token"
    );
    assert_eq!(revoked_key_status, KeyStatus::Revoked);
}
