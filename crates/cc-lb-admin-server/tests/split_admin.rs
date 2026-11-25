#[cfg(feature = "sqlite")]
mod sqlite_tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use cc_lb_admin_server::build_admin_app_with_storage;
    use cc_lb_aead::AeadService;
    use cc_lb_clock::ClockHandle;
    use cc_lb_config::{AdminAuthProviderConfig, Config, StorageConfig};
    use cc_lb_storage_api::upstream::{UpstreamCreate, UpstreamKind, UpstreamUpdate};
    use cc_lb_storage_api::{
        ManagedKeyStore, MetaStore as _, RequestEvent, RequestEventStore, Storage,
        SubscriptionQuotaSample, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
        SubscriptionQuotaStatus, SubscriptionQuotaWindow, UpstreamStore,
        UpstreamSubscriptionQuotaStore,
    };
    use http_body_util::BodyExt as _;
    use serde_json::{Value, json};
    use tower::ServiceExt as _;
    use uuid::Uuid;

    const TEST_TOKEN_ENV: &str = "CC_LB_TEST_SPLIT_ADMIN_TOKEN";
    const TEST_TOKEN: &str = "split-admin-test-secret-token";

    #[tokio::test]
    async fn split_admin_serves_db_status_blocks_save_allows_download_and_omits_proxy_routes() {
        // SAFETY/isolation note: dedicated test-only env var name not shared with other crates.
        #[allow(unsafe_code)]
        unsafe {
            std::env::set_var(TEST_TOKEN_ENV, TEST_TOKEN);
        }

        let dir = tempfile::TempDir::new().expect("tempdir");
        let sqlite_path = dir.path().join("storage.sqlite");
        let config_path = dir.path().join("cc-lb.toml");
        let aead = Arc::new(AeadService::from_master_key([0u8; 32]));
        let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let database_url = format!("sqlite://{}", sqlite_path.display());
        let storage_arc = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
                .await
                .expect("open sqlite"),
        );
        storage_arc.initialize().await.expect("init sqlite");

        let active_id = Uuid::new_v4();
        let encrypted_key = aead
            .encrypt(b"sk-ant-split-active", active_id.as_bytes())
            .expect("encrypt key");
        UpstreamStore::create(
            storage_arc.as_ref(),
            UpstreamCreate {
                id: active_id,
                name: "split-seeded-active".to_owned(),
                kind: UpstreamKind::AnthropicApiKey,
                base_url: None,
                api_key_ciphertext: Some(encrypted_key),
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
                vertex: None,
                vertex_credential_type: None,
                vertex_quota_project_id: None,
            },
        )
        .await
        .expect("seed active upstream");

        let unconfigured_id = Uuid::new_v4();
        UpstreamStore::create(
            storage_arc.as_ref(),
            UpstreamCreate {
                id: unconfigured_id,
                name: "split-seeded-unconfigured".to_owned(),
                kind: UpstreamKind::AnthropicApiKey,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
                vertex: None,
                vertex_credential_type: None,
                vertex_quota_project_id: None,
            },
        )
        .await
        .expect("seed unconfigured upstream");

        let oauth_id = Uuid::new_v4();
        let oauth_rec = UpstreamStore::create(
            storage_arc.as_ref(),
            UpstreamCreate {
                id: oauth_id,
                name: "split-seeded-oauth".to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
                vertex: None,
                vertex_credential_type: None,
                vertex_quota_project_id: None,
            },
        )
        .await
        .expect("seed oauth upstream");
        UpstreamStore::update(
            storage_arc.as_ref(),
            oauth_rec.id,
            oauth_rec.revision,
            UpstreamUpdate {
                enabled: Some(false),
                ..Default::default()
            },
        )
        .await
        .expect("disable oauth upstream");

        let now_secs = cc_lb_clock::unix_secs(clock.now());
        let now_ms = now_secs * 1_000;
        storage_arc
            .record_subscription_quota_samples(&[SubscriptionQuotaSample {
                upstream_id: oauth_id,
                window: SubscriptionQuotaWindow::FiveHour,
                source: SubscriptionQuotaSource::Header,
                sample_kind: SubscriptionQuotaSampleKind::Sample,
                observed_at_unix_millis: now_ms,
                sample_id: Uuid::new_v4(),
                utilization: Some(0.42),
                status: Some(SubscriptionQuotaStatus::Allowed),
                resets_at_unix_secs: Some(now_secs + 3_600),
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
                ingested_at_unix_millis: now_ms,
            }])
            .await
            .expect("seed quota sample");

        let mut config = Config {
            storage: StorageConfig::Sqlite {
                path: sqlite_path.clone(),
            },
            ..Config::default()
        };
        config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
        config.runtime.data_dir = Some(dir.path().to_path_buf());
        config.event_bus.storage_tail_poll_interval_ms = 25;
        config.admin.auth.providers = vec![AdminAuthProviderConfig::StaticToken {
            id: "split-test-token".to_owned(),
            token_env: TEST_TOKEN_ENV.to_owned(),
        }];
        let raw_toml =
            Config::partial_json_toml(serde_json::to_value(&config).expect("config json"))
                .expect("config toml");
        std::fs::write(&config_path, &raw_toml).expect("write writable config file");

        let managed_store: Arc<dyn ManagedKeyStore> = storage_arc.clone();
        let storage: Arc<dyn Storage> = storage_arc.clone();
        let app = build_admin_app_with_storage(
            config.clone(),
            Some(&config_path),
            managed_store,
            storage,
            aead,
            clock,
        )
        .await
        .expect("build_admin_app_with_storage");

        // 1. /admin/health returns 200.
        let health = app
            .admin_router
            .clone()
            .oneshot(
                Request::get("/admin/health")
                    .body(Body::empty())
                    .expect("health request"),
            )
            .await
            .expect("health response");
        assert_eq!(health.status(), StatusCode::OK);

        // 2. Proxy route /v1/messages does not exist on split admin server
        //    (405 Method Not Allowed from `GET /{*file}` SPA wildcard when `embed-spa`
        //    is enabled, or 404 Not Found without `embed-spa`).
        let proxy_route = app
            .admin_router
            .clone()
            .oneshot(
                Request::post("/v1/messages")
                    .body(Body::empty())
                    .expect("proxy route request"),
            )
            .await
            .expect("proxy route response");
        #[cfg(feature = "embed-spa")]
        assert_eq!(proxy_route.status(), StatusCode::METHOD_NOT_ALLOWED);
        #[cfg(not(feature = "embed-spa"))]
        assert_eq!(proxy_route.status(), StatusCode::NOT_FOUND);

        // 3. DB-only /admin/v1/status falls back to storage and accurately classifies active/disabled/error.
        let status_resp = app
            .admin_router
            .clone()
            .oneshot(
                Request::get("/admin/v1/status")
                    .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
                    .body(Body::empty())
                    .expect("status request"),
            )
            .await
            .expect("status response");
        assert_eq!(status_resp.status(), StatusCode::OK);
        let status_bytes = status_resp
            .into_body()
            .collect()
            .await
            .expect("status body")
            .to_bytes();
        let status_json: Value = serde_json::from_slice(&status_bytes).expect("status json");
        let upstreams = status_json["upstreams"]
            .as_array()
            .expect("upstreams array");
        assert_eq!(upstreams.len(), 3, "unexpected status json: {status_json}");
        let by_name = |name: &str| {
            upstreams
                .iter()
                .find(|u| u["name"] == name)
                .unwrap_or_else(|| panic!("missing {name} in {status_json}"))
        };
        assert_eq!(by_name("split-seeded-active")["status"], "active");
        assert_eq!(by_name("split-seeded-unconfigured")["status"], "error");
        assert_eq!(by_name("split-seeded-oauth")["status"], "disabled");

        // 3b. /admin/v1/subscription-quotas/latest reads durable latest rows in split mode.
        let latest_resp = app
            .admin_router
            .clone()
            .oneshot(
                Request::get("/admin/v1/subscription-quotas/latest")
                    .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
                    .body(Body::empty())
                    .expect("latest quota request"),
            )
            .await
            .expect("latest quota response");
        assert_eq!(latest_resp.status(), StatusCode::OK);
        let latest_json: Value = serde_json::from_slice(
            &latest_resp
                .into_body()
                .collect()
                .await
                .expect("latest quota body")
                .to_bytes(),
        )
        .expect("latest quota json");
        let oauth_latest = latest_json["upstreams"]
            .as_array()
            .expect("latest upstreams")
            .iter()
            .find(|u| u["upstream_id"] == oauth_id.to_string())
            .expect("oauth upstream in latest");
        let five_h = oauth_latest["windows"]
            .as_array()
            .expect("windows array")
            .iter()
            .find(|w| w["window"] == "5h")
            .unwrap_or_else(|| panic!("missing 5h window in {oauth_latest}"));
        assert_eq!(five_h["state"], "fresh");
        assert_eq!(five_h["utilization"], 0.42);
        assert_eq!(five_h["source"], "header");

        let latest_header_resp = app
            .admin_router
            .clone()
            .oneshot(
                Request::get("/admin/v1/subscription-quotas/latest?source=header")
                    .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
                    .body(Body::empty())
                    .expect("latest header quota request"),
            )
            .await
            .expect("latest header quota response");
        assert_eq!(latest_header_resp.status(), StatusCode::OK);
        let latest_header_json: Value = serde_json::from_slice(
            &latest_header_resp
                .into_body()
                .collect()
                .await
                .expect("latest header quota body")
                .to_bytes(),
        )
        .expect("latest header quota json");
        let oauth_latest_header = latest_header_json["upstreams"]
            .as_array()
            .expect("latest header upstreams")
            .iter()
            .find(|u| u["upstream_id"] == oauth_id.to_string())
            .expect("oauth upstream in header latest");
        let five_h_header = oauth_latest_header["windows"]
            .as_array()
            .expect("header windows array")
            .iter()
            .find(|w| w["window"] == "5h")
            .unwrap_or_else(|| panic!("missing 5h window in {oauth_latest_header}"));
        assert_eq!(five_h_header["state"], "fresh");
        assert_eq!(five_h_header["source"], "header");
        assert_eq!(five_h_header["utilization"], 0.42);

        // 3c. SQLite split StorageTailPoller streams externally inserted events over SSE.
        let sse_resp = app
            .admin_router
            .clone()
            .oneshot(
                Request::get("/admin/v1/events/stream")
                    .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
                    .body(Body::empty())
                    .expect("sse request"),
            )
            .await
            .expect("sse response");
        assert_eq!(sse_resp.status(), StatusCode::OK);
        storage_arc
            .append_request_event(&RequestEvent {
                ts: now_secs,
                ts_ms: Some(now_ms),
                request_id: "split-sqlite-sse-req".to_owned(),
                event_id: Some("split-sqlite-sse-evt".to_owned()),
                status: 200,
                duration_ms: 12,
                ..Default::default()
            })
            .await
            .expect("append external request event");
        let mut sse_body = sse_resp.into_body();
        let mut seen_event = false;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        let mut buf = String::new();
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout_at(deadline, sse_body.frame()).await {
                Ok(Some(Ok(frame))) => {
                    if let Ok(data) = frame.into_data() {
                        buf.push_str(&String::from_utf8_lossy(&data));
                        if buf.contains("split-sqlite-sse-evt") {
                            seen_event = true;
                            break;
                        }
                    }
                }
                _ => break,
            }
        }
        assert!(
            seen_event,
            "SQLite StorageTailPoller must deliver external request_events_v1 row; got: {buf}"
        );

        // 4. Config editor reports read_only in split mode; draft validate succeeds; save returns 409; download returns 200 TOML.
        let editor_resp = app
            .admin_router
            .clone()
            .oneshot(
                Request::get("/admin/v1/config/editor")
                    .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
                    .body(Body::empty())
                    .expect("editor request"),
            )
            .await
            .expect("editor response");
        assert_eq!(editor_resp.status(), StatusCode::OK);
        let editor_json: Value = serde_json::from_slice(
            &editor_resp
                .into_body()
                .collect()
                .await
                .expect("editor body")
                .to_bytes(),
        )
        .expect("editor json");
        assert_eq!(editor_json["file"]["mode"], "read_only");

        let mut draft_cfg = editor_json["file_config"].clone();
        draft_cfg["timeouts"]["upstream_total_secs"] = json!(456);
        let put_resp = app
            .admin_router
            .clone()
            .oneshot(
                Request::put("/admin/v1/config/draft")
                    .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({ "expected_revision": 0, "draft": draft_cfg }).to_string(),
                    ))
                    .expect("put draft"),
            )
            .await
            .expect("put draft response");
        assert_eq!(put_resp.status(), StatusCode::OK);
        let put_json: Value = serde_json::from_slice(
            &put_resp
                .into_body()
                .collect()
                .await
                .expect("put body")
                .to_bytes(),
        )
        .expect("put json");
        let revision = put_json["revision"].as_u64().expect("draft revision");

        let validate_resp = app
            .admin_router
            .clone()
            .oneshot(
                Request::post("/admin/v1/config/draft/validate")
                    .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({ "expected_revision": revision }).to_string(),
                    ))
                    .expect("validate draft"),
            )
            .await
            .expect("validate response");
        assert_eq!(validate_resp.status(), StatusCode::OK);
        let validate_json: Value = serde_json::from_slice(
            &validate_resp
                .into_body()
                .collect()
                .await
                .expect("validate body")
                .to_bytes(),
        )
        .expect("validate json");
        assert_eq!(validate_json["file"]["valid"], true);
        assert_eq!(validate_json["effective"]["valid"], true);
        assert_eq!(validate_json["filesystem"], json!([]));

        let editor_after_resp = app
            .admin_router
            .clone()
            .oneshot(
                Request::get("/admin/v1/config/editor")
                    .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
                    .body(Body::empty())
                    .expect("editor after request"),
            )
            .await
            .expect("editor after response");
        assert_eq!(editor_after_resp.status(), StatusCode::OK);
        let editor_after_json: Value = serde_json::from_slice(
            &editor_after_resp
                .into_body()
                .collect()
                .await
                .expect("editor after body")
                .to_bytes(),
        )
        .expect("editor after json");
        assert_eq!(editor_after_json["last_validated_revision"], revision);

        let save_resp = app
            .admin_router
            .clone()
            .oneshot(
                Request::post("/admin/v1/config/save")
                    .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "expected_revision": revision,
                            "expected_fingerprint": editor_json["file"]["fingerprint"],
                            "confirm_self_lockout": false,
                        })
                        .to_string(),
                    ))
                    .expect("save request"),
            )
            .await
            .expect("save response");
        assert_eq!(save_resp.status(), StatusCode::CONFLICT);
        let save_json: Value = serde_json::from_slice(
            &save_resp
                .into_body()
                .collect()
                .await
                .expect("save body")
                .to_bytes(),
        )
        .expect("save json");
        assert_eq!(save_json["error"], "file_not_writable");

        let dl_resp = app
            .admin_router
            .oneshot(
                Request::post("/admin/v1/config/draft/download")
                    .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({ "expected_revision": revision }).to_string(),
                    ))
                    .expect("download request"),
            )
            .await
            .expect("download response");
        assert_eq!(dl_resp.status(), StatusCode::OK);
        let dl_bytes = dl_resp
            .into_body()
            .collect()
            .await
            .expect("download body")
            .to_bytes();
        let downloaded = String::from_utf8(dl_bytes.to_vec()).expect("download utf8");
        assert!(downloaded.contains("upstream_total_secs = 456"));
    }

    #[tokio::test]
    async fn split_admin_wires_configured_wasmtime_runtime() {
        // SAFETY/isolation note: dedicated test-only env var name not shared with other crates.
        #[allow(unsafe_code)]
        unsafe {
            std::env::set_var(TEST_TOKEN_ENV, TEST_TOKEN);
        }

        let dir = tempfile::TempDir::new().expect("tempdir");
        let sqlite_path = dir.path().join("storage.sqlite");
        let aead = Arc::new(AeadService::from_master_key([0u8; 32]));
        let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let database_url = format!("sqlite://{}", sqlite_path.display());
        let storage_arc = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
                .await
                .expect("open sqlite"),
        );
        storage_arc.initialize().await.expect("init sqlite");

        let mut bad_cfg = Config {
            storage: StorageConfig::Sqlite {
                path: sqlite_path.clone(),
            },
            ..Config::default()
        };
        bad_cfg.aead.key_env = "__CC_LB_TEST_SPLIT_ADMIN_KEY__".to_owned();
        bad_cfg.runtime.data_dir = Some(dir.path().to_path_buf());
        bad_cfg.admin.auth.providers = vec![AdminAuthProviderConfig::StaticToken {
            id: "split-test-token".to_owned(),
            token_env: TEST_TOKEN_ENV.to_owned(),
        }];
        bad_cfg.runtime.wasmtime.allocation_strategy =
            cc_lb_config::WasmtimeAllocationStrategy::Pooling;
        bad_cfg.runtime.wasmtime.memory_max_pages = Some(64);
        bad_cfg.runtime.wasmtime.memory_reservation_bytes = Some(4096);

        let err = match build_admin_app_with_storage(
            bad_cfg.clone(),
            None,
            storage_arc.clone(),
            storage_arc.clone(),
            aead.clone(),
            clock.clone(),
        )
        .await
        {
            Ok(_) => {
                panic!(
                    "pooling reservation smaller than max memory must fail WasmtimeRuntime init in split admin"
                )
            }
            Err(err) => err,
        };
        assert!(
            matches!(
                err,
                cc_lb_admin_server::AdminBuildError::WasmtimeRuntimeInit(_)
            ),
            "expected AdminBuildError::WasmtimeRuntimeInit when configured wasmtime runtime is invalid, got {err}"
        );

        let mut ok_cfg = bad_cfg;
        ok_cfg.runtime.wasmtime = cc_lb_config::WasmtimeConfig {
            execution_timeout_ms: Some(50),
            ..Default::default()
        };
        let app = build_admin_app_with_storage(
            ok_cfg,
            None,
            storage_arc.clone(),
            storage_arc,
            aead,
            clock,
        )
        .await
        .expect("split admin builds with configured WasmtimeRuntime");
        let boundary = "----split-admin-wasm-test";
        let mut body = Vec::new();
        body.extend_from_slice(
            format!("--{boundary}\r\nContent-Disposition: form-data; name=\"original_filename\"\r\n\r\nbad.wasm\r\n").as_bytes(),
        );
        body.extend_from_slice(
            format!("--{boundary}\r\nContent-Disposition: form-data; name=\"bytes\"; filename=\"bad.wasm\"\r\nContent-Type: application/octet-stream\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(b"\0asm\x01\x00\x00\x00");
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        let resp = app
            .admin_router
            .oneshot(
                Request::post("/admin/v1/plugins/wasm")
                    .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
                    .header(
                        header::CONTENT_TYPE,
                        format!("multipart/form-data; boundary={boundary}"),
                    )
                    .body(Body::from(body))
                    .expect("upload request"),
            )
            .await
            .expect("upload response");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn split_admin_manual_warmup_bridges_via_scheduler_worker_and_degrades_without_worker() {
        #[allow(unsafe_code)]
        unsafe {
            std::env::set_var(TEST_TOKEN_ENV, TEST_TOKEN);
        }

        let dir = tempfile::TempDir::new().expect("tempdir");
        let sqlite_path = dir.path().join("storage.sqlite");
        let aead = Arc::new(AeadService::from_master_key([0u8; 32]));
        let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let database_url = format!("sqlite://{}", sqlite_path.display());
        let storage_arc = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
                .await
                .expect("open sqlite"),
        );
        storage_arc.initialize().await.expect("init sqlite");

        let upstream_id = Uuid::new_v4();
        let bundle = cc_lb_aead::OAuthTokenBundle {
            access_token: "split-access".to_owned(),
            refresh_token: "split-refresh".to_owned(),
            expires_at_unix_secs: 1_900_000_000,
            refresh_token_expires_at_unix_secs: None,
            account_id: Some("acct-split".to_owned()),
            project_id: None,
            oauth_profile: cc_lb_aead::GeminiOAuthProfile::default(),
            scopes: Vec::new(),
            never_refresh: false,
        };
        let encrypted_oauth =
            cc_lb_aead::EncryptedOAuthTokens::encrypt(&aead, &bundle, upstream_id.as_bytes())
                .expect("encrypt oauth");
        UpstreamStore::create(
            storage_arc.as_ref(),
            UpstreamCreate {
                id: upstream_id,
                name: "split-warmup-oauth".to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: Some(cc_lb_storage_api::OAuthTokensCreate {
                    tokens: encrypted_oauth,
                    never_refresh: false,
                }),
                warmup_enabled: true,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
                vertex: None,
                vertex_credential_type: None,
                vertex_quota_project_id: None,
            },
        )
        .await
        .expect("seed oauth upstream");

        let storage_cfg = StorageConfig::Sqlite {
            path: sqlite_path.clone(),
        };
        let worker_scheduler = cc_lb_scheduler::factory::open_scheduler_storage_with_role(
            &storage_cfg,
            &cc_lb_config::SchedulerConfig::default(),
            cc_lb_scheduler::factory::SchedulerPoolRole::Worker,
            clock.clone(),
        )
        .await
        .expect("open worker scheduler");
        let worker_backend = worker_scheduler.lazy_handle();

        // 1. Direct port with short timeout degrades cleanly when no worker picks up the job.
        let unserviced_id = Uuid::new_v4();
        let unserviced = cc_lb_admin_server::split_ports::SplitSchedulerManualWarmup::new(
            storage_arc.clone(),
            worker_backend.clone(),
            clock.clone(),
        )
        .with_timeout(
            std::time::Duration::from_millis(120),
            std::time::Duration::from_millis(15),
        );
        let unserviced_err = cc_lb_admin::SplitManualWarmup::fire_manual_warmup(
            &unserviced,
            unserviced_id,
            cc_lb_storage_api::WarmupLane::Primary,
            "fire-now:timeout".to_owned(),
            1_800_000_000,
        )
        .await
        .expect_err("no worker should time out gracefully");
        assert!(
            matches!(
                unserviced_err,
                cc_lb_admin::ports::SplitManualWarmupError::OutcomeUnknown(_)
            ),
            "unexpected error: {unserviced_err}"
        );

        // 2. Full HTTP route on split admin enqueues ManualWarmupJob and returns 200 once worker settles it.
        let mut cfg = Config {
            storage: storage_cfg,
            ..Config::default()
        };
        cfg.runtime.data_dir = Some(dir.path().to_path_buf());
        cfg.admin.auth.providers = vec![AdminAuthProviderConfig::StaticToken {
            id: "split-test-token".to_owned(),
            token_env: TEST_TOKEN_ENV.to_owned(),
        }];
        let app = build_admin_app_with_storage(
            cfg,
            None,
            storage_arc.clone(),
            storage_arc.clone(),
            aead,
            clock,
        )
        .await
        .expect("build split admin app");

        let storage_for_worker = storage_arc.clone();
        let backend_for_worker = worker_backend.clone();
        let worker_task = tokio::spawn(async move {
            for _ in 0..200 {
                let tasks = backend_for_worker
                    .list_adaptive_tasks(&cc_lb_scheduler::worker::Filter {
                        status: Some(cc_lb_scheduler::worker::TaskStatus::Pending),
                        page: 1,
                        page_size: Some(20),
                    })
                    .await
                    .expect("list adaptive tasks");
                if let Some(job) = tasks.into_iter().find_map(|row| match row.args {
                    cc_lb_scheduler::worker::AdaptiveJob::ManualWarmup(job)
                        if job.upstream_id == upstream_id =>
                    {
                        Some(job)
                    }
                    _ => None,
                }) {
                    let now_secs = cc_lb_clock::unix_secs(std::time::SystemTime::now());
                    cc_lb_control::record_manual_warmup_result(
                        storage_for_worker.as_ref(),
                        job.upstream_id,
                        job.lane,
                        now_secs,
                        cc_lb_control::ManualWarmupJobResult {
                            nonce: job.nonce,
                            outcome: cc_lb_storage_api::WarmupAttemptOutcome::Success(
                                cc_lb_storage_api::WarmupSuccessReason::CycleAdvanced,
                            ),
                            cycle_key: Some(job.candidate_cycle_key),
                            error_detail: None,
                            expires_at_unix_secs: now_secs
                                .saturating_add(cc_lb_control::SPLIT_BRIDGE_RESULT_TTL_SECS),
                        },
                    )
                    .await
                    .expect("record result");
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(15)).await;
            }
            panic!("expected split admin to enqueue ManualWarmupJob");
        });

        let resp = app
            .admin_router
            .oneshot(
                Request::post(format!("/admin/v1/upstreams/{upstream_id}/warmup/fire-now"))
                    .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json!({ "lane": "primary" }).to_string()))
                    .expect("fire-now request"),
            )
            .await
            .expect("fire-now response");
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value =
            serde_json::from_slice(&resp.into_body().collect().await.expect("body").to_bytes())
                .expect("json");
        assert_eq!(body["fired"], true);
        assert_eq!(body["lane"], "primary");
        worker_task.await.expect("mock worker settled");
    }

    #[tokio::test]
    async fn split_admin_limit_reset_usage_refresh_and_lazy_refresh_bridge_via_scheduler() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let sqlite_path = dir.path().join("storage.sqlite");
        let aead = Arc::new(AeadService::from_master_key([0u8; 32]));
        let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let database_url = format!("sqlite://{}", sqlite_path.display());
        let storage_arc = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
                .await
                .expect("open sqlite"),
        );
        storage_arc.initialize().await.expect("init sqlite");

        let upstream_id = Uuid::new_v4();
        let bundle = cc_lb_aead::OAuthTokenBundle {
            access_token: "old-access".to_owned(),
            refresh_token: "refresh-token".to_owned(),
            expires_at_unix_secs: 1_900_000_000,
            refresh_token_expires_at_unix_secs: None,
            account_id: Some("acct".to_owned()),
            project_id: None,
            oauth_profile: cc_lb_aead::GeminiOAuthProfile::default(),
            scopes: Vec::new(),
            never_refresh: false,
        };
        UpstreamStore::create(
            storage_arc.as_ref(),
            UpstreamCreate {
                id: upstream_id,
                name: "split-bridge-oauth".to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: Some(cc_lb_storage_api::OAuthTokensCreate {
                    tokens: cc_lb_aead::EncryptedOAuthTokens::encrypt(
                        &aead,
                        &bundle,
                        upstream_id.as_bytes(),
                    )
                    .expect("encrypt"),
                    never_refresh: false,
                }),
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
                vertex: None,
                vertex_credential_type: None,
                vertex_quota_project_id: None,
            },
        )
        .await
        .expect("create upstream");

        let storage_cfg = StorageConfig::Sqlite { path: sqlite_path };
        let scheduler = cc_lb_scheduler::factory::open_scheduler_storage_with_role(
            &storage_cfg,
            &cc_lb_config::SchedulerConfig::default(),
            cc_lb_scheduler::factory::SchedulerPoolRole::Worker,
            clock.clone(),
        )
        .await
        .expect("open scheduler");
        let backend = scheduler.lazy_handle();

        // 1. Usage refresh timeout -> Failed; settled -> Refreshed.
        let unserviced_id = Uuid::new_v4();
        let usage_port =
            cc_lb_admin_server::split_ports::SplitSchedulerLimitResetUsageRefresh::new(
                storage_arc.clone(),
                backend.clone(),
                clock.clone(),
            )
            .with_timeout(
                std::time::Duration::from_millis(120),
                std::time::Duration::from_millis(15),
            );
        let cancel = tokio_util::sync::CancellationToken::new();
        assert_eq!(
            cc_lb_control::LimitResetUsageRefreshPort::refresh_upstream_usage(
                &usage_port,
                unserviced_id,
                &cancel,
            )
            .await,
            cc_lb_control::LimitResetUsageRefresh::Failed
        );

        let usage_port_ok =
            cc_lb_admin_server::split_ports::SplitSchedulerLimitResetUsageRefresh::new(
                storage_arc.clone(),
                backend.clone(),
                clock.clone(),
            );
        let s_usage = storage_arc.clone();
        let b_usage = backend.clone();
        let usage_worker = tokio::spawn(async move {
            for _ in 0..200 {
                let tasks = b_usage
                    .list_adaptive_tasks(&cc_lb_scheduler::worker::Filter {
                        status: Some(cc_lb_scheduler::worker::TaskStatus::Pending),
                        page: 1,
                        page_size: Some(20),
                    })
                    .await
                    .expect("list");
                if let Some(job) = tasks.into_iter().find_map(|row| match row.args {
                    cc_lb_scheduler::worker::AdaptiveJob::OAuthUsageRefresh(job)
                        if job.upstream_id == upstream_id =>
                    {
                        Some(job)
                    }
                    _ => None,
                }) {
                    let now_secs = cc_lb_clock::unix_secs(std::time::SystemTime::now());
                    cc_lb_control::record_oauth_usage_refresh_result(
                        s_usage.as_ref(),
                        job.upstream_id,
                        now_secs,
                        cc_lb_control::OAuthUsageRefreshJobResult {
                            nonce: job.nonce,
                            outcome: cc_lb_control::LimitResetUsageRefresh::Refreshed,
                            expires_at_unix_secs: now_secs
                                .saturating_add(cc_lb_control::SPLIT_BRIDGE_RESULT_TTL_SECS),
                        },
                    )
                    .await
                    .expect("record");
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(15)).await;
            }
            panic!("usage job not seen");
        });
        assert_eq!(
            cc_lb_control::LimitResetUsageRefreshPort::refresh_upstream_usage(
                &usage_port_ok,
                upstream_id,
                &cancel,
            )
            .await,
            cc_lb_control::LimitResetUsageRefresh::Refreshed
        );
        usage_worker.await.expect("usage worker done");

        // 2. Lazy refresh timeout -> Err; bumped token generation -> Ok.
        let lazy_short = cc_lb_admin_server::split_ports::SplitSchedulerLazyRefresher::new(
            storage_arc.clone(),
            backend.clone(),
            clock.clone(),
            Some(aead.clone()),
        )
        .with_timeout(
            std::time::Duration::from_millis(120),
            std::time::Duration::from_millis(15),
        );
        assert!(
            cc_lb_signer_anthropic_oauth::LazyRefreshHandle::refresh_one(&lazy_short, upstream_id)
                .await
                .is_err()
        );

        let lazy_ok = cc_lb_admin_server::split_ports::SplitSchedulerLazyRefresher::new(
            storage_arc.clone(),
            backend.clone(),
            clock.clone(),
            Some(aead.clone()),
        );
        let s_lazy = storage_arc.clone();
        let aead_lazy = aead.clone();
        let lazy_worker = tokio::spawn(async move {
            for _ in 0..200 {
                let tasks = backend
                    .list_adaptive_tasks(&cc_lb_scheduler::worker::Filter {
                        status: Some(cc_lb_scheduler::worker::TaskStatus::Pending),
                        page: 1,
                        page_size: Some(20),
                    })
                    .await
                    .expect("list");
                if tasks.iter().any(|row| {
                    matches!(
                        &row.args,
                        cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(j) if j.upstream_id == upstream_id
                    )
                }) {
                    let current = UpstreamStore::get_by_id(s_lazy.as_ref(), upstream_id)
                        .await
                        .expect("get")
                        .expect("exists");
                    let encrypted = cc_lb_aead::EncryptedOAuthTokens::encrypt(
                        &aead_lazy,
                        &bundle,
                        upstream_id.as_bytes(),
                    )
                    .expect("encrypt");
                    UpstreamStore::store_oauth_tokens(
                        s_lazy.as_ref(),
                        upstream_id,
                        current.revision,
                        encrypted,
                        false,
                    )
                    .await
                    .expect("bump oauth_token_generation");
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(15)).await;
            }
            panic!("oauth refresh job not seen");
        });
        cc_lb_signer_anthropic_oauth::LazyRefreshHandle::refresh_one(&lazy_ok, upstream_id)
            .await
            .expect("split lazy refresher succeeds once generation bumps");
        lazy_worker.await.expect("lazy worker done");
    }

    #[tokio::test]
    async fn split_admin_aggregate_reads_durable_latest_without_local_quota_cache() {
        #[allow(unsafe_code)]
        unsafe {
            std::env::set_var(TEST_TOKEN_ENV, TEST_TOKEN);
        }

        let dir = tempfile::TempDir::new().expect("tempdir");
        let sqlite_path = dir.path().join("storage.sqlite");
        let aead = Arc::new(AeadService::from_master_key([0u8; 32]));
        let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let database_url = format!("sqlite://{}", sqlite_path.display());
        let storage_arc = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
                .await
                .expect("open sqlite"),
        );
        storage_arc.initialize().await.expect("init sqlite");

        let upstream_id = Uuid::new_v4();
        UpstreamStore::create(
            storage_arc.as_ref(),
            UpstreamCreate {
                id: upstream_id,
                name: "split-openai-agg".to_owned(),
                kind: UpstreamKind::OpenaiOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
                vertex: None,
                vertex_credential_type: None,
                vertex_quota_project_id: None,
            },
        )
        .await
        .expect("create upstream");

        let now_millis = u64::try_from(cc_lb_clock::unix_millis(clock.now())).expect("millis");
        let now_secs = now_millis / 1_000;
        storage_arc
            .record_subscription_quota_samples(&[SubscriptionQuotaSample {
                upstream_id,
                window: SubscriptionQuotaWindow::FiveHour,
                source: SubscriptionQuotaSource::Api,
                sample_kind: SubscriptionQuotaSampleKind::Sample,
                observed_at_unix_millis: now_millis,
                sample_id: Uuid::new_v4(),
                utilization: Some(0.35),
                status: Some(SubscriptionQuotaStatus::Allowed),
                resets_at_unix_secs: Some(now_secs + 3_600),
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
                ingested_at_unix_millis: now_millis,
            }])
            .await
            .expect("record quota sample");

        let mut cfg = Config {
            storage: StorageConfig::Sqlite { path: sqlite_path },
            ..Config::default()
        };
        cfg.runtime.data_dir = Some(dir.path().to_path_buf());
        cfg.admin.auth.providers = vec![AdminAuthProviderConfig::StaticToken {
            id: "split-test-token".to_owned(),
            token_env: TEST_TOKEN_ENV.to_owned(),
        }];
        let app =
            build_admin_app_with_storage(cfg, None, storage_arc.clone(), storage_arc, aead, clock)
                .await
                .expect("build split admin");

        let resp = app
            .admin_router
            .oneshot(
                Request::get("/admin/v1/subscription-quotas/aggregate?provider=openai&window=5h")
                    .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
                    .body(Body::empty())
                    .expect("aggregate request"),
            )
            .await
            .expect("aggregate response");
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value =
            serde_json::from_slice(&resp.into_body().collect().await.expect("body").to_bytes())
                .expect("json");
        assert_eq!(body["windows"][0]["contributing_upstreams"], 1);
        assert!(
            (body["windows"][0]["utilization"].as_f64().unwrap() - 0.35).abs() < 1e-6,
            "expected split admin aggregate to use durable latest rows: {body}"
        );
    }

    #[tokio::test]
    async fn split_lazy_refresher_joins_active_generation_and_watchdog_jobs() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let sqlite_path = dir.path().join("storage.sqlite");
        let aead = Arc::new(AeadService::from_master_key([0u8; 32]));
        let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let database_url = format!("sqlite://{}", sqlite_path.display());
        let storage_arc = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
                .await
                .expect("open sqlite"),
        );
        storage_arc.initialize().await.expect("init sqlite");

        let upstream_id = Uuid::new_v4();
        let expires_at = 1_900_000_000u64;
        let bundle = cc_lb_aead::OAuthTokenBundle {
            access_token: "old-access".to_owned(),
            refresh_token: "refresh-token".to_owned(),
            expires_at_unix_secs: expires_at,
            refresh_token_expires_at_unix_secs: None,
            account_id: Some("acct".to_owned()),
            project_id: None,
            oauth_profile: cc_lb_aead::GeminiOAuthProfile::default(),
            scopes: Vec::new(),
            never_refresh: false,
        };
        UpstreamStore::create(
            storage_arc.as_ref(),
            UpstreamCreate {
                id: upstream_id,
                name: "split-lazy-join-oauth".to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: Some(
                    url::Url::parse("https://api.anthropic.com/").expect("valid base url"),
                ),
                api_key_ciphertext: None,
                oauth_tokens: Some(cc_lb_storage_api::OAuthTokensCreate {
                    tokens: cc_lb_aead::EncryptedOAuthTokens::encrypt(
                        &aead,
                        &bundle,
                        upstream_id.as_bytes(),
                    )
                    .expect("encrypt bundle"),
                    never_refresh: false,
                }),
                warmup_enabled: true,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
                vertex: None,
                vertex_credential_type: None,
                vertex_quota_project_id: None,
            },
        )
        .await
        .expect("seed oauth upstream");

        let storage_cfg = StorageConfig::Sqlite { path: sqlite_path };
        let worker_scheduler = cc_lb_scheduler::factory::open_scheduler_storage_with_role(
            &storage_cfg,
            &cc_lb_config::SchedulerConfig::default(),
            cc_lb_scheduler::factory::SchedulerPoolRole::Worker,
            clock.clone(),
        )
        .await
        .expect("open worker scheduler");
        let backend = worker_scheduler.lazy_handle();

        // 1. Pre-seed exact generation-keyed job: SplitSchedulerLazyRefresher joins/expedites without conflict.
        let initial = UpstreamStore::get_by_id(storage_arc.as_ref(), upstream_id)
            .await
            .expect("get")
            .expect("exists");
        let gen0 = initial.oauth_token_generation;
        let job0 = cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob::for_credential(
            upstream_id,
            gen0,
            expires_at,
        );
        let gen0_key = job0.generation_idempotency_key(gen0, expires_at);
        backend
            .push_adaptive_task(cc_lb_scheduler::worker::SchedulerPushTask {
                args: cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(job0),
                idempotency_key: Some(gen0_key.clone()),
                run_at_unix_secs: None,
                max_attempts: None,
            })
            .await
            .expect("seed exact generation job");

        let refresher = cc_lb_admin_server::split_ports::SplitSchedulerLazyRefresher::new(
            storage_arc.clone(),
            backend.clone(),
            clock.clone(),
            Some(aead.clone()),
        );

        let b1 = backend.clone();
        let s1 = storage_arc.clone();
        let a1 = aead.clone();
        let bundle1 = bundle.clone();
        let exact_key = gen0_key.clone();
        let worker1 = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            let tasks = b1
                .list_adaptive_tasks(&cc_lb_scheduler::worker::Filter {
                    status: Some(cc_lb_scheduler::worker::TaskStatus::Pending),
                    page: 1,
                    page_size: Some(20),
                })
                .await
                .expect("list");
            let matching: Vec<_> = tasks
                .into_iter()
                .filter(|row| {
                    matches!(
                        &row.args,
                        cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(j) if j.upstream_id == upstream_id
                    )
                })
                .collect();
            assert_eq!(
                matching.len(),
                1,
                "must expedite existing exact generation job without creating a duplicate"
            );
            assert_eq!(
                matching[0].idempotency_key.as_deref(),
                Some(exact_key.as_str())
            );

            let current = UpstreamStore::get_by_id(s1.as_ref(), upstream_id)
                .await
                .expect("get")
                .expect("exists");
            let encrypted =
                cc_lb_aead::EncryptedOAuthTokens::encrypt(&a1, &bundle1, upstream_id.as_bytes())
                    .expect("encrypt");
            UpstreamStore::store_oauth_tokens(
                s1.as_ref(),
                upstream_id,
                current.revision,
                encrypted,
                false,
            )
            .await
            .expect("bump gen0 -> gen1");
        });
        cc_lb_signer_anthropic_oauth::LazyRefreshHandle::refresh_one(&refresher, upstream_id)
            .await
            .expect("joins exact generation job and succeeds once generation bumps");
        worker1.await.expect("worker1 done");

        // 2. Pre-seed `:watchdog:` subkeyed job on the new generation: refresher joins it without pushing another row.
        let after_gen1 = UpstreamStore::get_by_id(storage_arc.as_ref(), upstream_id)
            .await
            .expect("get")
            .expect("exists");
        let gen1 = after_gen1.oauth_token_generation;
        assert!(gen1 > gen0);
        let job1 = cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob::for_credential(
            upstream_id,
            gen1,
            expires_at,
        );
        let watchdog_key = format!(
            "{}:watchdog:test",
            job1.generation_idempotency_key(gen1, expires_at)
        );
        backend
            .push_adaptive_task(cc_lb_scheduler::worker::SchedulerPushTask {
                args: cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(job1),
                idempotency_key: Some(watchdog_key.clone()),
                run_at_unix_secs: None,
                max_attempts: None,
            })
            .await
            .expect("seed watchdog job");

        let b2 = backend.clone();
        let s2 = storage_arc.clone();
        let a2 = aead.clone();
        let bundle2 = bundle.clone();
        let expected_watchdog_key = watchdog_key.clone();
        let worker2 = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            let tasks = b2
                .list_adaptive_tasks(&cc_lb_scheduler::worker::Filter {
                    status: Some(cc_lb_scheduler::worker::TaskStatus::Pending),
                    page: 1,
                    page_size: Some(20),
                })
                .await
                .expect("list");
            let matching: Vec<_> = tasks
                .into_iter()
                .filter(|row| {
                    matches!(
                        &row.args,
                        cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(j)
                            if j.upstream_id == upstream_id && j.expected_generation == Some(gen1)
                    )
                })
                .collect();
            assert_eq!(
                matching.len(),
                1,
                "must join existing active :watchdog: job without enqueuing a duplicate row: {:?}",
                matching
                    .iter()
                    .map(|r| r.idempotency_key.as_deref())
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                matching[0].idempotency_key.as_deref(),
                Some(expected_watchdog_key.as_str())
            );

            let current = UpstreamStore::get_by_id(s2.as_ref(), upstream_id)
                .await
                .expect("get")
                .expect("exists");
            let encrypted =
                cc_lb_aead::EncryptedOAuthTokens::encrypt(&a2, &bundle2, upstream_id.as_bytes())
                    .expect("encrypt");
            UpstreamStore::store_oauth_tokens(
                s2.as_ref(),
                upstream_id,
                current.revision,
                encrypted,
                false,
            )
            .await
            .expect("bump gen1 -> gen2");
        });
        cc_lb_signer_anthropic_oauth::LazyRefreshHandle::refresh_one(&refresher, upstream_id)
            .await
            .expect("joins :watchdog: job and succeeds once generation bumps");
        worker2.await.expect("worker2 done");
    }

    #[tokio::test]
    async fn split_admin_fire_now_reports_503_expired_before_start_and_504_outcome_unknown_with_audit()
     {
        use cc_lb_storage_api::{AuditQueryScope, AuditStore as _};

        struct NoopSplitSignerFactory;

        impl cc_lb_upstream::ApiKeyAwareSignerFactory for NoopSplitSignerFactory {
            fn with_router_choice(
                &self,
                _router_chosen_upstream_name: String,
            ) -> Arc<dyn cc_lb_upstream::SignerFactory> {
                Arc::new(NoopSplitSignerFactory)
            }
        }

        #[async_trait::async_trait]
        impl cc_lb_upstream::SignerFactory for NoopSplitSignerFactory {
            async fn build(
                &self,
                _upstream: &cc_lb_domain::Upstream,
            ) -> Result<Arc<dyn cc_lb_upstream::Signer>, cc_lb_upstream::SignerError> {
                Err(cc_lb_upstream::SignerError::MissingCredentials {
                    reason: "noop".to_owned(),
                })
            }
        }

        #[allow(unsafe_code)]
        unsafe {
            std::env::set_var(TEST_TOKEN_ENV, TEST_TOKEN);
        }

        let dir = tempfile::TempDir::new().expect("tempdir");
        let sqlite_path = dir.path().join("storage.sqlite");
        let aead = Arc::new(AeadService::from_master_key([0u8; 32]));
        let test_clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
        let clock: ClockHandle = test_clock.clone();
        let database_url = format!("sqlite://{}", sqlite_path.display());
        let storage_arc = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
                .await
                .expect("open sqlite"),
        );
        storage_arc.initialize().await.expect("init sqlite");

        let upstream_id = Uuid::new_v4();
        let bundle = cc_lb_aead::OAuthTokenBundle {
            access_token: "access".to_owned(),
            refresh_token: "refresh".to_owned(),
            expires_at_unix_secs: 1_900_000_000,
            refresh_token_expires_at_unix_secs: None,
            account_id: Some("acct".to_owned()),
            project_id: None,
            oauth_profile: cc_lb_aead::GeminiOAuthProfile::default(),
            scopes: Vec::new(),
            never_refresh: false,
        };
        UpstreamStore::create(
            storage_arc.as_ref(),
            UpstreamCreate {
                id: upstream_id,
                name: "split-fire-now-errors".to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: Some(
                    url::Url::parse("https://api.anthropic.com/").expect("valid base url"),
                ),
                api_key_ciphertext: None,
                oauth_tokens: Some(cc_lb_storage_api::OAuthTokensCreate {
                    tokens: cc_lb_aead::EncryptedOAuthTokens::encrypt(
                        &aead,
                        &bundle,
                        upstream_id.as_bytes(),
                    )
                    .expect("encrypt"),
                    never_refresh: false,
                }),
                warmup_enabled: true,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
                vertex: None,
                vertex_credential_type: None,
                vertex_quota_project_id: None,
            },
        )
        .await
        .expect("seed oauth upstream");

        let storage_cfg = StorageConfig::Sqlite {
            path: sqlite_path.clone(),
        };
        let worker_scheduler = cc_lb_scheduler::factory::open_scheduler_storage_with_role(
            &storage_cfg,
            &cc_lb_config::SchedulerConfig::default(),
            cc_lb_scheduler::factory::SchedulerPoolRole::Worker,
            clock.clone(),
        )
        .await
        .expect("open worker scheduler");
        let worker_backend = worker_scheduler.lazy_handle();

        let mut cfg = Config {
            storage: storage_cfg,
            ..Config::default()
        };
        cfg.runtime.data_dir = Some(dir.path().to_path_buf());
        cfg.admin.auth.providers = vec![AdminAuthProviderConfig::StaticToken {
            id: "split-test-token".to_owned(),
            token_env: TEST_TOKEN_ENV.to_owned(),
        }];
        let admin_auth = Arc::new(cc_lb_admin::auth::AdminAuthenticator::new(
            cc_lb_admin::auth::build_providers(&cfg.admin.auth).expect("auth providers"),
        ));
        let post_commit_tracker = cc_lb_control::PostCommitTracker::default();
        let split_manual_warmup = Arc::new(
            cc_lb_admin_server::split_ports::SplitSchedulerManualWarmup::new(
                storage_arc.clone(),
                worker_backend.clone(),
                clock.clone(),
            )
            .with_timeout(
                std::time::Duration::from_millis(80),
                std::time::Duration::from_millis(10),
            ),
        );
        let dynamic_view = Arc::new(cc_lb_control::DynamicViewHolder::new(
            cc_lb_control::DynamicViewBuilder::new(0)
                .signer_factory(Arc::new(NoopSplitSignerFactory))
                .principal_view(Arc::new(
                    cc_lb_control::api_keys::principal_view::PrincipalView::from_db(
                        &[],
                        std::collections::HashMap::new(),
                    ),
                ))
                .upstream_status_snapshot(
                    Arc::new(cc_lb_control::UpstreamStatusSnapshot::default()),
                )
                .build(),
        ));
        let state = cc_lb_admin::AdminState {
            build: cc_lb_admin::AdminBuildInfo {
                git_sha: "test",
                rustc: "test",
                profile: "test",
                target: "test",
            },
            config_path: None,
            startup_config_overrides: Default::default(),
            storage: Some(storage_arc.clone()),
            key_store: Some(Arc::new(cc_lb_control::api_keys::key_store::KeyStore::new(
                storage_arc.clone(),
            ))),
            aead,
            lifecycle: Some(cc_lb_admin::AdminPorts {
                warmup: None,
                subscription_quota_ingestion: None,
                replica_identity: None,
                split_manual_warmup: Some(split_manual_warmup),
            }),
            lazy_refresher: None,
            runtime: None,
            data_dir: Some(dir.path().to_path_buf()),
            warmup_dialect_dispatcher: None,
            warmup_window_verifier: None,
            limit_reset_usage_refresh: None,
            dynamic_view,
            config: Arc::new(cfg),
            dynamic_view_rebinder: None,
            scheduler: None,
            admin_auth,
            start_time: std::time::Instant::now(),
            event_bus: None,
            storage_tail: cc_lb_admin::events::storage_tail_channel(),
            clock: clock.clone(),
            anthropic_org_api: cc_lb_control::anthropic_api_key::AnthropicOrgApi::production()
                .expect("anthropic org api"),
            post_commit_tracker: post_commit_tracker.clone(),
        };
        let router = cc_lb_admin::router(state);

        // 1. Advance TestClock past expiry while task remains Pending (attempts == 0) -> 503 warmup_expired_before_start + audit row.
        let b_expired = worker_backend.clone();
        let clock_advance = test_clock.clone();
        let expirer = tokio::spawn(async move {
            for _ in 0..100 {
                let tasks = b_expired
                    .list_adaptive_tasks(&cc_lb_scheduler::worker::Filter {
                        status: Some(cc_lb_scheduler::worker::TaskStatus::Pending),
                        page: 1,
                        page_size: Some(20),
                    })
                    .await
                    .expect("list");
                if tasks.iter().any(|row| {
                    matches!(
                        &row.args,
                        cc_lb_scheduler::worker::AdaptiveJob::ManualWarmup(j) if j.upstream_id == upstream_id
                    )
                }) {
                    clock_advance.advance_secs(5);
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            panic!("expected ManualWarmupJob to be queued");
        });

        let resp_503 = router
            .clone()
            .oneshot(
                Request::post(format!("/admin/v1/upstreams/{upstream_id}/warmup/fire-now"))
                    .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json!({ "lane": "primary" }).to_string()))
                    .expect("503 request"),
            )
            .await
            .expect("503 response");
        assert_eq!(resp_503.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body_503: Value = serde_json::from_slice(
            &resp_503
                .into_body()
                .collect()
                .await
                .expect("body")
                .to_bytes(),
        )
        .expect("json");
        assert_eq!(body_503["fired"], false);
        assert_eq!(body_503["error"], "warmup_expired_before_start");
        expirer.await.expect("expirer done");

        // 2. Without clock passing expiry when deadline elapses (indeterminate outcome) -> 504 warmup_outcome_unknown + audit row.
        let resp_504 = router
            .oneshot(
                Request::post(format!("/admin/v1/upstreams/{upstream_id}/warmup/fire-now"))
                    .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json!({ "lane": "primary" }).to_string()))
                    .expect("504 request"),
            )
            .await
            .expect("504 response");
        assert_eq!(resp_504.status(), StatusCode::GATEWAY_TIMEOUT);
        let body_504: Value = serde_json::from_slice(
            &resp_504
                .into_body()
                .collect()
                .await
                .expect("body")
                .to_bytes(),
        )
        .expect("json");
        assert_eq!(body_504["fired"], false);
        assert_eq!(body_504["error"], "warmup_outcome_unknown");

        post_commit_tracker.wait_idle().await;
        let audits = storage_arc
            .query_recent_audit(AuditQueryScope::All, 0, u64::MAX, 20, true)
            .await
            .expect("query audit rows");
        let statuses: Vec<(u16, Option<&str>, Option<&str>)> = audits
            .iter()
            .filter(|entry| entry.admin_action.as_deref() == Some("upstream_warmup_fire_now"))
            .map(|entry| {
                (
                    entry.status,
                    entry.actor_authority.as_deref(),
                    entry.actor_subject.as_deref(),
                )
            })
            .collect();
        assert!(
            statuses.contains(&(503, Some("static_token"), Some("split-test-token")))
                || statuses
                    .iter()
                    .any(|(s, _, subj)| *s == 503 && *subj == Some("split-test-token")),
            "expected 503 audit row attributed to split-test-token actor: {audits:?}"
        );
        assert!(
            statuses
                .iter()
                .any(|(s, _, subj)| *s == 504 && *subj == Some("split-test-token")),
            "expected 504 audit row attributed to split-test-token actor: {audits:?}"
        );
    }
}
