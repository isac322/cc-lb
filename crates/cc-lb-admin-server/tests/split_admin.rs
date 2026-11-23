#[cfg(feature = "sqlite")]
mod sqlite_tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use cc_lb_admin_server::build_admin_app_with_storage;
    use cc_lb_aead::AeadService;
    use cc_lb_clock::ClockHandle;
    use cc_lb_config::{AdminAuthProviderConfig, Config, StorageConfig};
    use cc_lb_storage_api::upstream::{UpstreamCreate, UpstreamKind};
    use cc_lb_storage_api::{ManagedKeyStore, MetaStore as _, Storage, UpstreamStore};
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

        UpstreamStore::create(
            storage_arc.as_ref(),
            UpstreamCreate {
                id: Uuid::new_v4(),
                name: "split-seeded-upstream".to_owned(),
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
        .expect("seed upstream");

        let mut config = Config {
            storage: StorageConfig::Sqlite {
                path: sqlite_path.clone(),
            },
            ..Config::default()
        };
        config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
        config.runtime.data_dir = Some(dir.path().to_path_buf());
        config.admin.auth.providers = vec![AdminAuthProviderConfig::StaticToken {
            id: "split-test-token".to_owned(),
            token_env: TEST_TOKEN_ENV.to_owned(),
        }];
        let raw_toml =
            Config::partial_json_toml(serde_json::to_value(&config).expect("config json"))
                .expect("config toml");
        std::fs::write(&config_path, &raw_toml).expect("write writable config file");

        let managed_store: Arc<dyn ManagedKeyStore> = storage_arc.clone();
        let storage: Arc<dyn Storage> = storage_arc;
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

        // 3. DB-only /admin/v1/status falls back to storage and lists the seeded upstream.
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
        assert!(
            upstreams
                .iter()
                .any(|u| u["name"] == "split-seeded-upstream"),
            "expected split-seeded-upstream in DB fallback status: {status_json}"
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
}
