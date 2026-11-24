#[cfg(feature = "sqlite")]
mod sqlite_tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use cc_lb_aead::AeadService;
    use cc_lb_clock::ClockHandle;
    use cc_lb_config::Config;
    use cc_lb_proxy::cli::{Cli, CliProxyRole, Command, resolve_role};
    use cc_lb_server::{ProxyApp, ProxyRole, build_proxy_app_with_storage};
    use cc_lb_storage_api::{ManagedKeyStore, MetaStore as _, Storage};
    use clap::Parser as _;
    use tower::ServiceExt as _;

    async fn build_test_proxy(role: ProxyRole) -> (tempfile::TempDir, ProxyApp) {
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
        let managed_store: Arc<dyn ManagedKeyStore> = storage_arc.clone();
        let storage: Arc<dyn Storage> = storage_arc;
        let mut config = Config {
            storage: cc_lb_config::StorageConfig::Sqlite { path: sqlite_path },
            ..Config::default()
        };
        config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
        config.runtime.data_dir = Some(dir.path().to_path_buf());
        let app = build_proxy_app_with_storage(config, role, managed_store, storage, aead, clock)
            .await
            .expect("build_proxy_app_with_storage");
        (dir, app)
    }

    #[tokio::test]
    async fn proxy_only_serves_health_and_auth_first_proxy_routes_without_admin() {
        let cli = Cli::try_parse_from([
            "cc-lb-proxy",
            "serve",
            "--config",
            "/tmp/c.toml",
            "--role",
            "proxy",
        ])
        .expect("parse --role proxy");
        let Some(Command::Serve {
            role, no_worker, ..
        }) = cli.command
        else {
            panic!("expected serve command");
        };
        assert_eq!(role, Some(CliProxyRole::Proxy));
        let resolved = resolve_role(role, no_worker).expect("resolve role");
        assert_eq!(resolved, ProxyRole::ProxyOnly);

        let (_dir, app) = build_test_proxy(resolved).await;

        let health = app
            .router
            .clone()
            .oneshot(
                Request::get("/healthz")
                    .body(Body::empty())
                    .expect("healthz request"),
            )
            .await
            .expect("healthz response");
        assert_eq!(health.status(), StatusCode::OK);

        let messages = app
            .router
            .clone()
            .oneshot(
                Request::post("/v1/messages")
                    .body(Body::empty())
                    .expect("messages request"),
            )
            .await
            .expect("messages response");
        assert_eq!(messages.status(), StatusCode::UNAUTHORIZED);

        let admin = app
            .router
            .oneshot(
                Request::get("/admin/health")
                    .body(Body::empty())
                    .expect("admin request"),
            )
            .await
            .expect("admin response");
        assert_eq!(admin.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn worker_only_serves_health_only_without_proxy_or_admin() {
        let (_dir, app) = build_test_proxy(ProxyRole::WorkerOnly).await;

        let health = app
            .router
            .clone()
            .oneshot(
                Request::get("/healthz")
                    .body(Body::empty())
                    .expect("healthz request"),
            )
            .await
            .expect("healthz response");
        assert_eq!(health.status(), StatusCode::OK);

        let messages = app
            .router
            .clone()
            .oneshot(
                Request::post("/v1/messages")
                    .body(Body::empty())
                    .expect("messages request"),
            )
            .await
            .expect("messages response");
        assert_eq!(messages.status(), StatusCode::NOT_FOUND);

        let admin = app
            .router
            .oneshot(
                Request::get("/admin/health")
                    .body(Body::empty())
                    .expect("admin request"),
            )
            .await
            .expect("admin response");
        assert_eq!(admin.status(), StatusCode::NOT_FOUND);
    }
}
