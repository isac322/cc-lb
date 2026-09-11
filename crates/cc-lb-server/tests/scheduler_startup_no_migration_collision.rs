#![cfg(feature = "sqlite")]

use cc_lb_config::{SchedulerConfig, StorageConfig};
use cc_lb_server::scheduler_factory::{SchedulerBackend, open_scheduler_storage};
use cc_lb_storage_api::{BackendKind, MetaStore};

#[tokio::test]
async fn t3__sqlite_main_storage_and_apalis_setup_share_file_without_migration_collision() {
    let directory = tempfile::tempdir().expect("tempdir is created");
    let database_path = directory.path().join("cc-lb.sqlite");
    let database_url = format!("sqlite://{}", database_path.display());

    let clock = cc_lb_testkit::fixed_clock(1_700_000_000);
    let main_storage = cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
        .await
        .expect("main sqlite storage opens");
    main_storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("main sqlite migrations run");

    let main_migration_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
        .fetch_one(main_storage.pool())
        .await
        .expect("main sqlx migration rows remain readable");
    assert!(main_migration_rows > 0);

    let storage_config = StorageConfig::Sqlite {
        path: database_path,
    };
    let opened_scheduler =
        open_scheduler_storage(&storage_config, &SchedulerConfig::default(), clock)
            .await
            .expect("scheduler storage opens after main migrations using derived sqlite file");

    #[allow(clippy::infallible_destructuring_match)]
    let sqlite_scheduler = match opened_scheduler.backend {
        SchedulerBackend::Sqlite(sqlite_scheduler) => sqlite_scheduler,
        #[cfg(feature = "postgres")]
        SchedulerBackend::Postgres(_) => panic!("expected sqlite scheduler backend"),
    };

    let main_migration_rows_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(main_storage.pool())
            .await
            .expect("main sqlx migration rows remain after apalis setup");
    assert_eq!(main_migration_rows_after, main_migration_rows);

    let main_jobs_table_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'Jobs'",
    )
    .fetch_one(main_storage.pool())
    .await
    .expect("main sqlite file remains free of apalis Jobs table");
    assert_eq!(main_jobs_table_count, 0);

    for table_name in ["Jobs", "Workers"] {
        let exists: i64 = scheduler_sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(table_name)
        .fetch_one(sqlite_scheduler.pool())
        .await
        .expect("apalis table existence query succeeds");
        assert_eq!(exists, 1, "missing apalis table {table_name}");
    }
}
