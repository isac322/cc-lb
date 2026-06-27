use std::sync::Arc;

use cc_lb_config::{PostgresPoolConfig, SchedulerConfig, StorageConfig};
use cc_lb_server::scheduler_factory::{
    SchedulerBackend, SchedulerFactoryError, open_scheduler_storage,
};

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn scheduler_factory_sqlite_happy_path_sets_up_tables_and_partial_index() {
    let directory = tempfile::tempdir().expect("tempdir is created");
    let storage = StorageConfig::Sqlite {
        path: directory.path().join("scheduler.sqlite"),
    };

    let opened = open_scheduler_storage(
        &storage,
        &SchedulerConfig::default(),
        Arc::new(cc_lb_core::SystemClock),
    )
        .await
        .expect("sqlite scheduler opens");

    #[allow(clippy::infallible_destructuring_match)]
    let sqlite = match opened.backend {
        SchedulerBackend::Sqlite(sqlite) => sqlite,
        #[cfg(feature = "postgres")]
        SchedulerBackend::Postgres(_) => panic!("expected sqlite backend"),
    };
    assert!(opened.leader_connection.is_none());
    let table_count: i64 = scheduler_sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'Jobs'",
    )
    .fetch_one(&sqlite.pool)
    .await
    .expect("Jobs table query succeeds");
    assert_eq!(table_count, 1);
    let index_sql: String = scheduler_sqlx::query_scalar(
        "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'idx_jobs_idempotency_key'",
    )
    .fetch_one(&sqlite.pool)
    .await
    .expect("partial index query succeeds");
    assert!(!index_sql.contains("WHERE"));
    sqlite.pool.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn scheduler_factory_sqlite_bad_path_returns_connection_failed() {
    let directory = tempfile::tempdir().expect("tempdir is created");
    let storage = StorageConfig::Sqlite {
        path: directory
            .path()
            .join("missing-parent")
            .join("scheduler.sqlite"),
    };

    let error = open_scheduler_storage(
        &storage,
        &SchedulerConfig::default(),
        Arc::new(cc_lb_core::SystemClock),
    )
        .await
        .expect_err("missing parent cannot open sqlite database");

    assert!(matches!(
        error,
        SchedulerFactoryError::ConnectionFailed { .. }
    ));
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn scheduler_factory_postgres_happy_path_sets_up_tables_index_and_leader() {
    let Some(admin_url) = postgres_url() else {
        eprintln!("SKIP: postgres URL unset");
        return;
    };
    if !is_safe_database_url(&admin_url) {
        eprintln!("SKIP: postgres URL is not local/test-like");
        return;
    }
    let database_name = format!("cc_lb_scheduler_factory_{}", uuid::Uuid::new_v4().simple());
    let admin_pool = scheduler_sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url)
        .await
        .expect("admin postgres connects");
    let create_database = format!(r#"CREATE DATABASE "{database_name}""#);
    if let Err(error) = scheduler_sqlx::query(&create_database)
        .execute(&admin_pool)
        .await
    {
        eprintln!("SKIP: could not create temporary postgres database: {error}");
        admin_pool.close().await;
        return;
    }

    let database_url = database_url_for(&admin_url, &database_name).expect("database URL is valid");
    let storage = StorageConfig::Postgres {
        url: database_url,
        pool: PostgresPoolConfig::default(),
    };
    let opened = open_scheduler_storage(
        &storage,
        &SchedulerConfig::default(),
        Arc::new(cc_lb_core::SystemClock),
    )
        .await
        .expect("postgres scheduler opens");
    let SchedulerBackend::Postgres(postgres) = opened.backend else {
        panic!("expected postgres backend")
    };
    let leader = opened.leader_connection.expect("leader connection exists");
    assert!(
        leader
            .election
            .try_acquire()
            .await
            .expect("leader acquires")
    );
    assert!(leader.election.release().await.expect("leader releases"));
    let table_count: i64 = scheduler_sqlx::query_scalar("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'apalis' AND table_name = 'jobs'").fetch_one(&postgres.pool).await.expect("jobs table query succeeds");
    assert_eq!(table_count, 1);
    let predicate: Option<String> = scheduler_sqlx::query_scalar("SELECT pg_get_expr(indexes.indpred, indexes.indrelid) FROM pg_index indexes JOIN pg_class classes ON classes.oid = indexes.indexrelid JOIN pg_namespace namespaces ON namespaces.oid = classes.relnamespace WHERE namespaces.nspname = 'apalis' AND classes.relname = 'idx_jobs_idempotency_key'").fetch_one(&postgres.pool).await.expect("idempotency index query succeeds");
    assert!(predicate.is_none());
    drop(leader);
    postgres.pool.close().await;
    let drop_database = format!(r#"DROP DATABASE IF EXISTS "{database_name}""#);
    scheduler_sqlx::query(&drop_database)
        .execute(&admin_pool)
        .await
        .expect("temporary database drops");
    admin_pool.close().await;
}

#[cfg(not(feature = "postgres"))]
#[tokio::test]
async fn scheduler_factory_postgres_feature_disabled_returns_error() {
    let storage = StorageConfig::Postgres {
        url: "postgres://localhost/cc_lb".to_owned(),
        pool: PostgresPoolConfig::default(),
    };

    let error = open_scheduler_storage(
        &storage,
        &SchedulerConfig::default(),
        Arc::new(cc_lb_core::SystemClock),
    )
        .await
        .expect_err("postgres feature is disabled");

    assert!(matches!(
        error,
        SchedulerFactoryError::FeatureDisabled { backend } if backend == "postgres"
    ));
}

#[cfg(not(feature = "sqlite"))]
#[tokio::test]
async fn scheduler_factory_sqlite_feature_disabled_returns_error() {
    let storage = StorageConfig::Sqlite {
        path: std::path::PathBuf::from("scheduler.sqlite"),
    };

    let error = open_scheduler_storage(
        &storage,
        &SchedulerConfig::default(),
        Arc::new(cc_lb_core::SystemClock),
    )
        .await
        .expect_err("sqlite feature is disabled");

    assert!(matches!(
        error,
        SchedulerFactoryError::FeatureDisabled { backend } if backend == "sqlite"
    ));
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn scheduler_factory_postgres_bad_url_returns_connection_failed() {
    let storage = StorageConfig::Postgres {
        url: "not-a-postgres-url".to_owned(),
        pool: PostgresPoolConfig::default(),
    };

    let error = open_scheduler_storage(
        &storage,
        &SchedulerConfig::default(),
        Arc::new(cc_lb_core::SystemClock),
    )
        .await
        .expect_err("invalid postgres URL cannot open");

    assert!(matches!(
        error,
        SchedulerFactoryError::ConnectionFailed { .. }
    ));
}

#[cfg(feature = "postgres")]
fn postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL")
        .or_else(|_| std::env::var("DATABASE_URL_TEST"))
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok()
}

#[cfg(feature = "postgres")]
fn is_safe_database_url(url: &str) -> bool {
    url.contains("localhost") || url.contains("127.0.0.1") || url.contains("cc_lb_test")
}

#[cfg(feature = "postgres")]
fn database_url_for(admin_url: &str, database_name: &str) -> Option<String> {
    let mut url = url::Url::parse(admin_url).ok()?;
    url.set_path(&format!("/{database_name}"));
    Some(url.to_string())
}
