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
        Arc::new(cc_lb_clock::SystemClock),
    )
    .await
    .expect("sqlite scheduler opens");

    #[allow(clippy::infallible_destructuring_match)]
    let sqlite = match opened.backend {
        SchedulerBackend::Sqlite(sqlite) => sqlite,
        #[cfg(feature = "postgres")]
        SchedulerBackend::Postgres(_) => panic!("expected sqlite backend"),
    };
    let table_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'Jobs'",
    )
    .fetch_one(sqlite.pool())
    .await
    .expect("Jobs table query succeeds");
    assert_eq!(table_count, 1);
    let index_sql: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'idx_jobs_idempotency_key'",
    )
    .fetch_one(sqlite.pool())
    .await
    .expect("partial index query succeeds");
    assert!(!index_sql.contains("WHERE"));
    let synchronous: i64 = sqlx::query_scalar("PRAGMA synchronous")
        .fetch_one(sqlite.pool())
        .await
        .expect("PRAGMA synchronous query succeeds");
    assert_eq!(
        synchronous, 1,
        "post-setup migrations must restore PRAGMA synchronous = NORMAL (1)"
    );
    sqlite.pool().close().await;
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
        Arc::new(cc_lb_clock::SystemClock),
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
async fn scheduler_factory_postgres_happy_path_sets_up_tables_and_index() {
    let Some(admin_url) = postgres_url() else {
        eprintln!("SKIP: postgres URL unset");
        return;
    };
    if !is_safe_database_url(&admin_url) {
        eprintln!("SKIP: postgres URL is not local/test-like");
        return;
    }
    let database_name = format!("cc_lb_scheduler_factory_{}", uuid::Uuid::new_v4().simple());
    let admin_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url)
        .await
        .expect("admin postgres connects");
    let create_database = format!(r#"CREATE DATABASE "{database_name}""#);
    if let Err(error) = sqlx::query(sqlx::AssertSqlSafe(create_database))
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
        Arc::new(cc_lb_clock::SystemClock),
    )
    .await
    .expect("postgres scheduler opens");
    let SchedulerBackend::Postgres(postgres) = opened.backend else {
        panic!("expected postgres backend")
    };
    let table_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'apalis' AND table_name = 'jobs'").fetch_one(postgres.pool()).await.expect("jobs table query succeeds");
    assert_eq!(table_count, 1);
    let apalis_ledger_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM apalis._sqlx_migrations WHERE success")
            .fetch_one(postgres.pool())
            .await
            .expect("apalis._sqlx_migrations count succeeds");
    assert_eq!(apalis_ledger_rows, 23);
    let legacy_ledger_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('cc_lb_scheduler._sqlx_migrations') IS NOT NULL")
            .fetch_one(postgres.pool())
            .await
            .expect("legacy ledger check succeeds");
    assert!(!legacy_ledger_exists);
    let hstore_schema: String = sqlx::query_scalar(
        "SELECT namespaces.nspname
         FROM pg_extension extensions
         JOIN pg_namespace namespaces ON namespaces.oid = extensions.extnamespace
         WHERE extensions.extname = 'hstore'",
    )
    .fetch_one(postgres.pool())
    .await
    .expect("hstore extension namespace query succeeds");
    assert_eq!(hstore_schema, "cc_lb_scheduler");
    let predicate: Option<String> = sqlx::query_scalar("SELECT pg_get_expr(indexes.indpred, indexes.indrelid) FROM pg_index indexes JOIN pg_class classes ON classes.oid = indexes.indexrelid JOIN pg_namespace namespaces ON namespaces.oid = classes.relnamespace WHERE namespaces.nspname = 'apalis' AND classes.relname = 'idx_jobs_idempotency_key'").fetch_one(postgres.pool()).await.expect("idempotency index query succeeds");
    assert!(predicate.is_none());
    postgres.pool().close().await;
    let drop_database = format!(r#"DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)"#);
    sqlx::query(sqlx::AssertSqlSafe(drop_database))
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
        Arc::new(cc_lb_clock::SystemClock),
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
        Arc::new(cc_lb_clock::SystemClock),
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
        Arc::new(cc_lb_clock::SystemClock),
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

#[cfg(feature = "postgres")]
const PRE_1_0_LAST_MIGRATION_VERSION: i64 = 20260508093314;
#[cfg(feature = "postgres")]
const PRE_1_0_FIRST_MIGRATION_VERSION: i64 = 20220530084123;
#[cfg(feature = "postgres")]
const PRE_1_0_FIRST_MIGRATION_SHA384_HEX: &str = "0e2ae5fd117c32925d7ff4ba4f26368269fba7706bf1c3e9578a763a3e2acc5fd44c8edf2aa25e7c0ac1c6ebc8e95a9a";

#[cfg(feature = "postgres")]
async fn with_temporary_postgres_database<F, Fut>(test_fn: F)
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = ()>,
{
    let Some(admin_url) = postgres_url() else {
        eprintln!("SKIP: postgres URL unset");
        return;
    };
    if !is_safe_database_url(&admin_url) {
        eprintln!("SKIP: postgres URL is not local/test-like");
        return;
    }
    let database_name = format!("cc_lb_scheduler_factory_{}", uuid::Uuid::new_v4().simple());
    let admin_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url)
        .await
        .expect("admin postgres connects");
    let create_database = format!(r#"CREATE DATABASE "{database_name}""#);
    if let Err(error) = sqlx::query(sqlx::AssertSqlSafe(create_database))
        .execute(&admin_pool)
        .await
    {
        eprintln!("SKIP: could not create temporary postgres database: {error}");
        admin_pool.close().await;
        return;
    }
    let database_url = database_url_for(&admin_url, &database_name).expect("database URL is valid");
    let outcome =
        futures_util::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(test_fn(database_url)))
            .await;
    let drop_database = format!(r#"DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)"#);
    let _ = sqlx::query(sqlx::AssertSqlSafe(drop_database))
        .execute(&admin_pool)
        .await;
    admin_pool.close().await;
    if let Err(payload) = outcome {
        std::panic::resume_unwind(payload);
    }
}

#[cfg(feature = "postgres")]
async fn seed_pre_1_0_apalis_scheduler_database(database_url: &str) {
    use cc_lb_storage_api::MetaStore as _;
    use sha2::Digest as _;
    use sqlx::Connection as _;

    let main_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
        .expect("main postgres pool connects");
    let main_storage =
        cc_lb_storage_postgres::PostgresStorage::new(main_pool, Arc::new(cc_lb_clock::SystemClock));
    main_storage
        .initialize()
        .await
        .expect("main postgres migrations succeed");
    main_storage.pool().close().await;

    let mut conn = sqlx::PgConnection::connect(database_url)
        .await
        .expect("seed postgres connection opens");
    sqlx::query("SET search_path = cc_lb_scheduler, apalis, public")
        .execute(&mut conn)
        .await
        .expect("set scheduler search_path");
    sqlx::query("CREATE SCHEMA IF NOT EXISTS cc_lb_scheduler")
        .execute(&mut conn)
        .await
        .expect("create cc_lb_scheduler schema");
    sqlx::query(
        "CREATE TABLE cc_lb_scheduler._sqlx_migrations (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
            success BOOLEAN NOT NULL,
            checksum BYTEA NOT NULL,
            execution_time BIGINT NOT NULL
        )",
    )
    .execute(&mut conn)
    .await
    .expect("create legacy cc_lb_scheduler._sqlx_migrations");

    let migrator = apalis_postgres::PostgresStorage::<()>::migrations();
    let pre_1_0_migrations: Vec<_> = migrator
        .iter()
        .filter(|migration| migration.version <= PRE_1_0_LAST_MIGRATION_VERSION)
        .collect();
    assert_eq!(pre_1_0_migrations.len(), 19);

    for migration in pre_1_0_migrations {
        let raw_migration_sql = migration.sql.as_str();
        let sql = if migration.version == PRE_1_0_FIRST_MIGRATION_VERSION {
            let legacy_sql = raw_migration_sql.replacen(
                "CREATE SCHEMA IF NOT EXISTS apalis;",
                "CREATE SCHEMA apalis;",
                1,
            );
            let digest_hex = hex::encode(sha2::Sha384::digest(legacy_sql.as_bytes()));
            assert_eq!(digest_hex, PRE_1_0_FIRST_MIGRATION_SHA384_HEX);
            legacy_sql
        } else {
            raw_migration_sql.to_owned()
        };
        let checksum = sha2::Sha384::digest(sql.as_bytes()).to_vec();
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
            .execute(&mut conn)
            .await
            .expect("pre-1.0 apalis migration SQL executes");
        sqlx::query(
            "INSERT INTO cc_lb_scheduler._sqlx_migrations
             (version, description, success, checksum, execution_time)
             VALUES ($1, $2, TRUE, $3, 1)",
        )
        .bind(migration.version)
        .bind(migration.description.as_ref())
        .bind(checksum)
        .execute(&mut conn)
        .await
        .expect("insert pre-1.0 migration ledger row");
    }
    conn.close().await.expect("close seed connection");

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|connection, _meta| {
            Box::pin(async move {
                sqlx::query("SET search_path = cc_lb_scheduler, apalis, public")
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        })
        .connect(database_url)
        .await
        .expect("connect post-setup pool");
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool)
        .await
        .expect("pre-upgrade post-setup migrations succeed");
    sqlx::query(
        "INSERT INTO apalis.jobs (job, id, job_type, status, idempotency_key, metadata)
         VALUES
           ($1, 'job-null-meta', 'adaptive', 'Pending', 'adaptive:test:1', NULL),
           ($1, 'job-empty-meta', 'adaptive', 'Pending', 'adaptive:test:2', '{}'::jsonb),
           ($1, 'job-kv-meta', 'adaptive', 'Done', 'adaptive:test:3', '{\"traceparent\":\"00-abc\"}'::jsonb)",
    )
    .bind(br#"{"type":"price_catalog_refresh"}"#.to_vec())
    .execute(&pool)
    .await
    .expect("seed pre-1.0 apalis.jobs rows");
    pool.close().await;
}

#[cfg(feature = "postgres")]
async fn create_empty_apalis_migrations_table(database_url: &str) {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
        .expect("connect for empty apalis._sqlx_migrations");
    sqlx::raw_sql(
        "CREATE SCHEMA IF NOT EXISTS apalis;
         CREATE TABLE apalis._sqlx_migrations (
             version BIGINT PRIMARY KEY,
             description TEXT NOT NULL,
             installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
             success BOOLEAN NOT NULL,
             checksum BYTEA NOT NULL,
             execution_time BIGINT NOT NULL
         );",
    )
    .execute(&pool)
    .await
    .expect("create empty apalis._sqlx_migrations");
    pool.close().await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn scheduler_factory_postgres_upgrades_pre_1_0_apalis_ledger_and_restarts_cleanly() {
    with_temporary_postgres_database(|database_url| async move {
        seed_pre_1_0_apalis_scheduler_database(&database_url).await;

        let inspect_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("inspect pool connects");
        let public_rows_before: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM public._sqlx_migrations")
                .fetch_one(&inspect_pool)
                .await
                .expect("read public._sqlx_migrations");
        assert!(public_rows_before > 0);

        let storage = StorageConfig::Postgres {
            url: database_url.clone(),
            pool: PostgresPoolConfig::default(),
        };
        let opened = open_scheduler_storage(
            &storage,
            &SchedulerConfig::default(),
            Arc::new(cc_lb_clock::SystemClock),
        )
        .await
        .expect("upgrade from pre-1.0 apalis ledger succeeds");
        let SchedulerBackend::Postgres(postgres) = opened.backend else {
            panic!("expected postgres backend");
        };

        let legacy_exists: bool = sqlx::query_scalar(
            "SELECT to_regclass('cc_lb_scheduler._sqlx_migrations') IS NOT NULL",
        )
        .fetch_one(postgres.pool())
        .await
        .expect("check legacy ledger");
        assert!(!legacy_exists);
        let apalis_rows: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM apalis._sqlx_migrations WHERE success")
                .fetch_one(postgres.pool())
                .await
                .expect("count apalis._sqlx_migrations");
        assert_eq!(apalis_rows, 23);
        let public_rows_after: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM public._sqlx_migrations")
                .fetch_one(postgres.pool())
                .await
                .expect("recheck public._sqlx_migrations");
        assert_eq!(public_rows_after, public_rows_before);

        let null_meta: Option<String> =
            sqlx::query_scalar("SELECT metadata::text FROM apalis.jobs WHERE id = 'job-null-meta'")
                .fetch_one(postgres.pool())
                .await
                .expect("query null metadata row");
        assert_eq!(null_meta, None);
        let empty_meta: Option<String> = sqlx::query_scalar(
            "SELECT metadata::text FROM apalis.jobs WHERE id = 'job-empty-meta'",
        )
        .fetch_one(postgres.pool())
        .await
        .expect("query empty metadata row");
        assert_eq!(empty_meta, None);
        let kv_meta: Option<String> = sqlx::query_scalar(
            "SELECT metadata -> 'traceparent' FROM apalis.jobs WHERE id = 'job-kv-meta'",
        )
        .fetch_one(postgres.pool())
        .await
        .expect("query hstore metadata key");
        assert_eq!(kv_meta.as_deref(), Some("00-abc"));
        postgres.pool().close().await;

        // QA-04: second startup takes the fast no-op path and remains healthy.
        let reopened = open_scheduler_storage(
            &storage,
            &SchedulerConfig::default(),
            Arc::new(cc_lb_clock::SystemClock),
        )
        .await
        .expect("second startup after upgrade succeeds");
        let SchedulerBackend::Postgres(reopened_pg) = reopened.backend else {
            panic!("expected postgres backend");
        };
        let apalis_rows_second: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM apalis._sqlx_migrations WHERE success")
                .fetch_one(reopened_pg.pool())
                .await
                .expect("count apalis._sqlx_migrations after restart");
        assert_eq!(apalis_rows_second, 23);
        reopened_pg.pool().close().await;
        inspect_pool.close().await;
    })
    .await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn scheduler_factory_postgres_recovers_after_failed_2_2_0_empty_apalis_ledger() {
    with_temporary_postgres_database(|database_url| async move {
        seed_pre_1_0_apalis_scheduler_database(&database_url).await;
        create_empty_apalis_migrations_table(&database_url).await;

        let storage = StorageConfig::Postgres {
            url: database_url,
            pool: PostgresPoolConfig::default(),
        };
        let opened = open_scheduler_storage(
            &storage,
            &SchedulerConfig::default(),
            Arc::new(cc_lb_clock::SystemClock),
        )
        .await
        .expect("recovery from empty apalis._sqlx_migrations succeeds");
        let SchedulerBackend::Postgres(postgres) = opened.backend else {
            panic!("expected postgres backend");
        };
        let legacy_exists: bool = sqlx::query_scalar(
            "SELECT to_regclass('cc_lb_scheduler._sqlx_migrations') IS NOT NULL",
        )
        .fetch_one(postgres.pool())
        .await
        .expect("check legacy ledger");
        assert!(!legacy_exists);
        let apalis_rows: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM apalis._sqlx_migrations WHERE success")
                .fetch_one(postgres.pool())
                .await
                .expect("count apalis._sqlx_migrations");
        assert_eq!(apalis_rows, 23);
        postgres.pool().close().await;
    })
    .await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn scheduler_factory_postgres_fails_closed_when_both_ledgers_are_non_empty() {
    with_temporary_postgres_database(|database_url| async move {
        seed_pre_1_0_apalis_scheduler_database(&database_url).await;
        create_empty_apalis_migrations_table(&database_url).await;

        let inspect_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("inspect pool connects");
        sqlx::query(
            "INSERT INTO apalis._sqlx_migrations (version, description, success, checksum, execution_time)
             VALUES (1, 'conflicting row', TRUE, $1, 1)",
        )
        .bind(vec![0x11_u8; 48])
        .execute(&inspect_pool)
        .await
        .expect("insert conflicting apalis._sqlx_migrations row");

        let storage = StorageConfig::Postgres {
            url: database_url,
            pool: PostgresPoolConfig::default(),
        };
        let err = open_scheduler_storage(
            &storage,
            &SchedulerConfig::default(),
            Arc::new(cc_lb_clock::SystemClock),
        )
        .await
        .expect_err("dual non-empty ledgers must fail closed");
        assert!(
            matches!(&err, SchedulerFactoryError::MigrationFailed { message } if message.contains("both cc_lb_scheduler._sqlx_migrations") && message.contains("non-empty apalis._sqlx_migrations")),
            "unexpected error: {err:?}"
        );

        let legacy_rows: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM cc_lb_scheduler._sqlx_migrations")
                .fetch_one(&inspect_pool)
                .await
                .expect("legacy ledger preserved");
        assert_eq!(legacy_rows, 19);
        let apalis_rows: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM apalis._sqlx_migrations")
                .fetch_one(&inspect_pool)
                .await
                .expect("apalis ledger preserved");
        assert_eq!(apalis_rows, 1);
        inspect_pool.close().await;
    })
    .await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn scheduler_factory_postgres_rolls_back_ledger_move_on_unexpected_first_migration_checksum()
{
    with_temporary_postgres_database(|database_url| async move {
        seed_pre_1_0_apalis_scheduler_database(&database_url).await;
        create_empty_apalis_migrations_table(&database_url).await;

        let inspect_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("inspect pool connects");
        let tampered = vec![0xAB_u8; 48];
        sqlx::query("UPDATE cc_lb_scheduler._sqlx_migrations SET checksum = $1 WHERE version = $2")
            .bind(&tampered)
            .bind(PRE_1_0_FIRST_MIGRATION_VERSION)
            .execute(&inspect_pool)
            .await
            .expect("tamper first migration checksum");

        let storage = StorageConfig::Postgres {
            url: database_url,
            pool: PostgresPoolConfig::default(),
        };
        for _ in 0..2 {
            let err = open_scheduler_storage(
                &storage,
                &SchedulerConfig::default(),
                Arc::new(cc_lb_clock::SystemClock),
            )
            .await
            .expect_err("tampered checksum must fail closed on every boot");
            assert!(
                matches!(&err, SchedulerFactoryError::MigrationFailed { message } if message.contains("unexpected checksum for migration 20220530084123")),
                "unexpected error: {err:?}"
            );

            let preserved_checksum: Vec<u8> = sqlx::query_scalar(
                "SELECT checksum FROM cc_lb_scheduler._sqlx_migrations WHERE version = $1",
            )
            .bind(PRE_1_0_FIRST_MIGRATION_VERSION)
            .fetch_one(&inspect_pool)
            .await
            .expect("legacy ledger remains in cc_lb_scheduler");
            assert_eq!(preserved_checksum, tampered);
        }
        inspect_pool.close().await;
    })
    .await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn scheduler_factory_postgres_drops_empty_legacy_ledger_on_fresh_database() {
    with_temporary_postgres_database(|database_url| async move {
        let seed_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("seed pool connects");
        sqlx::raw_sql(
            "CREATE SCHEMA IF NOT EXISTS cc_lb_scheduler;
             CREATE TABLE cc_lb_scheduler._sqlx_migrations (
                 version BIGINT PRIMARY KEY,
                 description TEXT NOT NULL,
                 installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
                 success BOOLEAN NOT NULL,
                 checksum BYTEA NOT NULL,
                 execution_time BIGINT NOT NULL
             );",
        )
        .execute(&seed_pool)
        .await
        .expect("seed empty legacy ledger");
        seed_pool.close().await;

        let storage = StorageConfig::Postgres {
            url: database_url,
            pool: PostgresPoolConfig::default(),
        };
        let opened = open_scheduler_storage(
            &storage,
            &SchedulerConfig::default(),
            Arc::new(cc_lb_clock::SystemClock),
        )
        .await
        .expect("empty legacy ledger is dropped and fresh 1.0 migrations succeed");
        let SchedulerBackend::Postgres(postgres) = opened.backend else {
            panic!("expected postgres backend");
        };
        let legacy_exists: bool = sqlx::query_scalar(
            "SELECT to_regclass('cc_lb_scheduler._sqlx_migrations') IS NOT NULL",
        )
        .fetch_one(postgres.pool())
        .await
        .expect("check legacy ledger");
        assert!(!legacy_exists);
        let apalis_rows: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM apalis._sqlx_migrations WHERE success")
                .fetch_one(postgres.pool())
                .await
                .expect("count apalis._sqlx_migrations");
        assert_eq!(apalis_rows, 23);
        postgres.pool().close().await;
    })
    .await;
}
