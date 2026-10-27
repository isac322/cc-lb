use std::{error::Error, sync::Arc};

use cc_lb_storage_api::{
    MetaStore, StorageError, UpstreamCreate, UpstreamStore, upstream::UpstreamKind,
};
use cc_lb_storage_sqlite::SqliteStorage;
use sha2::{Digest, Sha256};
use sqlx::{
    AssertSqlSafe, SqlSafeStr,
    migrate::{MigrateError, Migration, MigrationType, Migrator},
};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const HISTORICAL_0003: &str = include_str!("fixtures/99b32/0003_principals_upstreams.sql");
const HISTORICAL_LAST_VERSION: i64 = 98;

// Provenance: 99b32d4caa0dc243d9526f3f4984a9c03a45160a contains 94 SQLite
// migrations through 0098. Comparing every file with 0bdf277c556f58b9807acc01657b2eeace76de33
// finds exactly one changed historical file: 0003. The other 93 are byte-identical.
// The fixture is that commit's exact 0003 (Git blob
// 0f8fcb6af0d0588a868b80e7b3e394a713bc2282); no SQL is synthesized or tracker
// checksum overwritten. This fingerprint pins each version and SQLx-computed
// SHA-384 checksum, in version order, so later edits cannot silently change the
// historical database used by this test. SQLx itself creates all migration rows.
fn historical_migrator() -> Migrator {
    let current = sqlx::migrate!("./migrations");
    let migrations: Vec<_> = current
        .iter()
        .filter(|migration| migration.version <= HISTORICAL_LAST_VERSION)
        .map(|migration| {
            if migration.version == 3 {
                Migration::new(
                    migration.version,
                    migration.description.clone(),
                    migration.migration_type,
                    HISTORICAL_0003.into_sql_str(),
                    migration.no_tx,
                )
            } else {
                migration.clone()
            }
        })
        .collect();
    assert_eq!(migrations.len(), 94, "historical migration catalog changed");
    let mut fingerprint = Sha256::new();
    for migration in &migrations {
        fingerprint.update(migration.version.to_be_bytes());
        fingerprint.update(migration.checksum.as_ref());
    }
    assert_eq!(
        fingerprint.finalize().as_slice(),
        &[
            0x00, 0x80, 0x01, 0x16, 0x06, 0x99, 0x22, 0x64, 0x5f, 0x24, 0xc8, 0xd2, 0x49, 0xf9,
            0xb3, 0xdc, 0xc6, 0x2a, 0x4c, 0xfd, 0xcb, 0x73, 0xa6, 0x27, 0x61, 0xc0, 0x08, 0x10,
            0x58, 0xc4, 0x64, 0xb6,
        ],
        "historical fixture no longer matches the actual 99b32 migration bytes"
    );
    Migrator::with_migrations(migrations)
}

async fn open(database_url: &str) -> TestResult<SqliteStorage> {
    Ok(cc_lb_storage_sqlite::open_sqlite(database_url, Arc::new(cc_lb_clock::SystemClock)).await?)
}

async fn seed_historical_upstreams(storage: &SqliteStorage) -> TestResult {
    for (id, name, kind, deleted_at) in [
        (
            Uuid::from_u128(1),
            "historical-api",
            "anthropic_api_key",
            None,
        ),
        (
            Uuid::from_u128(2),
            "historical-oauth",
            "anthropic_oauth",
            Some(1_030_i64),
        ),
    ] {
        sqlx::query(
            "INSERT INTO upstream_spec_v1 (
                id, name, kind, base_url, enabled, warmup_enabled,
                warmup_dialect_plugin, spec_revision, created_at, updated_at,
                deleted_at, workspace_id
             ) VALUES (?, ?, ?, ?, 0, 1, NULL, 7, 1000, 1020, ?, ?)",
        )
        .bind(id.to_string())
        .bind(name)
        .bind(kind)
        .bind("https://migration.example.test")
        .bind(deleted_at)
        .bind("historical-workspace")
        .execute(storage.pool())
        .await?;
        sqlx::query(
            "INSERT INTO upstream_status_v1 (
                upstream_id, last_apply_error, last_apply_at,
                last_warmup_at, updated_at
             ) VALUES (?, 'historical-status', 1010, 1005, 1020)",
        )
        .bind(id.to_string())
        .execute(storage.pool())
        .await?;
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct DurableRows {
    specs: Vec<String>,
    statuses: Vec<String>,
}

async fn durable_rows(storage: &SqliteStorage) -> TestResult<DurableRows> {
    Ok(DurableRows {
        specs: sqlx::query_scalar(
            "SELECT json_array(id, name, kind, base_url, enabled, warmup_enabled,
                warmup_dialect_plugin, spec_revision, created_at, updated_at,
                deleted_at, workspace_id)
             FROM upstream_spec_v1 ORDER BY id",
        )
        .fetch_all(storage.pool())
        .await?,
        statuses: sqlx::query_scalar(
            "SELECT json_array(upstream_id, last_apply_error, last_apply_at,
                last_warmup_at, updated_at)
             FROM upstream_status_v1 ORDER BY upstream_id",
        )
        .fetch_all(storage.pool())
        .await?,
    })
}

async fn assert_foreign_keys(storage: &SqliteStorage) -> TestResult {
    let enabled: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(storage.pool())
        .await?;
    assert_eq!(enabled, 1, "startup must leave foreign keys enabled");
    let violations = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(storage.pool())
        .await?;
    assert!(
        violations.is_empty(),
        "foreign-key violations: {violations:?}"
    );
    Ok(())
}

async fn assert_provider_writes(storage: &SqliteStorage) -> TestResult {
    for kind in [
        UpstreamKind::AnthropicApiKey,
        UpstreamKind::AnthropicOauth,
        UpstreamKind::OpenaiApiKey,
        UpstreamKind::OpenaiOauth,
        UpstreamKind::GeminiApiKey,
        UpstreamKind::GeminiOauth,
    ] {
        let id = Uuid::new_v4();
        let record = UpstreamStore::create(
            storage,
            UpstreamCreate {
                id,
                name: format!("provider-{}", kind.as_str()),
                kind,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await?;
        assert_eq!(record.kind, kind);
        let stored = UpstreamStore::get_by_id(storage, id).await?;
        assert_eq!(stored.map(|record| record.kind), Some(kind));
    }
    Ok(())
}

async fn assert_reinitialize_is_noop(database_url: &str, storage: SqliteStorage) -> TestResult {
    let before = durable_rows(&storage).await?;
    let schema: Vec<String> = sqlx::query_scalar(
        "SELECT type || ':' || name || ':' || COALESCE(sql, '')
         FROM sqlite_master ORDER BY type, name",
    )
    .fetch_all(storage.pool())
    .await?;
    let applied: Vec<(i64, Vec<u8>)> = sqlx::query_as(
        "SELECT version, checksum FROM _sqlx_migrations WHERE success = 1 ORDER BY version",
    )
    .fetch_all(storage.pool())
    .await?;
    storage.pool().close().await;
    let reopened = open(database_url).await?;
    reopened.initialize().await?;
    assert_eq!(durable_rows(&reopened).await?, before);
    let after_schema: Vec<String> = sqlx::query_scalar(
        "SELECT type || ':' || name || ':' || COALESCE(sql, '')
         FROM sqlite_master ORDER BY type, name",
    )
    .fetch_all(reopened.pool())
    .await?;
    assert_eq!(after_schema, schema);
    let after_applied: Vec<(i64, Vec<u8>)> = sqlx::query_as(
        "SELECT version, checksum FROM _sqlx_migrations WHERE success = 1 ORDER BY version",
    )
    .fetch_all(reopened.pool())
    .await?;
    assert_eq!(after_applied, applied);
    assert_foreign_keys(&reopened).await?;
    reopened.pool().close().await;
    Ok(())
}

#[tokio::test]
async fn historical_database_initializes_and_preserves_upstreams() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let database_url = format!(
        "sqlite://{}",
        temp_dir.path().join("historical.sqlite").display()
    );
    let historical = open(&database_url).await?;
    historical_migrator().run(historical.pool()).await?;
    let version: i64 = sqlx::query_scalar("SELECT MAX(version) FROM _sqlx_migrations")
        .fetch_one(historical.pool())
        .await?;
    assert_eq!(version, HISTORICAL_LAST_VERSION);
    seed_historical_upstreams(&historical).await?;
    let before = durable_rows(&historical).await?;
    historical.pool().close().await;

    let storage = open(&database_url).await?;
    let initialized = storage.initialize().await;
    // Check persisted data even when startup fails, before propagating its error.
    assert_eq!(durable_rows(&storage).await?, before);
    assert_foreign_keys(&storage).await?;
    initialized?;
    let applied: Vec<i64> = sqlx::query_scalar(
        "SELECT version FROM _sqlx_migrations WHERE version IN (99, 100) AND success = 1 ORDER BY version",
    )
    .fetch_all(storage.pool())
    .await?;
    assert_eq!(applied, vec![99, 100]);
    assert_provider_writes(&storage).await?;
    assert_reinitialize_is_noop(&database_url, storage).await
}

#[tokio::test]
async fn fresh_database_initializes_and_stores_all_provider_kinds() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let database_url = format!(
        "sqlite://{}",
        temp_dir.path().join("fresh.sqlite").display()
    );
    let storage = open(&database_url).await?;
    storage.initialize().await?;
    assert_provider_writes(&storage).await?;
    assert_foreign_keys(&storage).await?;
    assert_reinitialize_is_noop(&database_url, storage).await
}

#[tokio::test]
async fn startup_rejects_genuinely_modified_migration() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let database_url = format!(
        "sqlite://{}",
        temp_dir.path().join("modified.sqlite").display()
    );
    let storage = open(&database_url).await?;
    let current = sqlx::migrate!("./migrations");
    let migrations = current
        .iter()
        .map(|migration| {
            if migration.version == 3 {
                Migration::new(
                    migration.version,
                    migration.description.clone(),
                    migration.migration_type,
                    AssertSqlSafe(format!(
                        "{}\n-- distinct historical migration\n",
                        migration.sql.as_str()
                    ))
                    .into_sql_str(),
                    migration.no_tx,
                )
            } else {
                migration.clone()
            }
        })
        .collect();
    Migrator::with_migrations(migrations)
        .run(storage.pool())
        .await?;
    let result = storage.initialize().await;
    assert!(
        matches!(&result, Err(StorageError::Fatal { message })
            if message == &MigrateError::VersionMismatch(3).to_string()),
        "modified migration must be rejected: {result:?}"
    );
    Ok(())
}

#[tokio::test]
async fn startup_rejects_genuinely_unknown_migration() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let database_url = format!(
        "sqlite://{}",
        temp_dir.path().join("unknown.sqlite").display()
    );
    let storage = open(&database_url).await?;
    let current = sqlx::migrate!("./migrations");
    let unknown_version = current
        .iter()
        .map(|migration| migration.version)
        .max()
        .ok_or("empty migration catalog")?
        + 1;
    let mut migrations: Vec<_> = current.iter().cloned().collect();
    migrations.push(Migration::new(
        unknown_version,
        "unknown schema change".into(),
        MigrationType::Simple,
        "CREATE TABLE unknown_schema_change (id INTEGER PRIMARY KEY);".into_sql_str(),
        false,
    ));
    Migrator::with_migrations(migrations)
        .run(storage.pool())
        .await?;
    let result = storage.initialize().await;
    assert!(
        matches!(&result, Err(StorageError::Fatal { message })
            if message == &MigrateError::VersionMissing(unknown_version).to_string()),
        "unknown migration must be rejected: {result:?}"
    );
    Ok(())
}
