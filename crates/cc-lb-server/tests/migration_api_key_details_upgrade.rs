//! STO-01 / STO-02: upgrading a populated database from SQLite 0095 /
//! Postgres 0132 to 0096 / 0133 through the actual `sqlx` migration runner.
//! Existing spec, secret, token, request-event, and rollup rows survive
//! byte-for-byte, `spec_revision` is not bumped, every upstream reads with
//! `workspace_id = None` and no details or usage rows, and restarting (running
//! the migrator again) is a no-op. The Postgres half needs `CI_POSTGRES_URL`.

use std::{path::Path, str::FromStr, sync::Arc};

use cc_lb_storage_api::{
    MetaStore, RequestEvent, RequestEventStore, UpstreamApiKeyStore, UpstreamRecord, UpstreamStore,
    UsageRollupStore,
};
use cc_lb_storage_postgres::PostgresStorage;
use cc_lb_storage_sqlite::open_sqlite;
use sqlx::{
    AssertSqlSafe, Connection, PgPool, SqliteConnection,
    migrate::Migrator,
    postgres::{PgConnectOptions, PgPoolOptions},
    sqlite::SqliteConnectOptions,
};
use uuid::Uuid;

const SQLITE_MIGRATIONS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../cc-lb-storage-sqlite/migrations"
);
const POSTGRES_MIGRATIONS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../cc-lb-storage-postgres/migrations"
);
const SQLITE_BEFORE: i64 = 95;
const SQLITE_AFTER: i64 = 96;
const POSTGRES_BEFORE: i64 = 132;
const POSTGRES_AFTER: i64 = 133;

const API_KEY_ID: Uuid = Uuid::from_u128(0x0096_0000_0000_0000_0000_0000_0000_0001);
const OAUTH_ID: Uuid = Uuid::from_u128(0x0096_0000_0000_0000_0000_0000_0000_0002);
const DELETED_ID: Uuid = Uuid::from_u128(0x0096_0000_0000_0000_0000_0000_0000_0003);
const DISABLED_ID: Uuid = Uuid::from_u128(0x0096_0000_0000_0000_0000_0000_0000_0004);

/// STO-01: each backend gains exactly one new migration directly after the
/// previous head; later migrations may follow it.
#[test]
fn sto01_new_migrations_are_the_heads() {
    for (dir, previous, new) in [
        (
            SQLITE_MIGRATIONS,
            "0095_revoke_disabled_managed_keys.sql",
            "0096_upstream_workspace_and_key_details.sql",
        ),
        (
            POSTGRES_MIGRATIONS,
            "0132_revoke_disabled_managed_keys.sql",
            "0133_upstream_workspace_and_key_details.sql",
        ),
    ] {
        let mut files = std::fs::read_dir(dir)
            .expect("read migrations directory")
            .filter_map(|entry| {
                let name = entry.ok()?.file_name().to_string_lossy().into_owned();
                name.ends_with(".sql").then_some(name)
            })
            .collect::<Vec<_>>();
        files.sort();
        let position = files
            .iter()
            .position(|name| name == previous)
            .expect("previous head present");
        assert_eq!(
            files.get(position + 1).map(String::as_str),
            Some(new),
            "{dir}: the new migration directly follows the previous head"
        );
    }
    let postgres = std::fs::read_to_string(
        Path::new(POSTGRES_MIGRATIONS).join("0133_upstream_workspace_and_key_details.sql"),
    )
    .expect("read 0133");
    assert!(postgres.contains("SET LOCAL lock_timeout = '1s';"));
    assert!(!postgres.contains("statement_timeout"));
}

#[tokio::test]
async fn sto02_sqlite_upgrade_preserves_rows_and_is_idempotent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let url = format!("sqlite://{}", dir.path().join("upgrade.sqlite").display());
    let options = SqliteConnectOptions::from_str(&url)
        .expect("sqlite options")
        .create_if_missing(true)
        .foreign_keys(true);
    let mut connection = SqliteConnection::connect_with(&options)
        .await
        .expect("connect sqlite");
    let migrator = Migrator::new(Path::new(SQLITE_MIGRATIONS))
        .await
        .expect("load sqlite migrations");
    migrator
        .run_direct(Some(SQLITE_BEFORE), &mut connection, false)
        .await
        .expect("migrate to 0095");
    seed_sqlite(&mut connection).await;
    connection.close().await.expect("close seed connection");

    // Request events and rollups are written by the current storage code,
    // which only touches tables 0096 leaves alone.
    let storage = open_sqlite(&url, Arc::new(cc_lb_clock::SystemClock))
        .await
        .expect("open sqlite at 0095");
    append_events(&storage).await;
    storage.rollup_usage_once().await.expect("rollup at 0095");
    let before = sqlite_snapshot(storage.pool()).await;
    assert!(before.rollup_rows > 0, "seeded rollups");

    // App boot runs the migrator; a restart runs it again.
    storage.initialize().await.expect("upgrade to 0096");
    drop(storage);
    let storage = open_sqlite(&url, Arc::new(cc_lb_clock::SystemClock))
        .await
        .expect("reopen sqlite");
    storage.initialize().await.expect("restart is a no-op");

    let after = sqlite_snapshot(storage.pool()).await;
    assert_eq!(after, before, "existing rows are untouched");
    let applied: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM _sqlx_migrations WHERE version = ? AND success)",
    )
    .bind(SQLITE_AFTER)
    .fetch_one(storage.pool())
    .await
    .expect("migration version");
    assert!(applied, "migration {SQLITE_AFTER} is applied");
    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM upstream_spec_v1 WHERE workspace_id IS NOT NULL),
                (SELECT COUNT(*) FROM upstream_api_key_details_v1),
                (SELECT COUNT(*) FROM upstream_api_usage_v1)",
    )
    .fetch_one(storage.pool())
    .await
    .expect("new-table counts");
    assert_eq!(counts, (0, 0, 0), "no workspace, details, or usage rows");

    assert_upgraded_reads(&storage).await;
}

#[test]
fn sto02_postgres_upgrade_preserves_rows_and_is_idempotent() {
    let Ok(url) = std::env::var("CI_POSTGRES_URL") else {
        eprintln!("skip: CI_POSTGRES_URL not set");
        return;
    };
    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(async move {
            let fixture = PgFixture::create(&url).await;
            postgres_upgrade(&fixture.pool).await;
            fixture.drop_schema().await;
        });
}

async fn postgres_upgrade(pool: &PgPool) {
    let migrator = Migrator::new(Path::new(POSTGRES_MIGRATIONS))
        .await
        .expect("load postgres migrations");
    {
        let mut connection = pool.acquire().await.expect("acquire");
        migrator
            .run_direct(Some(POSTGRES_BEFORE), &mut *connection, false)
            .await
            .expect("migrate to 0132");
    }
    seed_postgres(pool).await;
    let storage = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
    append_events(&storage).await;
    let before = postgres_snapshot(pool).await;
    assert!(before.rollup_rows > 0, "seeded rollups");

    storage.initialize().await.expect("upgrade to 0133");
    let restarted = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
    restarted.initialize().await.expect("restart is a no-op");
    // The DDL itself is idempotent: re-running 0133 by hand succeeds.
    let sql = std::fs::read_to_string(
        Path::new(POSTGRES_MIGRATIONS).join("0133_upstream_workspace_and_key_details.sql"),
    )
    .expect("read 0133");
    let mut tx = pool.begin().await.expect("begin");
    sqlx::raw_sql(AssertSqlSafe(sql))
        .execute(&mut *tx)
        .await
        .expect("0133 re-applies cleanly");
    tx.commit().await.expect("commit re-apply");

    let after = postgres_snapshot(pool).await;
    assert_eq!(after, before, "existing rows are untouched");
    let applied: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM _sqlx_migrations WHERE version = $1 AND success)",
    )
    .bind(POSTGRES_AFTER)
    .fetch_one(pool)
    .await
    .expect("migration version");
    assert!(applied, "migration {POSTGRES_AFTER} is applied");
    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM upstream_spec_v1 WHERE workspace_id IS NOT NULL),
                (SELECT COUNT(*) FROM upstream_api_key_details_v1),
                (SELECT COUNT(*) FROM upstream_api_usage_v1)",
    )
    .fetch_one(pool)
    .await
    .expect("new-table counts");
    assert_eq!(counts, (0, 0, 0), "no workspace, details, or usage rows");

    assert_upgraded_reads(&restarted).await;
}

/// The read paths the router, admin API, and poller use all decode the
/// upgraded rows.
async fn assert_upgraded_reads<S>(storage: &S)
where
    S: UpstreamStore + UpstreamApiKeyStore,
{
    let live: Vec<UpstreamRecord> = UpstreamStore::list(storage, None, 100)
        .await
        .expect("list upstreams");
    let mut live_ids = live.iter().map(|record| record.id).collect::<Vec<_>>();
    live_ids.sort();
    assert_eq!(live_ids, [API_KEY_ID, OAUTH_ID, DISABLED_ID]);
    assert!(live.iter().all(|record| record.workspace_id.is_none()));
    let api_key = live
        .iter()
        .find(|record| record.id == API_KEY_ID)
        .expect("api-key upstream listed");
    assert_eq!(api_key.revision, 7, "spec_revision not bumped");
    assert_eq!(
        api_key.api_key_ciphertext.as_deref(),
        Some(&b"ciphertext"[..])
    );
    let oauth = UpstreamStore::get_by_id(storage, OAUTH_ID)
        .await
        .expect("get oauth")
        .expect("oauth upstream");
    assert_eq!((oauth.revision, oauth.oauth_token_generation), (3, 5));
    assert!(oauth.oauth_never_refresh);
    let deleted = UpstreamStore::get_by_id(storage, DELETED_ID)
        .await
        .expect("get deleted")
        .expect("deleted upstream");
    assert!(deleted.deleted_at_unix_secs.is_some());

    let details = UpstreamApiKeyStore::list_api_key_details(
        storage,
        &[API_KEY_ID, OAUTH_ID, DELETED_ID, DISABLED_ID],
    )
    .await
    .expect("list details");
    assert!(details.is_empty(), "legacy rows read as not checked");

    // Legacy api-key upstreams are poll candidates with no details row.
    let candidates =
        UpstreamApiKeyStore::list_api_key_usage_poll_candidates(storage, 1_800_000_000, None, 100)
            .await
            .expect("poll candidates");
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| (candidate.upstream.id, candidate.details.is_none()))
            .collect::<Vec<_>>(),
        [(API_KEY_ID, true)]
    );

    let outcome = UpstreamApiKeyStore::latest_upstream_request_outcome(storage, API_KEY_ID, 0)
        .await
        .expect("latest outcome")
        .expect("seeded event is visible");
    assert_eq!(outcome.status, 402);
    assert_eq!(
        outcome.upstream_error_type.as_deref(),
        Some("billing_error")
    );
    assert_eq!(outcome.observed_at_unix_ms, 1_700_000_200_000);
}

async fn append_events<S>(storage: &S)
where
    S: RequestEventStore,
{
    for (index, (status, ts_ms, error_type)) in [
        (200_u16, 1_700_000_100_000_u64, None),
        (402, 1_700_000_200_000, Some("billing_error")),
    ]
    .into_iter()
    .enumerate()
    {
        let event = RequestEvent {
            ts: ts_ms / 1_000,
            ts_ms: Some(ts_ms),
            request_id: format!("req-upgrade-{index}"),
            event_id: Some(format!("evt-upgrade-{index}")),
            principal_id: Some("principal-upgrade".to_owned()),
            upstream_id: Some(API_KEY_ID),
            upstream_name: Some("legacy-api-key".to_owned()),
            model: Some("claude-sonnet-4-5".to_owned()),
            status,
            input_tokens: Some(10),
            output_tokens: Some(5),
            upstream_error_type: error_type.map(str::to_owned),
            upstream_error_message: error_type.map(|_| "credit balance is too low".to_owned()),
            ..RequestEvent::default()
        };
        storage
            .append_request_event(&event)
            .await
            .expect("append request event");
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    specs: Vec<String>,
    secrets: Vec<String>,
    tokens: Vec<String>,
    request_events: i64,
    rollup_rows: i64,
}

async fn seed_sqlite(connection: &mut SqliteConnection) {
    for (id, name, kind, enabled, revision, deleted_at) in [
        (
            API_KEY_ID,
            "legacy-api-key",
            "anthropic_api_key",
            1_i64,
            7_i64,
            None,
        ),
        (OAUTH_ID, "legacy-oauth", "anthropic_oauth", 1, 3, None),
        (
            DELETED_ID,
            "legacy-deleted",
            "anthropic_api_key",
            1,
            9,
            Some(1_700_000_500_i64),
        ),
        (
            DISABLED_ID,
            "legacy-disabled",
            "anthropic_api_key",
            0,
            2,
            None,
        ),
    ] {
        sqlx::query(
            "INSERT INTO upstream_spec_v1 (id, name, kind, base_url, enabled, warmup_enabled,
                 warmup_dialect_plugin, spec_revision, created_at, updated_at, deleted_at)
             VALUES (?, ?, ?, NULL, ?, 0, NULL, ?, 1700000000, 1700000010, ?)",
        )
        .bind(id.to_string())
        .bind(name)
        .bind(kind)
        .bind(enabled)
        .bind(revision)
        .bind(deleted_at)
        .execute(&mut *connection)
        .await
        .expect("seed spec");
    }
    for id in [API_KEY_ID, DELETED_ID, DISABLED_ID] {
        sqlx::query(
            "INSERT INTO upstream_api_key_secret_v1 (upstream_id, api_key_ciphertext, created_at, updated_at)
             VALUES (?, ?, 1700000000, 1700000010)",
        )
        .bind(id.to_string())
        .bind(&b"ciphertext"[..])
        .execute(&mut *connection)
        .await
        .expect("seed secret");
    }
    sqlx::query(
        "INSERT INTO upstream_oauth_token_v1 (upstream_id, oauth_credentials_ciphertext,
             oauth_token_generation, never_refresh, created_at, updated_at)
         VALUES (?, ?, 5, 1, 1700000000, 1700000010)",
    )
    .bind(OAUTH_ID.to_string())
    .bind(&b"oauth-ciphertext"[..])
    .execute(&mut *connection)
    .await
    .expect("seed oauth token");
}

async fn sqlite_snapshot(pool: &sqlx::SqlitePool) -> Snapshot {
    let rows = |sql: &'static str| async move {
        sqlx::query_scalar::<_, String>(sql)
            .fetch_all(pool)
            .await
            .expect("snapshot rows")
    };
    Snapshot {
        specs: rows(
            "SELECT json_array(id, name, kind, base_url, enabled, warmup_enabled,
                    warmup_dialect_plugin, spec_revision, created_at, updated_at, deleted_at)
               FROM upstream_spec_v1 ORDER BY id",
        )
        .await,
        secrets: rows(
            "SELECT json_array(upstream_id, hex(api_key_ciphertext), created_at, updated_at)
               FROM upstream_api_key_secret_v1 ORDER BY upstream_id",
        )
        .await,
        tokens: rows(
            "SELECT json_array(upstream_id, hex(oauth_credentials_ciphertext),
                    oauth_token_generation, never_refresh, created_at, updated_at)
               FROM upstream_oauth_token_v1 ORDER BY upstream_id",
        )
        .await,
        request_events: sqlx::query_scalar("SELECT COUNT(*) FROM request_events_v1")
            .fetch_one(pool)
            .await
            .expect("count events"),
        rollup_rows: sqlx::query_scalar("SELECT COUNT(*) FROM usage_rollups_v2")
            .fetch_one(pool)
            .await
            .expect("count rollups"),
    }
}

async fn seed_postgres(pool: &PgPool) {
    for (id, name, kind, enabled, revision, deleted) in [
        (
            API_KEY_ID,
            "legacy-api-key",
            "anthropic_api_key",
            true,
            7_i64,
            false,
        ),
        (OAUTH_ID, "legacy-oauth", "anthropic_oauth", true, 3, false),
        (
            DELETED_ID,
            "legacy-deleted",
            "anthropic_api_key",
            true,
            9,
            true,
        ),
        (
            DISABLED_ID,
            "legacy-disabled",
            "anthropic_api_key",
            false,
            2,
            false,
        ),
    ] {
        sqlx::query(
            "INSERT INTO upstream_spec_v1 (id, name, kind, base_url, enabled, warmup_enabled,
                 warmup_dialect_plugin, spec_revision, created_at, updated_at, deleted_at)
             VALUES ($1, $2, $3, NULL, $4, FALSE, NULL, $5, to_timestamp(1700000000),
                 to_timestamp(1700000010), CASE WHEN $6 THEN to_timestamp(1700000500) END)",
        )
        .bind(id)
        .bind(name)
        .bind(kind)
        .bind(enabled)
        .bind(revision)
        .bind(deleted)
        .execute(pool)
        .await
        .expect("seed spec");
    }
    for id in [API_KEY_ID, DELETED_ID, DISABLED_ID] {
        sqlx::query(
            "INSERT INTO upstream_api_key_secret_v1 (upstream_id, api_key_ciphertext, created_at, updated_at)
             VALUES ($1, $2, to_timestamp(1700000000), to_timestamp(1700000010))",
        )
        .bind(id)
        .bind(&b"ciphertext"[..])
        .execute(pool)
        .await
        .expect("seed secret");
    }
    sqlx::query(
        "INSERT INTO upstream_oauth_token_v1 (upstream_id, oauth_credentials_ciphertext,
             oauth_token_generation, never_refresh, created_at, updated_at)
         VALUES ($1, $2, 5, TRUE, to_timestamp(1700000000), to_timestamp(1700000010))",
    )
    .bind(OAUTH_ID)
    .bind(&b"oauth-ciphertext"[..])
    .execute(pool)
    .await
    .expect("seed oauth token");
    sqlx::query(
        "INSERT INTO usage_rollups_v2 (resolution, bucket_start_unix_secs, principal_id,
             upstream_id, upstream_name, model, request_count, input_tokens, updated_at)
         VALUES ('hour', 1699999200, 'principal-upgrade', $1, 'legacy-api-key',
             'claude-sonnet-4-5', 2, 20, to_timestamp(1700000300))",
    )
    .bind(API_KEY_ID)
    .execute(pool)
    .await
    .expect("seed rollup");
}

async fn postgres_snapshot(pool: &PgPool) -> Snapshot {
    let rows = |sql: &'static str| async move {
        sqlx::query_scalar::<_, String>(sql)
            .fetch_all(pool)
            .await
            .expect("snapshot rows")
    };
    Snapshot {
        specs: rows(
            "SELECT json_build_array(id, name, kind, base_url, enabled, warmup_enabled,
                    warmup_dialect_plugin, spec_revision, created_at, updated_at, deleted_at)::text
               FROM upstream_spec_v1 ORDER BY id",
        )
        .await,
        secrets: rows(
            "SELECT json_build_array(upstream_id, encode(api_key_ciphertext, 'hex'),
                    created_at, updated_at)::text
               FROM upstream_api_key_secret_v1 ORDER BY upstream_id",
        )
        .await,
        tokens: rows(
            "SELECT json_build_array(upstream_id, encode(oauth_credentials_ciphertext, 'hex'),
                    oauth_token_generation, never_refresh, created_at, updated_at)::text
               FROM upstream_oauth_token_v1 ORDER BY upstream_id",
        )
        .await,
        request_events: sqlx::query_scalar("SELECT COUNT(*) FROM request_events_v1")
            .fetch_one(pool)
            .await
            .expect("count events"),
        rollup_rows: sqlx::query_scalar(
            "SELECT COALESCE(SUM(request_count), 0)::BIGINT FROM usage_rollups_v2",
        )
        .fetch_one(pool)
        .await
        .expect("sum rollups"),
    }
}

struct PgFixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl PgFixture {
    async fn create(url: &str) -> Self {
        let schema = format!("api_key_upgrade_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await
            .expect("admin pool");
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&admin_pool)
            .await
            .expect("create schema");
        let options = PgConnectOptions::from_str(url)
            .expect("postgres options")
            .options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .expect("schema pool");
        Self {
            schema,
            admin_pool,
            pool,
        }
    }

    async fn drop_schema(self) {
        self.pool.close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
            self.schema
        )))
        .execute(&self.admin_pool)
        .await
        .expect("drop schema");
        self.admin_pool.close().await;
    }
}
