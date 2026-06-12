use std::{
    error::Error,
    str::FromStr,
    time::{Duration, Instant},
};

use sqlx::{
    AssertSqlSafe, PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

const ROW_COUNT: i64 = 1_000;
const MIN_POSTGRES_VERSION_NUM: u32 = 110_000;
const MAX_MIGRATION_ELAPSED: Duration = Duration::from_secs(2);
const MIGRATIONS_TO_0032: &[&str] = &[
    include_str!("../migrations/0001_meta.sql"),
    include_str!("../migrations/0002_killswitch.sql"),
    include_str!("../migrations/0003_audit_log.sql"),
    include_str!("../migrations/0004_request_events.sql"),
    include_str!("../migrations/0005_quotas.sql"),
    include_str!("../migrations/0006_principal_limit_states.sql"),
    include_str!("../migrations/0007_oauth_credentials.sql"),
    include_str!("../migrations/0008_api_keys.sql"),
    include_str!("../migrations/0009_anthropic_api_keys.sql"),
    include_str!("../migrations/0010_usage_rollups.sql"),
    include_str!("../migrations/0011_config_draft.sql"),
    include_str!("../migrations/0012_config_history.sql"),
    include_str!("../migrations/0013_managed_api_keys.sql"),
    include_str!("../migrations/0014_managed_api_key_index.sql"),
    include_str!("../migrations/0015_upstreams.sql"),
    include_str!("../migrations/0016_principals.sql"),
    include_str!("../migrations/0017_plugin_registry.sql"),
    include_str!("../migrations/0018_upstream_rate_limit_state.sql"),
    include_str!("../migrations/0019_principal_allowed_upstreams.sql"),
    include_str!("../migrations/0020_drop_per_key_pin.sql"),
    include_str!("../migrations/0021_plugin_slot_shape.sql"),
    include_str!("../migrations/0022_plugin_registry.sql"),
    include_str!("../migrations/0023_anthropic_compatibility_kv.sql"),
    include_str!("../migrations/0024_upstream_subscription_quota.sql"),
    include_str!("../migrations/0025_usage_rollups_v2.sql"),
    include_str!("../migrations/0026_usage_rollup_checkpoint_reset.sql"),
    include_str!("../migrations/0027_cache_analysis_fields.sql"),
    include_str!("../migrations/0028_cache_breakpoints.sql"),
    include_str!("../migrations/0029_subscription_and_org_metadata.sql"),
    include_str!("../migrations/0030_prompt_cache_observation.sql"),
    include_str!("../migrations/0031_plugin_chain_wire_version.sql"),
    include_str!("../migrations/0032_drop_custom_upstream_kind.sql"),
];
const MIGRATION_0037: &str = include_str!("../migrations/0037_upstream_warmup.sql");

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn migration_0037_on_populated_upstreams_is_fast_and_backfills_defaults() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };

    let result = run_migration_assertions(&fixture).await;
    let teardown = fixture.drop_schema().await;

    result?;
    teardown?;
    Ok(())
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl Fixture {
    async fn create() -> TestResult<Option<Self>> {
        let Some(url) = std::env::var("DATABASE_URL_TEST").ok() else {
            eprintln!("SKIP: missing_DATABASE_URL_TEST");
            return Ok(None);
        };

        let schema = format!("test_migration_0037_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(&url)?)
            .await?;

        require_postgres_11(&admin_pool).await?;
        drop_schema_on_pool(&admin_pool, &schema).await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;

        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(
                PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())]),
            )
            .await?;

        let fixture = Self {
            schema,
            admin_pool,
            pool,
        };

        if let Err(error) = fixture.apply_migrations_to_0032().await {
            let _ = fixture.drop_schema().await;
            return Err(error);
        }

        Ok(Some(fixture))
    }

    async fn apply_migrations_to_0032(&self) -> TestResult {
        for migration in MIGRATIONS_TO_0032 {
            sqlx::raw_sql(*migration).execute(&self.pool).await?;
        }

        Ok(())
    }

    async fn drop_schema(self) -> TestResult {
        self.pool.close().await;
        drop_schema_on_pool(&self.admin_pool, &self.schema).await?;
        self.admin_pool.close().await;
        Ok(())
    }
}

async fn run_migration_assertions(fixture: &Fixture) -> TestResult {
    seed_upstreams(&fixture.pool).await?;
    assert_seeded_rows(&fixture.pool).await?;

    let started_at = Instant::now();
    sqlx::raw_sql(MIGRATION_0037).execute(&fixture.pool).await?;
    let elapsed = started_at.elapsed();

    eprintln!("migration_elapsed_secs={:.6}", elapsed.as_secs_f64());
    assert!(
        elapsed < MAX_MIGRATION_ELAPSED,
        "migration 0037 took {elapsed:?}, expected < {MAX_MIGRATION_ELAPSED:?}"
    );

    let counts = warmup_default_counts(&fixture.pool).await?;
    assert_eq!(counts.total, ROW_COUNT, "unexpected upstream row count");
    assert_eq!(
        counts.warmup_disabled, ROW_COUNT,
        "warmup_enabled default mismatch"
    );
    assert_eq!(
        counts.next_warmup_null, ROW_COUNT,
        "next_warmup_at default mismatch"
    );
    assert_eq!(
        counts.last_cycle_key_null, ROW_COUNT,
        "last_warmup_cycle_key default mismatch"
    );
    assert_eq!(
        counts.lease_holder_null, ROW_COUNT,
        "warmup_lease_holder default mismatch"
    );
    assert_eq!(
        counts.lease_until_null, ROW_COUNT,
        "warmup_lease_until_unix_secs default mismatch"
    );
    eprintln!("defaults_verified rows={}", counts.total);

    let index_name = sqlx::query_scalar::<_, String>(
        "SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND indexname = 'upstreams_v1_warmup_due_idx'",
    )
    .bind(&fixture.schema)
    .fetch_optional(&fixture.pool)
    .await?;
    assert_eq!(
        index_name.as_deref(),
        Some("upstreams_v1_warmup_due_idx"),
        "missing upstreams_v1_warmup_due_idx"
    );
    eprintln!(
        "index_present indexname={}",
        index_name.expect("index asserted present")
    );

    Ok(())
}

async fn require_postgres_11(pool: &PgPool) -> TestResult {
    let server_version_num = sqlx::query_scalar::<_, String>("SHOW server_version_num")
        .fetch_one(pool)
        .await?
        .parse::<u32>()?;

    assert!(
        server_version_num >= MIN_POSTGRES_VERSION_NUM,
        "PostgreSQL server_version_num={server_version_num}; migration 0037 timing test requires PostgreSQL 11+ metadata-only ADD COLUMN behavior"
    );

    Ok(())
}

async fn seed_upstreams(pool: &PgPool) -> TestResult {
    sqlx::query(
        "INSERT INTO upstreams_v1 (
             id,
             name,
             kind,
             base_url,
             enabled,
             oauth_credentials,
             api_key_ciphertext,
             revision,
             created_at,
             updated_at
         )
         SELECT
             ('00000000-0000-0000-0000-' || lpad(to_hex(row_number), 12, '0'))::uuid,
             'migration-0037-warmup-' || row_number::text,
             CASE
                 WHEN row_number % 2 = 0 THEN 'anthropic_api_key'
                 ELSE 'anthropic_oauth'
             END,
             'https://api.anthropic.com',
             row_number % 5 <> 0,
             CASE
                 WHEN row_number % 2 = 0 THEN NULL::bytea
                 ELSE convert_to('oauth-' || row_number::text, 'UTF8')
             END,
             CASE
                 WHEN row_number % 2 = 0 THEN convert_to('api-key-' || row_number::text, 'UTF8')
                 ELSE NULL::bytea
             END,
             1,
             now(),
             now()
         FROM generate_series(1, $1::integer) AS rows(row_number)",
    )
    .bind(ROW_COUNT)
    .execute(pool)
    .await?;

    Ok(())
}

async fn assert_seeded_rows(pool: &PgPool) -> TestResult {
    let row = sqlx::query(
        "SELECT
             COUNT(*) AS total,
             COUNT(*) FILTER (WHERE kind = 'anthropic_api_key') AS api_key_rows,
             COUNT(*) FILTER (WHERE kind = 'anthropic_oauth') AS oauth_rows
         FROM upstreams_v1",
    )
    .fetch_one(pool)
    .await?;

    assert_eq!(
        row.try_get::<i64, _>("total")?,
        ROW_COUNT,
        "seed should happen before migration 0037"
    );
    assert_eq!(row.try_get::<i64, _>("api_key_rows")?, ROW_COUNT / 2);
    assert_eq!(row.try_get::<i64, _>("oauth_rows")?, ROW_COUNT / 2);

    Ok(())
}

#[derive(Debug)]
struct DefaultCounts {
    total: i64,
    warmup_disabled: i64,
    next_warmup_null: i64,
    last_cycle_key_null: i64,
    lease_holder_null: i64,
    lease_until_null: i64,
}

async fn warmup_default_counts(pool: &PgPool) -> TestResult<DefaultCounts> {
    let row = sqlx::query(
        "SELECT
             COUNT(*) AS total,
             COUNT(*) FILTER (WHERE warmup_enabled = false) AS warmup_disabled,
             COUNT(*) FILTER (WHERE next_warmup_at IS NULL) AS next_warmup_null,
             COUNT(*) FILTER (WHERE last_warmup_cycle_key IS NULL) AS last_cycle_key_null,
             COUNT(*) FILTER (WHERE warmup_lease_holder IS NULL) AS lease_holder_null,
             COUNT(*) FILTER (WHERE warmup_lease_until_unix_secs IS NULL) AS lease_until_null
         FROM upstreams_v1",
    )
    .fetch_one(pool)
    .await?;

    Ok(DefaultCounts {
        total: row.try_get("total")?,
        warmup_disabled: row.try_get("warmup_disabled")?,
        next_warmup_null: row.try_get("next_warmup_null")?,
        last_cycle_key_null: row.try_get("last_cycle_key_null")?,
        lease_holder_null: row.try_get("lease_holder_null")?,
        lease_until_null: row.try_get("lease_until_null")?,
    })
}

async fn drop_schema_on_pool(pool: &PgPool, schema: &str) -> TestResult {
    sqlx::query(AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {} CASCADE",
        quote_ident(schema)
    )))
    .execute(pool)
    .await?;
    Ok(())
}

fn quote_ident(identifier: &str) -> String {
    assert!(
        identifier
            .chars()
            .all(|character| character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || character == '_'),
        "unsafe postgres identifier: {identifier}"
    );
    format!("\"{identifier}\"")
}
