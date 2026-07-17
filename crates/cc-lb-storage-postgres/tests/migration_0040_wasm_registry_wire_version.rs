use std::{error::Error, str::FromStr};

use sqlx::{
    AssertSqlSafe, PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

const BUILTIN_CACHE_AFFINITY_ID: &str = "00000000-0000-0000-0000-000000000001";
const MIGRATION_0040: &str = include_str!("../migrations/0040_wasm_registry_wire_version.sql");
const MIGRATIONS_TO_0039: &[&str] = &[
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
    include_str!("../migrations/0033_router_pipeline.sql"),
    include_str!("../migrations/0034_drop_incompatible_router_chain.sql"),
    include_str!("../migrations/0035_drop_incompatible_router_chain.sql"),
    include_str!("../migrations/0036_seed_builtin_cache_affinity.sql"),
    include_str!("../migrations/0037_upstream_warmup.sql"),
    include_str!("../migrations/0038_warmup_dialect_plugin.sql"),
    include_str!("../migrations/0039_wasm_registry_supported_slots.sql"),
];

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn migration_0039_wasm_registry_has_no_wire_version() -> TestResult {
    let Some(url) = postgres_url() else {
        eprintln!("skip: CI_POSTGRES_URL or DATABASE_URL_TEST not set");
        return Ok(());
    };

    let fixture = Fixture::create(&url).await?;
    let result = assert_wire_version_absent_after_0039(&fixture.pool).await;
    let teardown = fixture.drop_schema().await;

    result?;
    teardown
}

#[tokio::test]
async fn migration_0040_adds_wire_version_and_updates_builtin() -> TestResult {
    let Some(url) = postgres_url() else {
        eprintln!("skip: CI_POSTGRES_URL or DATABASE_URL_TEST not set");
        return Ok(());
    };

    let fixture = Fixture::create(&url).await?;
    let result = async {
        fixture.apply_migration_0040().await?;
        assert_wire_version_present_after_0040(&fixture.pool).await
    }
    .await;
    let teardown = fixture.drop_schema().await;

    result?;
    teardown
}

#[tokio::test]
async fn migration_0040_is_idempotent() -> TestResult {
    let Some(url) = postgres_url() else {
        eprintln!("skip: CI_POSTGRES_URL or DATABASE_URL_TEST not set");
        return Ok(());
    };

    let fixture = Fixture::create(&url).await?;
    let result = async {
        fixture.apply_migration_0040().await?;
        fixture.apply_migration_0040().await?;
        assert_wire_version_present_after_0040(&fixture.pool).await
    }
    .await;
    let teardown = fixture.drop_schema().await;

    result?;
    teardown
}

async fn assert_wire_version_absent_after_0039(pool: &PgPool) -> TestResult {
    let column_name = wire_version_column(pool).await?;
    assert_eq!(
        column_name, None,
        "wire_version should not exist before migration 0040"
    );

    let error = sqlx::query_scalar::<_, Option<i16>>(
        "SELECT wire_version FROM wasm_registry_v2 WHERE id = $1::uuid",
    )
    .bind(BUILTIN_CACHE_AFFINITY_ID)
    .fetch_optional(pool)
    .await
    .expect_err("selecting wasm_registry_v2.wire_version before 0040 should fail");
    assert_missing_column(error);

    Ok(())
}

async fn assert_wire_version_present_after_0040(pool: &PgPool) -> TestResult {
    let column_name = wire_version_column(pool).await?;
    assert_eq!(
        column_name.as_deref(),
        Some("wire_version"),
        "wire_version should exist after migration 0040"
    );

    let wire_version = sqlx::query_scalar::<_, Option<i16>>(
        "SELECT wire_version FROM wasm_registry_v2 WHERE id = $1::uuid",
    )
    .bind(BUILTIN_CACHE_AFFINITY_ID)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        wire_version,
        Some(3),
        "cache-affinity builtin row should use wire_version 3"
    );

    Ok(())
}

async fn wire_version_column(pool: &PgPool) -> TestResult<Option<String>> {
    let row = sqlx::query(
        "SELECT column_name
           FROM information_schema.columns
          WHERE table_schema = current_schema()
            AND table_name = 'wasm_registry_v2'
            AND column_name = 'wire_version'",
    )
    .fetch_optional(pool)
    .await?;

    row.map(|row| row.try_get("column_name"))
        .transpose()
        .map_err(Into::into)
}

fn assert_missing_column(error: sqlx::Error) {
    let message = error.to_string();
    assert!(
        message.contains("column \"wire_version\" does not exist"),
        "unexpected error: {message}"
    );
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl Fixture {
    async fn create(url: &str) -> TestResult<Self> {
        let schema = format!("test_migration_0040_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(url)?)
            .await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;

        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(
                PgConnectOptions::from_str(url)?.options([("search_path", schema.as_str())]),
            )
            .await?;

        let fixture = Self {
            schema,
            admin_pool,
            pool,
        };

        if let Err(error) = fixture.apply_migrations_to_0039().await {
            let _ = fixture.drop_schema().await;
            return Err(error);
        }

        Ok(fixture)
    }

    async fn apply_migrations_to_0039(&self) -> TestResult {
        for migration in MIGRATIONS_TO_0039 {
            sqlx::raw_sql(*migration).execute(&self.pool).await?;
        }

        Ok(())
    }

    async fn apply_migration_0040(&self) -> TestResult {
        sqlx::raw_sql(MIGRATION_0040).execute(&self.pool).await?;
        Ok(())
    }

    async fn drop_schema(self) -> TestResult {
        self.pool.close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_ident(&self.schema)
        )))
        .execute(&self.admin_pool)
        .await?;
        self.admin_pool.close().await;
        Ok(())
    }
}

fn postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL")
        .or_else(|_| std::env::var("DATABASE_URL_TEST"))
        .ok()
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
