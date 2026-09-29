//! Plugin hook metadata uploaded before hook modes existed has no `mode` key.
//! Migration 0130 stamps the former serde default (`active`) on those entries
//! so registry reads keep decoding them now that `mode` is mandatory.

use std::str::FromStr;

use anyhow::Result;
use cc_lb_plugin_wire::metadata::HookMode;
use cc_lb_storage_api::{MetaStore, PluginRegistryStore};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgConnectOptions, postgres::PgPoolOptions};
use uuid::Uuid;

const LEGACY_ID: Uuid = Uuid::from_u128(0x0130_0000_0000_0000_0000_0000_0000_0001);
const CURRENT_ID: Uuid = Uuid::from_u128(0x0130_0000_0000_0000_0000_0000_0000_0002);

const LEGACY_HOOKS: &str = r#"{"filter":{"wire_version":1,"description":"filter hook","usage":"called by router"},"shape":{"wire_version":1,"description":"shape hook","usage":"called by shaper","mode":"noop"}}"#;
const CURRENT_HOOKS: &str =
    r#"{"filter": {"mode": "noop", "usage": "u", "description": "d", "wire_version": 1}}"#;

#[test]
fn hook_metadata_mode_backfill() {
    let Ok(url) = std::env::var("CI_POSTGRES_URL") else {
        eprintln!("skip: CI_POSTGRES_URL not set");
        return;
    };

    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(async move { run_test(&url).await })
        .expect("hook metadata mode backfill test");
}

async fn run_test(url: &str) -> Result<()> {
    let fixture = Fixture::create(url).await?;

    {
        let mut connection = fixture.pool.acquire().await?;
        sqlx::migrate!("./migrations")
            .run_direct(Some(129), &mut *connection, false)
            .await?;
    }
    seed_registry_row(&fixture.pool, LEGACY_ID, 1, "legacy-plugin", LEGACY_HOOKS).await?;
    seed_registry_row(
        &fixture.pool,
        CURRENT_ID,
        2,
        "current-plugin",
        CURRENT_HOOKS,
    )
    .await?;

    let storage = PostgresStorage::new(
        fixture.pool.clone(),
        std::sync::Arc::new(cc_lb_clock::SystemClock),
    );
    storage.initialize().await?;

    let legacy = storage
        .get_registry_entry_by_id(LEGACY_ID)
        .await?
        .expect("legacy registry entry");
    assert_eq!(legacy.hook_metadata.len(), 2);
    assert_eq!(legacy.hook_metadata["filter"].mode, HookMode::Active);
    assert_eq!(legacy.hook_metadata["filter"].usage, "called by router");
    assert_eq!(legacy.hook_metadata["shape"].mode, HookMode::Noop);

    let current = storage
        .get_registry_entry_by_id(CURRENT_ID)
        .await?
        .expect("current registry entry");
    assert_eq!(current.hook_metadata["filter"].mode, HookMode::Noop);

    // Rows whose hooks all carry a mode are not rewritten.
    let current_text: String =
        sqlx::query_scalar("SELECT hook_metadata FROM wasm_registry_v2 WHERE id = $1")
            .bind(CURRENT_ID)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(current_text, CURRENT_HOOKS);

    let listed = storage.list_registry(None, 10).await?;
    assert!(
        listed.iter().any(|entry| entry.id == LEGACY_ID),
        "legacy entry is listed"
    );

    fixture.drop_schema().await
}

async fn seed_registry_row(
    pool: &PgPool,
    id: Uuid,
    sha_byte: u8,
    name: &str,
    hook_metadata: &str,
) -> Result<()> {
    let sha256 = vec![sha_byte; 32];
    sqlx::query(
        "INSERT INTO wasm_blobs_v2 (sha256, bytes, size_bytes, parse_validated_at)
         VALUES ($1, $2, 4, NOW())",
    )
    .bind(&sha256)
    .bind(b"wasm".as_slice())
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO wasm_registry_v2 (
            id, sha256, name, original_filename, uploaded_at, uploaded_by_admin_id,
            description, usage, hook_metadata
         ) VALUES ($1, $2, $3, 'plugin.wasm', NOW(), $4, 'plugin', 'usage', $5)",
    )
    .bind(id)
    .bind(&sha256)
    .bind(name)
    .bind(Uuid::from_u128(0xad))
    .bind(hook_metadata)
    .execute(pool)
    .await?;
    Ok(())
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let schema = format!("hook_mode_backfill_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;

        let options = PgConnectOptions::from_str(url)?.options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;

        Ok(Self {
            schema,
            admin_pool,
            pool,
        })
    }

    async fn drop_schema(self) -> Result<()> {
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
