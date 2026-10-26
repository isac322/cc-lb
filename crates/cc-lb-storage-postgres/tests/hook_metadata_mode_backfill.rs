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
#[test]
fn upstream_kind_migration() {
    let Ok(url) = std::env::var("CI_POSTGRES_URL") else {
        eprintln!("skip: CI_POSTGRES_URL not set");
        return;
    };

    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(async move { run_upstream_kind_migration(&url).await })
        .expect("upstream kind migration test");
}

async fn run_upstream_kind_migration(url: &str) -> Result<()> {
    let fixture = Fixture::create(url).await?;
    {
        let mut connection = fixture.pool.acquire().await?;
        sqlx::migrate!("./migrations")
            .run_direct(Some(138), &mut *connection, false)
            .await?;
    }

    let anthropic_api_id = Uuid::from_u128(0x0139_0000_0000_0000_0000_0000_0000_0001);
    let anthropic_oauth_id = Uuid::from_u128(0x0139_0000_0000_0000_0000_0000_0000_0002);
    let openai_api_id = Uuid::from_u128(0x0139_0000_0000_0000_0000_0000_0000_0003);
    let openai_oauth_id = Uuid::from_u128(0x0139_0000_0000_0000_0000_0000_0000_0004);
    let gemini_api_id = Uuid::from_u128(0x0139_0000_0000_0000_0000_0000_0000_0005);
    let gemini_oauth_id = Uuid::from_u128(0x0139_0000_0000_0000_0000_0000_0000_0006);
    let unknown_id = Uuid::from_u128(0x0139_0000_0000_0000_0000_0000_0000_0007);
    let rollback_id = Uuid::from_u128(0x0139_0000_0000_0000_0000_0000_0000_0008);

    insert_upstream_spec(
        &fixture.pool,
        anthropic_api_id,
        "legacy-anthropic-api",
        "anthropic_api_key",
        Some("https://anthropic.example.test/"),
        false,
        true,
        7,
        Some("legacy-workspace"),
    )
    .await?;
    insert_upstream_spec(
        &fixture.pool,
        anthropic_oauth_id,
        "legacy-anthropic-oauth",
        "anthropic_oauth",
        None,
        true,
        false,
        9,
        None,
    )
    .await?;
    seed_anthropic_children(&fixture.pool, anthropic_api_id, anthropic_oauth_id).await?;

    // The pre-0139 constraint rejects every non-Anthropic kind.
    assert!(
        insert_upstream_spec(
            &fixture.pool,
            openai_api_id,
            "openai-before-migration",
            "openai_api_key",
            None,
            true,
            false,
            1,
            None,
        )
        .await
        .is_err()
    );

    let storage = PostgresStorage::new(
        fixture.pool.clone(),
        std::sync::Arc::new(cc_lb_clock::SystemClock),
    );
    storage.initialize().await?;

    insert_upstream_spec(
        &fixture.pool,
        openai_api_id,
        "openai-api",
        "openai_api_key",
        None,
        true,
        false,
        1,
        None,
    )
    .await?;
    insert_upstream_spec(
        &fixture.pool,
        openai_oauth_id,
        "openai-oauth",
        "openai_oauth",
        None,
        true,
        false,
        1,
        None,
    )
    .await?;
    insert_upstream_spec(
        &fixture.pool,
        gemini_api_id,
        "gemini-api",
        "gemini_api_key",
        None,
        true,
        false,
        1,
        None,
    )
    .await?;
    insert_upstream_spec(
        &fixture.pool,
        gemini_oauth_id,
        "gemini-oauth",
        "gemini_oauth",
        None,
        true,
        false,
        1,
        None,
    )
    .await?;

    let kinds: Vec<String> = sqlx::query_scalar("SELECT kind FROM upstream_spec_v1 ORDER BY kind")
        .fetch_all(&fixture.pool)
        .await?;
    assert_eq!(
        kinds,
        vec![
            "anthropic_api_key",
            "anthropic_oauth",
            "gemini_api_key",
            "gemini_oauth",
            "openai_api_key",
            "openai_oauth",
        ]
    );

    let api_row: (String, Option<String>, bool, bool, i64, Option<String>) = sqlx::query_as(
        "SELECT kind, base_url, enabled, warmup_enabled, spec_revision, workspace_id
         FROM upstream_spec_v1 WHERE id = $1",
    )
    .bind(anthropic_api_id)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        api_row,
        (
            "anthropic_api_key".to_owned(),
            Some("https://anthropic.example.test/".to_owned()),
            false,
            true,
            7,
            Some("legacy-workspace".to_owned()),
        )
    );
    let oauth_kind: String = sqlx::query_scalar("SELECT kind FROM upstream_spec_v1 WHERE id = $1")
        .bind(anthropic_oauth_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(oauth_kind, "anthropic_oauth");
    assert_eq!(
        sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT api_key_ciphertext FROM upstream_api_key_secret_v1 WHERE upstream_id = $1",
        )
        .bind(anthropic_api_id)
        .fetch_one(&fixture.pool)
        .await?,
        b"legacy-api-key".to_vec()
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT oauth_credentials_ciphertext FROM upstream_oauth_token_v1 WHERE upstream_id = $1",
        )
        .bind(anthropic_oauth_id)
        .fetch_one(&fixture.pool)
        .await?,
        b"legacy-oauth-token".to_vec()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT last_apply_error FROM upstream_status_v1 WHERE upstream_id = $1",
        )
        .bind(anthropic_api_id)
        .fetch_one(&fixture.pool)
        .await?,
        "legacy apply error"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT holder FROM upstream_lease_v1 WHERE upstream_id = $1 AND lease_kind = 'refresh'",
        )
        .bind(anthropic_api_id)
        .fetch_one(&fixture.pool)
        .await?,
        "legacy-holder"
    );

    let constraint: String = sqlx::query_scalar(
        "SELECT pg_get_constraintdef(oid)
         FROM pg_constraint
         WHERE conrelid = 'upstream_spec_v1'::regclass
           AND conname = 'upstream_spec_v1_kind_check'",
    )
    .fetch_one(&fixture.pool)
    .await?;
    for kind in [
        "anthropic_api_key",
        "anthropic_oauth",
        "openai_api_key",
        "openai_oauth",
        "gemini_api_key",
        "gemini_oauth",
    ] {
        assert!(constraint.contains(&format!("'{kind}'")));
    }

    assert!(
        insert_upstream_spec(
            &fixture.pool,
            unknown_id,
            "unknown-kind",
            "unknown_kind",
            None,
            true,
            false,
            1,
            None,
        )
        .await
        .is_err()
    );

    let mut transaction = fixture.pool.begin().await?;
    sqlx::query(
        "INSERT INTO upstream_spec_v1
         (id, name, kind, enabled, warmup_enabled, spec_revision, created_at, updated_at)
         VALUES ($1, 'rolled-back', 'openai_api_key', TRUE, FALSE, 1, TIMESTAMPTZ '2026-01-01 00:00:00+00', TIMESTAMPTZ '2026-01-01 00:00:00+00')",
    )
    .bind(rollback_id)
    .execute(&mut *transaction)
    .await?;
    assert!(
        sqlx::query(
            "INSERT INTO upstream_spec_v1
             (id, name, kind, enabled, warmup_enabled, spec_revision, created_at, updated_at)
             VALUES ($1, 'rolled-back-unknown', 'unknown_kind', TRUE, FALSE, 1, TIMESTAMPTZ '2026-01-01 00:00:00+00', TIMESTAMPTZ '2026-01-01 00:00:00+00')",
        )
        .bind(unknown_id)
        .execute(&mut *transaction)
        .await
        .is_err()
    );
    transaction.rollback().await?;
    let rolled_back: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM upstream_spec_v1 WHERE id = $1)")
            .bind(rollback_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert!(!rolled_back);

    sqlx::query("DELETE FROM upstream_spec_v1 WHERE id = $1 OR id = $2")
        .bind(anthropic_api_id)
        .bind(anthropic_oauth_id)
        .execute(&fixture.pool)
        .await?;
    for (table, query) in [
        (
            "upstream_api_key_secret_v1",
            "SELECT COUNT(*) FROM upstream_api_key_secret_v1 WHERE upstream_id = $1 OR upstream_id = $2",
        ),
        (
            "upstream_oauth_token_v1",
            "SELECT COUNT(*) FROM upstream_oauth_token_v1 WHERE upstream_id = $1 OR upstream_id = $2",
        ),
        (
            "upstream_status_v1",
            "SELECT COUNT(*) FROM upstream_status_v1 WHERE upstream_id = $1 OR upstream_id = $2",
        ),
        (
            "upstream_lease_v1",
            "SELECT COUNT(*) FROM upstream_lease_v1 WHERE upstream_id = $1 OR upstream_id = $2",
        ),
    ] {
        let count: i64 = sqlx::query_scalar(query)
            .bind(anthropic_api_id)
            .bind(anthropic_oauth_id)
            .fetch_one(&fixture.pool)
            .await?;
        assert_eq!(count, 0, "{table} cascades with its upstream");
    }

    fixture.drop_schema().await
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

#[allow(clippy::too_many_arguments)]
async fn insert_upstream_spec(
    pool: &PgPool,
    id: Uuid,
    name: &str,
    kind: &str,
    base_url: Option<&str>,
    enabled: bool,
    warmup_enabled: bool,
    spec_revision: i64,
    workspace_id: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO upstream_spec_v1
         (id, name, kind, base_url, enabled, warmup_enabled, spec_revision,
          created_at, updated_at, workspace_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7,
                 TIMESTAMPTZ '2026-01-01 00:00:00+00',
                 TIMESTAMPTZ '2026-01-01 00:00:00+00', $8)",
    )
    .bind(id)
    .bind(name)
    .bind(kind)
    .bind(base_url)
    .bind(enabled)
    .bind(warmup_enabled)
    .bind(spec_revision)
    .bind(workspace_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_anthropic_children(
    pool: &PgPool,
    anthropic_api_id: Uuid,
    anthropic_oauth_id: Uuid,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO upstream_api_key_secret_v1
         (upstream_id, api_key_ciphertext, created_at, updated_at)
         VALUES ($1, $2, TIMESTAMPTZ '2026-01-01 00:00:00+00',
                 TIMESTAMPTZ '2026-01-01 00:00:00+00')",
    )
    .bind(anthropic_api_id)
    .bind(b"legacy-api-key".as_slice())
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO upstream_oauth_token_v1
         (upstream_id, oauth_credentials_ciphertext, oauth_token_generation,
          never_refresh, created_at, updated_at)
         VALUES ($1, $2, 4, TRUE, TIMESTAMPTZ '2026-01-01 00:00:00+00',
                 TIMESTAMPTZ '2026-01-01 00:00:00+00')",
    )
    .bind(anthropic_oauth_id)
    .bind(b"legacy-oauth-token".as_slice())
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO upstream_status_v1
         (upstream_id, last_apply_error, updated_at)
         VALUES ($1, 'legacy apply error', TIMESTAMPTZ '2026-01-01 00:00:00+00')",
    )
    .bind(anthropic_api_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO upstream_lease_v1
         (upstream_id, lease_kind, holder, until_unix_secs, updated_at)
         VALUES ($1, 'refresh', 'legacy-holder', 1234,
                 TIMESTAMPTZ '2026-01-01 00:00:00+00')",
    )
    .bind(anthropic_api_id)
    .execute(pool)
    .await?;
    Ok(())
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
