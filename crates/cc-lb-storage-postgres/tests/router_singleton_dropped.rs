use std::str::FromStr;

use anyhow::Result;
use cc_lb_storage_api::{
    BackendKind, MetaStore, PluginChainEntryInput, PluginRegistryStore, PluginSlot,
    PrincipalCreate, PrincipalKind, PrincipalStore, WasmBlob, WasmRegistryEntryInput,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgConnectOptions, postgres::PgPoolOptions};
use uuid::Uuid;

fn get_postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL").ok()
}

#[test]
fn router_singleton_dropped() {
    let url = match get_postgres_url() {
        Some(url) => url,
        None => {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return;
        }
    };

    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(async move { run_test(&url).await })
        .expect("router singleton dropped test");
}

async fn run_test(url: &str) -> Result<()> {
    let fixture = Fixture::create(url).await?;
    let storage = PostgresStorage::new(
        fixture.pool.clone(),
        std::sync::Arc::new(cc_lb_core::SystemClock),
    );
    storage.initialize(BackendKind::Postgres).await?;

    let principal = storage
        .create(
            PrincipalCreate {
                name: "test-principal".to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
            },
            1_800_000_000,
        )
        .await?;
    let principal_id = principal.id;

    let (registry_entry, _) = storage
        .persist_wasm_upload(
            WasmBlob {
                sha256: [42u8; 32],
                bytes: b"fake wasm bytes".to_vec(),
                size_bytes: b"fake wasm bytes".len() as u64,
                parse_validated_at_unix_secs: 1_800_000_000,
            },
            WasmRegistryEntryInput {
                schema_hash: None,
                name: "test-plugin".to_owned(),
                original_filename: "test.wasm".to_owned(),
                label: None,
                uploaded_at_unix_secs: 1_800_000_000,
                uploaded_by_admin_id: Uuid::new_v4(),
                wire_version: 1,
                supported_slots: Vec::new(),
            },
        )
        .await?;
    let registry_id = registry_entry.id;

    // Test 1: Verify Shape slot is still singleton
    let input1 = PluginChainEntryInput {
        principal_id,
        slot: PluginSlot::Shape,
        order: 100,
        wasm_registry_id: registry_id,
        config: serde_json::json!({}),
        sse_per_event: false,
        batched_events_per_flush: 32,
        batched_flush_ms: 100,
        wire_version: None,
    };

    let entry1 = storage.insert_chain_entry(input1.clone()).await?;
    assert_eq!(entry1.slot, PluginSlot::Shape);

    // Attempting to insert another Shape plugin should fail
    let result = storage.insert_chain_entry(input1).await;
    assert!(result.is_err(), "Shape slot should still be singleton");

    // Test 2: Verify Router slot is NO LONGER singleton
    let input_router1 = PluginChainEntryInput {
        principal_id,
        slot: PluginSlot::Router,
        order: 200,
        wasm_registry_id: registry_id,
        config: serde_json::json!({}),
        sse_per_event: false,
        batched_events_per_flush: 32,
        batched_flush_ms: 100,
        wire_version: None,
    };

    let entry_router1 = storage.insert_chain_entry(input_router1.clone()).await?;
    assert_eq!(entry_router1.slot, PluginSlot::Router);

    // Inserting a second Router plugin should succeed (no longer singleton)
    let input_router2 = PluginChainEntryInput {
        principal_id,
        slot: PluginSlot::Router,
        order: 201,
        wasm_registry_id: registry_id,
        config: serde_json::json!({}),
        sse_per_event: false,
        batched_events_per_flush: 32,
        batched_flush_ms: 100,
        wire_version: None,
    };

    let entry_router2 = storage.insert_chain_entry(input_router2).await?;
    assert_eq!(entry_router2.slot, PluginSlot::Router);
    assert_ne!(
        entry_router1.id, entry_router2.id,
        "Should be distinct router entries"
    );

    // Test 3: Verify router_terminal_strategy column exists
    let strategy: String =
        sqlx::query_scalar("SELECT router_terminal_strategy FROM principals_v1 WHERE id = $1")
            .bind(principal_id)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(
        strategy, "first-pick",
        "Default strategy should be first-pick"
    );

    fixture.drop_schema().await
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let schema = format!("router_singleton_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;

        let pool = schema_pool(url, &schema).await?;

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

async fn schema_pool(url: &str, schema: &str) -> Result<PgPool> {
    let options = PgConnectOptions::from_str(url)?.options([("search_path", schema)]);
    Ok(PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await?)
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
