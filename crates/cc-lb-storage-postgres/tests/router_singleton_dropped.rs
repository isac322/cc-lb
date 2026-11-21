use anyhow::Result;
use cc_lb_storage_api::{
    MetaStore, PluginChainEntryInput, PluginRegistryStore, PluginSlotKind, PrincipalCreate,
    PrincipalKind, PrincipalStore, WasmBlob, WasmRegistryEntryInput,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::PgPool;
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
        std::sync::Arc::new(cc_lb_clock::SystemClock),
    );
    storage.initialize().await?;

    let principal = storage
        .create(
            PrincipalCreate {
                name: "test-principal".to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
                cache_keepalive: None,
                openai_oauth_compat: None,
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
            },
            WasmRegistryEntryInput {
                name: "test-plugin".to_owned(),
                version: None,
                original_filename: "test.wasm".to_owned(),
                label: None,
                uploaded_at_unix_secs: 1_800_000_000,
                uploaded_by_admin_id: Uuid::new_v4(),
                description: "test plugin".to_owned(),
                usage: "test usage".to_owned(),
                hook_metadata: Default::default(),
                supported_slots: Vec::new(),
            },
        )
        .await?;
    let registry_id = registry_entry.id;

    // Test 1: Verify Shape slot is still singleton
    let input1 = PluginChainEntryInput {
        principal_id,
        slot: PluginSlotKind::Shape,
        order: 100,
        wasm_registry_id: registry_id,
        config: serde_json::json!({}),
    };

    let entry1 = storage.insert_chain_entry(input1.clone()).await?;
    assert_eq!(entry1.slot, PluginSlotKind::Shape);

    // Attempting to insert another Shape plugin should fail
    let result = storage.insert_chain_entry(input1).await;
    assert!(result.is_err(), "Shape slot should still be singleton");

    // Test 2: Verify Router slot is NO LONGER singleton
    let input_router1 = PluginChainEntryInput {
        principal_id,
        slot: PluginSlotKind::Router,
        order: 200,
        wasm_registry_id: registry_id,
        config: serde_json::json!({}),
    };

    let entry_router1 = storage.insert_chain_entry(input_router1.clone()).await?;
    assert_eq!(entry_router1.slot, PluginSlotKind::Router);

    // Inserting a second Router plugin should succeed (no longer singleton)
    let input_router2 = PluginChainEntryInput {
        principal_id,
        slot: PluginSlotKind::Router,
        order: 201,
        wasm_registry_id: registry_id,
        config: serde_json::json!({}),
    };

    let entry_router2 = storage.insert_chain_entry(input_router2).await?;
    assert_eq!(entry_router2.slot, PluginSlotKind::Router);
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
    isolated: crate::pg_test_support::IsolatedPg,
    pool: PgPool,
}

impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let isolated = crate::pg_test_support::IsolatedPg::create_templated(url).await?;
        let pool = match isolated.pool(4).await {
            Ok(pool) => pool,
            Err(error) => {
                let _ = isolated.drop().await;
                return Err(error);
            }
        };

        Ok(Self { isolated, pool })
    }

    async fn drop_schema(self) -> Result<()> {
        self.pool.close().await;
        self.isolated.drop().await
    }
}
