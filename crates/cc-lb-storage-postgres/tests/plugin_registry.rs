#![allow(deprecated)]

use std::str::FromStr;

use anyhow::Result;
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamWarmupDialectPlugin};
use cc_lb_storage_api::{
    BackendKind, MetaStore, PluginBlobRepo, PluginRegistryStore, UpstreamCreate, UpstreamStore,
    WasmBlob, WasmRegistryEntryInput,
};
use cc_lb_storage_postgres::{PostgresPluginBlobRepo, PostgresPluginRegistryRepo, PostgresStorage};
use sqlx::{AssertSqlSafe, PgPool, postgres::PgConnectOptions, postgres::PgPoolOptions};
use uuid::Uuid;

fn get_postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL").ok()
}

#[test]
fn plugin_registry_postgres_conformance() {
    let url = match get_postgres_url() {
        Some(url) => url,
        None => {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return;
        }
    };

    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(async move { run_conformance(&url).await })
        .expect("plugin registry postgres conformance");
}

#[test]
fn delete_registry_rejects_when_referenced_by_warmup_dialect_postgres() {
    let url = match get_postgres_url() {
        Some(url) => url,
        None => {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return;
        }
    };

    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(async move { run_warmup_dialect_guard(&url).await })
        .expect("delete-safety postgres");
}

async fn run_warmup_dialect_guard(url: &str) -> Result<()> {
    let fixture = Fixture::create(url).await?;
    let storage = PostgresStorage::new(fixture.pool.clone());

    let blob_bytes = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
    let blob = WasmBlob {
        sha256: [42; 32],
        size_bytes: blob_bytes.len() as u64,
        bytes: blob_bytes,
        parse_validated_at_unix_secs: 1_800_000_000,
    };
    let entry = WasmRegistryEntryInput {
        name: "warmup-dialect-test".to_owned(),
        original_filename: "warmup-dialect-test.wasm".to_owned(),
        label: None,
        uploaded_at_unix_secs: 1_800_000_100,
        uploaded_by_admin_id: Uuid::new_v4(),
        wire_version: 1,
        supported_slots: Vec::new(),
    };

    let result: Result<()> = async {
        let (registry, _) = storage.persist_wasm_upload(blob, entry).await?;
        UpstreamStore::create(
            &storage,
            UpstreamCreate {
                name: format!("oauth-{}", Uuid::new_v4().simple()),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: true,
                next_warmup_at: None,
                last_warmup_cycle_key: None,
                warmup_lease_holder: None,
                warmup_lease_until_unix_secs: None,
                warmup_dialect_plugin: Some(UpstreamWarmupDialectPlugin {
                    wasm_registry_id: registry.id,
                    config: serde_json::Value::Null,
                    wire_version: Some(1),
                }),
            },
        )
        .await?;

        let err = storage
            .delete_registry_entry(registry.id, registry.revision)
            .await
            .expect_err("delete must reject while referenced by warmup_dialect_plugin");
        assert!(
            err.to_string().contains("referenced"),
            "error mentions referenced: {err}"
        );
        assert!(
            storage
                .get_registry_entry_by_id(registry.id)
                .await?
                .is_some(),
            "registry entry remains after rejected delete"
        );
        Ok(())
    }
    .await;

    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

async fn run_conformance(url: &str) -> Result<()> {
    let fixture = Fixture::create(url).await?;
    let registry = PostgresPluginRegistryRepo::new(fixture.pool.clone());
    let blobs = PostgresPluginBlobRepo::new(fixture.pool.clone());

    let result =
        cc_lb_storage_conformance::plugin_registry::plugin_registry_roundtrip(&registry, &blobs)
            .await;
    let result = match result {
        Ok(()) => blob_roundtrip(&blobs).await,
        Err(error) => Err(error),
    };
    let teardown = fixture.drop_schema().await;

    result?;
    teardown
}

async fn blob_roundtrip(blobs: &PostgresPluginBlobRepo) -> Result<()> {
    let sha256 = [90; 32];
    blobs.put_blob(&sha256, b"plugin wasm bytes").await?;
    assert_eq!(
        blobs.get_blob(&sha256).await?.as_deref(),
        Some(b"plugin wasm bytes".as_slice())
    );
    assert!(blobs.list_blob_keys().await?.contains(&sha256));
    blobs.delete_blob(&sha256).await?;
    assert!(blobs.get_blob(&sha256).await?.is_none());
    Ok(())
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let schema = format!("plugin_registry_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;

        let pool = schema_pool(url, &schema).await?;
        let storage = PostgresStorage::new(pool.clone());
        storage.initialize(BackendKind::Postgres).await?;

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
