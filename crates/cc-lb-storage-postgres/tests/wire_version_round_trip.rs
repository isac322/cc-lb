use std::str::FromStr;

use anyhow::Result;
use cc_lb_storage_api::{
    BackendKind, MetaStore, PluginRegistryStore, PluginSlot, WasmBlob, WasmRegistryEntryInput,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

#[test]
fn wire_version_round_trip() {
    let url = match postgres_url() {
        Some(url) => url,
        None => {
            eprintln!("skip: CI_POSTGRES_URL or DATABASE_URL_TEST not set");
            return;
        }
    };

    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(async move { run_test(&url).await })
        .expect("wire version round trip");
}

async fn run_test(url: &str) -> Result<()> {
    let fixture = Fixture::create(url).await?;
    let result = async {
        let storage = PostgresStorage::new(
            fixture.pool.clone(),
            std::sync::Arc::new(cc_lb_core::SystemClock),
        );
        storage.initialize(BackendKind::Postgres).await?;

        let blob = WasmBlob {
            sha256: [2; 32],
            bytes: b"wire-version-plugin".to_vec(),
            size_bytes: b"wire-version-plugin".len() as u64,
            parse_validated_at_unix_secs: 1_800_000_000,
        };
        let input = WasmRegistryEntryInput {
            name: "wire-version-plugin".to_owned(),
            original_filename: "wire-version-plugin.wasm".to_owned(),
            label: Some("Wire version plugin".to_owned()),
            uploaded_at_unix_secs: 1_800_000_100,
            uploaded_by_admin_id: Uuid::new_v4(),
            wire_version: 2,
            supported_slots: vec![PluginSlot::Router],
        };

        let (entry, existed) = storage.persist_wasm_upload(blob, input).await?;
        assert!(!existed);
        assert_eq!(entry.wire_version, 2);

        let by_id = storage
            .get_registry_entry_by_id(entry.id)
            .await?
            .expect("entry should be readable by id");
        assert_eq!(by_id.wire_version, 2);

        storage.update_wire_version(entry.id, 3).await?;
        let updated = storage
            .get_registry_entry_by_id(entry.id)
            .await?
            .expect("entry should be readable after update");
        assert_eq!(updated.wire_version, 3);

        Ok::<_, anyhow::Error>(())
    }
    .await;
    let teardown = fixture.drop_schema().await;

    result?;
    teardown
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let schema = format!("wire_version_round_trip_{}", Uuid::new_v4().simple());
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
