use std::str::FromStr;

use anyhow::Result;
use cc_lb_storage_api::principal::{Limit, LimitKind};
use cc_lb_storage_api::{
    BackendKind, MetaStore, PrincipalCreate, PrincipalKind, PrincipalStore, PrincipalUpdate,
    StorageError,
};
use cc_lb_storage_postgres::PostgresStorage;
use serde_json::json;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgConnectOptions, postgres::PgPoolOptions};
use uuid::Uuid;

fn get_postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL").ok()
}

#[test]
fn principal_terminal_strategy() {
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
        .expect("principal terminal strategy test");
}

async fn run_test(url: &str) -> Result<()> {
    let fixture = Fixture::create(url).await?;
    let storage = PostgresStorage::new(fixture.pool.clone());
    storage.initialize(BackendKind::Postgres).await?;

    default_strategy_is_first_pick(&storage).await?;
    update_strategy_to_random_roundtrips(&storage).await?;
    unsupported_strategy_update_is_rejected(&storage).await?;

    fixture.drop_schema().await
}

async fn default_strategy_is_first_pick(storage: &PostgresStorage) -> Result<()> {
    let record =
        PrincipalStore::create(storage, principal_create("default-strategy"), 1_900_000_000)
            .await?;
    assert_eq!(
        strategy_json(&record.router_terminal_strategy)?,
        json!("first-pick")
    );

    let fetched = PrincipalStore::get_by_id(storage, record.id)
        .await?
        .expect("record should exist");
    assert_eq!(
        strategy_json(&fetched.router_terminal_strategy)?,
        json!("first-pick")
    );
    Ok(())
}

async fn update_strategy_to_random_roundtrips(storage: &PostgresStorage) -> Result<()> {
    let record =
        PrincipalStore::create(storage, principal_create("random-strategy"), 1_900_000_010).await?;
    let random_strategy = serde_json::from_value(json!("random"))?;

    let updated = PrincipalStore::update(
        storage,
        record.id,
        record.revision,
        PrincipalUpdate {
            router_terminal_strategy: Some(random_strategy),
            ..PrincipalUpdate::default()
        },
        1_900_000_011,
    )
    .await?
    .expect("record should exist");
    assert_eq!(
        strategy_json(&updated.router_terminal_strategy)?,
        json!("random")
    );

    let fetched = PrincipalStore::get_by_name(storage, &record.name)
        .await?
        .expect("record should exist");
    assert_eq!(
        strategy_json(&fetched.router_terminal_strategy)?,
        json!("random")
    );
    Ok(())
}

async fn unsupported_strategy_update_is_rejected(storage: &PostgresStorage) -> Result<()> {
    let record = PrincipalStore::create(
        storage,
        principal_create("unsupported-strategy"),
        1_900_000_020,
    )
    .await?;
    let unsupported_strategy = serde_json::from_value(json!("unsupported"))?;

    let error = PrincipalStore::update(
        storage,
        record.id,
        record.revision,
        PrincipalUpdate {
            router_terminal_strategy: Some(unsupported_strategy),
            ..PrincipalUpdate::default()
        },
        1_900_000_021,
    )
    .await
    .expect_err("unsupported terminal strategy must be rejected by postgres");
    assert!(
        matches!(error, StorageError::InvalidInput { .. }),
        "unexpected error: {error:?}"
    );

    let fetched = PrincipalStore::get_by_id(storage, record.id)
        .await?
        .expect("record should exist");
    assert_eq!(
        strategy_json(&fetched.router_terminal_strategy)?,
        json!("first-pick")
    );
    Ok(())
}

fn principal_create(name: &str) -> PrincipalCreate {
    PrincipalCreate {
        name: name.to_owned(),
        kind: PrincipalKind::Machine,
        allowed_models: vec!["claude-sonnet-*".to_owned()],
        allowed_upstreams: vec![],
        default_limits: vec![Limit {
            kind: LimitKind::Requests,
            window_secs: 60,
            cap_micros: 100,
        }],
    }
}

fn strategy_json(strategy: &impl serde::Serialize) -> Result<serde_json::Value> {
    Ok(serde_json::to_value(strategy)?)
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let schema = format!("principal_terminal_strategy_{}", Uuid::new_v4().simple());
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
