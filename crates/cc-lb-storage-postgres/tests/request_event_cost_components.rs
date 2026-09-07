use std::{error::Error, str::FromStr, sync::Arc};

use cc_lb_storage_api::{BackendKind, MetaStore, RequestEvent, RequestEventStore};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
type StoredCostComponents = (
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    bool,
);

#[tokio::test]
async fn append_request_event_persists_cost_component_columns() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let event = RequestEvent {
        cost_usd_micros: Some(101),
        cost_input_micros: Some(102),
        cost_output_micros: Some(103),
        cost_cache_creation_5m_micros: Some(104),
        cost_cache_creation_1h_micros: Some(105),
        cost_cache_read_micros: Some(106),
        ..request_event("cost-components-some")
    };

    let result: TestResult = async {
        disable_compatibility_trigger(&fixture).await?;
        fixture.storage.append_request_event(&event).await?;
        let stored = select_cost_components(&fixture, &event).await?;

        assert_eq!(
            stored,
            (
                Some(101),
                Some(102),
                Some(103),
                Some(104),
                Some(105),
                Some(106),
                true,
            )
        );
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn append_request_event_leaves_unrecorded_cost_components_null() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let event = request_event("cost-components-none");

    let result: TestResult = async {
        disable_compatibility_trigger(&fixture).await?;
        fixture.storage.append_request_event(&event).await?;
        let stored = select_cost_components(&fixture, &event).await?;

        assert_eq!(stored, (None, None, None, None, None, None, true));
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn legacy_insert_trigger_materializes_cost_components() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let event = RequestEvent {
        cost_usd_micros: Some(201),
        cost_input_micros: Some(202),
        cost_output_micros: Some(203),
        cost_cache_creation_5m_micros: Some(204),
        cost_cache_creation_1h_micros: Some(205),
        cost_cache_read_micros: Some(206),
        ..request_event("cost-components-legacy")
    };
    let payload = serde_json::to_vec(&event)?;

    let result: TestResult = async {
        sqlx::query(
            "INSERT INTO request_events_v1 (ts, event_id, payload) \
             VALUES (NOW(), $1, $2)",
        )
        .bind(event.event_id.as_deref())
        .bind(payload)
        .execute(fixture.storage.pool())
        .await?;
        let stored = select_cost_components(&fixture, &event).await?;

        assert_eq!(
            stored,
            (
                Some(201),
                Some(202),
                Some(203),
                Some(204),
                Some(205),
                Some(206),
                true,
            )
        );
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn cost_component_migrations_reapply_after_registry_rewind() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let backfill_event = RequestEvent {
        cost_usd_micros: Some(301),
        cost_input_micros: Some(302),
        cost_output_micros: Some(303),
        cost_cache_creation_5m_micros: Some(304),
        cost_cache_creation_1h_micros: Some(305),
        cost_cache_read_micros: Some(306),
        ..request_event("cost-components-reapply-backfill")
    };
    let trigger_event = RequestEvent {
        cost_usd_micros: Some(401),
        cost_input_micros: Some(402),
        cost_output_micros: Some(403),
        cost_cache_creation_5m_micros: Some(404),
        cost_cache_creation_1h_micros: Some(405),
        cost_cache_read_micros: Some(406),
        ..request_event("cost-components-reapply-trigger")
    };

    let result: TestResult = async {
        disable_compatibility_trigger(&fixture).await?;
        insert_legacy_event(&fixture, &backfill_event).await?;
        sqlx::query("DELETE FROM _sqlx_migrations WHERE version BETWEEN 108 AND 111")
            .execute(fixture.storage.pool())
            .await?;

        fixture.storage.initialize(BackendKind::Postgres).await?;

        let reapplied_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM _sqlx_migrations WHERE version BETWEEN 108 AND 111",
        )
        .fetch_one(fixture.storage.pool())
        .await?;
        assert_eq!(reapplied_count, 4);
        assert_eq!(
            select_cost_components(&fixture, &backfill_event).await?,
            (
                Some(301),
                Some(302),
                Some(303),
                Some(304),
                Some(305),
                Some(306),
                true,
            )
        );

        insert_legacy_event(&fixture, &trigger_event).await?;
        assert_eq!(
            select_cost_components(&fixture, &trigger_event).await?,
            (
                Some(401),
                Some(402),
                Some(403),
                Some(404),
                Some(405),
                Some(406),
                true,
            )
        );
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

async fn insert_legacy_event(fixture: &Fixture, event: &RequestEvent) -> TestResult {
    let payload = serde_json::to_vec(event)?;
    sqlx::query(
        "INSERT INTO request_events_v1 (ts, event_id, payload, list_ts_ms) \
         VALUES (NOW(), $1, $2, (EXTRACT(EPOCH FROM now()) * 1000)::bigint)",
    )
    .bind(event.event_id.as_deref())
    .bind(payload)
    .execute(fixture.storage.pool())
    .await?;
    Ok(())
}

async fn disable_compatibility_trigger(fixture: &Fixture) -> TestResult {
    sqlx::query(
        "ALTER TABLE request_events_v1 \
         DISABLE TRIGGER request_events_v1_materialize_cost_components",
    )
    .execute(fixture.storage.pool())
    .await?;
    Ok(())
}

async fn select_cost_components(
    fixture: &Fixture,
    event: &RequestEvent,
) -> TestResult<StoredCostComponents> {
    Ok(sqlx::query_as::<_, StoredCostComponents>(
        "SELECT list_cost_usd_micros, list_cost_input_micros, list_cost_output_micros, \
                list_cost_cache_creation_5m_micros, list_cost_cache_creation_1h_micros, \
                list_cost_cache_read_micros, list_cost_components_materialized \
         FROM request_events_v1 \
         WHERE event_id = $1",
    )
    .bind(event.event_id.as_deref())
    .fetch_one(fixture.storage.pool())
    .await?)
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    storage: PostgresStorage,
}

impl Fixture {
    async fn create() -> TestResult<Option<Self>> {
        let Some(database_url) = std::env::var("CI_POSTGRES_URL").ok() else {
            eprintln!("skipped: CI_POSTGRES_URL unset");
            return Ok(None);
        };
        let schema = format!("cc_lb_app_test_cost_components_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(&database_url)?)
            .await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;

        let search_path = format!("{schema}, public");
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(
                PgConnectOptions::from_str(&database_url)?
                    .options([("search_path", search_path.as_str())]),
            )
            .await?;
        let storage = PostgresStorage::new(pool, Arc::new(cc_lb_clock::SystemClock));
        if let Err(error) = storage.initialize(BackendKind::Postgres).await {
            storage.pool().close().await;
            sqlx::query(AssertSqlSafe(format!(
                "DROP SCHEMA IF EXISTS {} CASCADE",
                quote_ident(&schema)
            )))
            .execute(&admin_pool)
            .await?;
            return Err(error.into());
        }

        Ok(Some(Self {
            schema,
            admin_pool,
            storage,
        }))
    }

    async fn drop_schema(self) -> TestResult {
        self.storage.pool().close().await;
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

fn request_event(event_id: &str) -> RequestEvent {
    RequestEvent {
        ts: 1_800_300_100,
        request_id: format!("request-{event_id}"),
        event_id: Some(event_id.to_owned()),
        status: 200,
        duration_ms: 89,
        ..Default::default()
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
