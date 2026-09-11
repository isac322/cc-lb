use std::{error::Error, str::FromStr};

use cc_lb_storage_api::{BackendKind, MetaStore, RequestEvent, RequestEventStore};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn t3_postgres__migrated_schema_has_nullable_text_reasoning_effort() -> TestResult {
    let fixture = Fixture::create().await?;

    let result: TestResult = async {
        let column = sqlx::query_as::<_, (String, String)>(
            "SELECT data_type, is_nullable \
             FROM information_schema.columns \
             WHERE table_schema = current_schema() \
               AND table_name = 'request_events_v1' \
               AND column_name = 'reasoning_effort'",
        )
        .fetch_optional(fixture.storage.pool())
        .await?;

        assert_eq!(column, Some(("text".to_owned(), "YES".to_owned())));
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn t3_postgres__append_request_event_persists_reasoning_effort_value() -> TestResult {
    let fixture = Fixture::create().await?;
    let event = request_event("reasoning-effort-some", Some("max".to_owned()));

    let result: TestResult = async {
        fixture.storage.append_request_event(&event).await?;
        let stored = sqlx::query_scalar::<_, Option<String>>(
            "SELECT reasoning_effort FROM request_events_v1 WHERE event_id = $1",
        )
        .bind(event.event_id.as_deref())
        .fetch_one(fixture.storage.pool())
        .await?;

        assert_eq!(stored.as_deref(), Some("max"));
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn t3_postgres__append_request_event_accepts_null_reasoning_effort() -> TestResult {
    let fixture = Fixture::create().await?;
    let event = request_event("reasoning-effort-none", None);

    let result: TestResult = async {
        fixture.storage.append_request_event(&event).await?;
        let stored = sqlx::query_scalar::<_, Option<String>>(
            "SELECT reasoning_effort FROM request_events_v1 WHERE event_id = $1",
        )
        .bind(event.event_id.as_deref())
        .fetch_one(fixture.storage.pool())
        .await?;

        assert_eq!(stored, None);
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    storage: PostgresStorage,
}

impl Fixture {
    async fn create() -> TestResult<Self> {
        let database_url = crate::postgres_fixture::required_postgres_url();
        let schema = format!(
            "cc_lb_app_test_reasoning_effort_{}",
            Uuid::new_v4().simple()
        );
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
        let storage = PostgresStorage::new(pool, cc_lb_testkit::fixed_clock(1_700_000_000));
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

        Ok(Self {
            schema,
            admin_pool,
            storage,
        })
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

fn request_event(event_id: &str, reasoning_effort: Option<String>) -> RequestEvent {
    RequestEvent {
        ts: 1_800_300_100,
        request_id: format!("request-{event_id}"),
        event_id: Some(event_id.to_owned()),
        reasoning_effort,
        status: 200,
        duration_ms: 89,
        request_body_first_chunk_ms: None,
        request_body_receive_ms: None,
        request_body_wait_ms: None,
        request_body_process_ms: None,
        request_body_chunk_count: None,
        response_body_wait_ms: None,
        response_body_process_ms: None,
        response_body_downstream_poll_gap_ms: None,
        retry_overhead_ms: None,
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
