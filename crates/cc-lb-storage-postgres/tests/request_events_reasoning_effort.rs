use std::{error::Error, str::FromStr, sync::Arc};

use cc_lb_storage_api::{MetaStore, RequestEvent, RequestEventStore};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn migrated_schema_has_nullable_text_reasoning_effort() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };

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
        let thinking_level = sqlx::query_as::<_, (String, String)>(
            "SELECT data_type, is_nullable FROM information_schema.columns \
             WHERE table_schema = current_schema() AND table_name = 'request_events_v1' \
             AND column_name = 'thinking_level'",
        )
        .fetch_optional(fixture.storage.pool())
        .await?;
        assert_eq!(thinking_level, Some(("text".to_owned(), "YES".to_owned())));
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn append_request_event_persists_reasoning_effort_value() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
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
async fn append_request_event_accepts_null_reasoning_effort() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
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

#[tokio::test]
async fn thinking_level_roundtrips_without_overloading_reasoning_effort() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let result: TestResult = async {
        for (id, level) in [("level-present", Some("LOW")), ("level-absent", None)] {
            let mut event = request_event(id, None);
            event.thinking_level = level.map(str::to_owned);
            event.thinking_budget_tokens = Some(1024);
            event.service_tier = Some("priority".to_owned());
            fixture.storage.append_request_event(&event).await?;
            let stored = fixture
                .storage
                .get_request_event(id)
                .await?
                .expect("event persisted");
            assert_eq!(stored.thinking_level.as_deref(), level);
            assert_eq!(stored.thinking_budget_tokens, Some(1024));
            assert_eq!(stored.service_tier.as_deref(), Some("priority"));
            assert_eq!(stored.reasoning_effort, None);
        }
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn thinking_level_migration_preserves_legacy_event() -> TestResult {
    let Some(fixture) = Fixture::create_at_version(Some(153)).await? else {
        return Ok(());
    };
    let result: TestResult = async {
        sqlx::query(
            "INSERT INTO request_events_v1 \
             (request_id, ts, event_id, input_tokens, output_tokens, list_status, duration_ms) \
             VALUES ('legacy-level', to_timestamp(1800300100), 'legacy-level', 12, 34, 200, 89)",
        )
        .execute(fixture.storage.pool())
        .await?;
        fixture.storage.initialize().await?;
        let stored = fixture
            .storage
            .get_request_event("legacy-level")
            .await?
            .expect("legacy event preserved");
        assert_eq!(stored.input_tokens, Some(12));
        assert_eq!(stored.output_tokens, Some(34));
        assert_eq!(stored.thinking_level, None);
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
    async fn create() -> TestResult<Option<Self>> {
        Self::create_at_version(None).await
    }

    async fn create_at_version(version: Option<i64>) -> TestResult<Option<Self>> {
        let Some(database_url) = std::env::var("CI_POSTGRES_URL").ok() else {
            eprintln!("skipped: CI_POSTGRES_URL unset");
            return Ok(None);
        };
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
        let storage = PostgresStorage::new(pool, Arc::new(cc_lb_clock::SystemClock));
        let initialized: TestResult = if let Some(version) = version {
            let mut connection = storage.pool().acquire().await?;
            sqlx::migrate!("./migrations")
                .run_direct(Some(version), &mut *connection, false)
                .await
                .map_err(Into::into)
        } else {
            storage.initialize().await.map_err(Into::into)
        };
        if let Err(error) = initialized {
            storage.pool().close().await;
            sqlx::query(AssertSqlSafe(format!(
                "DROP SCHEMA IF EXISTS {} CASCADE",
                quote_ident(&schema)
            )))
            .execute(&admin_pool)
            .await?;
            return Err(error);
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

fn request_event(event_id: &str, reasoning_effort: Option<String>) -> RequestEvent {
    RequestEvent {
        ts: 1_800_300_100,
        ts_ms: Some(1_800_300_100_000),
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
