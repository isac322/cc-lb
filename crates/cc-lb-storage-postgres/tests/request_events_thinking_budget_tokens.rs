use std::{error::Error, sync::Arc};

use cc_lb_storage_api::{MetaStore, RequestEvent, RequestEventStore};
use cc_lb_storage_postgres::PostgresStorage;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn migrated_schema_has_nullable_bigint_thinking_budget_tokens() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };

    let result: TestResult = async {
        let column = sqlx::query_as::<_, (String, String)>(
            "SELECT data_type, is_nullable \
             FROM information_schema.columns \
             WHERE table_schema = current_schema() \
               AND table_name = 'request_events_v1' \
               AND column_name = 'thinking_budget_tokens'",
        )
        .fetch_optional(fixture.storage.pool())
        .await?;

        assert_eq!(column, Some(("bigint".to_owned(), "YES".to_owned())));
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn append_request_event_persists_thinking_budget_tokens_value() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let event = request_event("thinking-budget-some", Some(18_000));

    let result: TestResult = async {
        fixture.storage.append_request_event(&event).await?;
        let stored = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT thinking_budget_tokens FROM request_events_v1 WHERE event_id = $1",
        )
        .bind(event.event_id.as_deref())
        .fetch_one(fixture.storage.pool())
        .await?;

        assert_eq!(stored, Some(18_000));
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn append_request_event_accepts_null_thinking_budget_tokens() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let event = request_event("thinking-budget-none", None);

    let result: TestResult = async {
        fixture.storage.append_request_event(&event).await?;
        let stored = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT thinking_budget_tokens FROM request_events_v1 WHERE event_id = $1",
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
    isolated: crate::pg_test_support::IsolatedPg,
    storage: PostgresStorage,
}

impl Fixture {
    async fn create() -> TestResult<Option<Self>> {
        let Some(database_url) = std::env::var("CI_POSTGRES_URL").ok() else {
            eprintln!("skipped: CI_POSTGRES_URL unset");
            return Ok(None);
        };
        let isolated = crate::pg_test_support::IsolatedPg::create_templated(&database_url).await?;
        let pool = match isolated.pool(2).await {
            Ok(pool) => pool,
            Err(error) => {
                let _ = isolated.drop().await;
                return Err(error.into());
            }
        };
        let storage = PostgresStorage::new(pool, Arc::new(cc_lb_clock::SystemClock));
        if let Err(error) = storage.initialize().await {
            storage.pool().close().await;
            let _ = isolated.drop().await;
            return Err(error.into());
        }

        Ok(Some(Self { isolated, storage }))
    }

    async fn drop_schema(self) -> TestResult {
        self.storage.pool().close().await;
        self.isolated.drop().await?;
        Ok(())
    }
}

fn request_event(event_id: &str, thinking_budget_tokens: Option<u64>) -> RequestEvent {
    RequestEvent {
        ts: 1_800_300_100,
        ts_ms: Some(1_800_300_100_000),
        request_id: format!("request-{event_id}"),
        event_id: Some(event_id.to_owned()),
        thinking_budget_tokens,
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
