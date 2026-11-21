use std::{error::Error, sync::Arc};

use cc_lb_storage_api::{MetaStore, RequestEvent, RequestEventStore};
use cc_lb_storage_postgres::PostgresStorage;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn migrated_schema_has_nullable_nonnegative_bigint_tool_use_prompt_tokens() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };

    let result: TestResult = async {
        let column = sqlx::query_as::<_, (String, String)>(
            "SELECT data_type, is_nullable \
             FROM information_schema.columns \
             WHERE table_schema = current_schema() \
               AND table_name = 'request_events_v1' \
               AND column_name = 'tool_use_prompt_tokens'",
        )
        .fetch_optional(fixture.storage.pool())
        .await?;

        assert_eq!(column, Some(("bigint".to_owned(), "YES".to_owned())));
        let constraint: Option<String> = sqlx::query_scalar(
            "SELECT pg_get_constraintdef(oid) \
             FROM pg_constraint \
             WHERE conrelid = 'request_events_v1'::regclass \
               AND conname LIKE '%tool_use_prompt_tokens%'",
        )
        .fetch_optional(fixture.storage.pool())
        .await?;
        assert!(
            constraint.is_some(),
            "tool-use token nonnegative check missing"
        );
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn append_request_event_roundtrips_tool_use_prompt_tokens_some() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let event = request_event("tool-use-some", Some(42));

    let result: TestResult = async {
        fixture.storage.append_request_event(&event).await?;
        let stored = fixture
            .storage
            .get_request_event(event.event_id.as_deref().expect("event id"))
            .await?
            .expect("event persisted");
        assert_eq!(stored.tool_use_prompt_tokens, Some(42));
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn append_request_event_roundtrips_tool_use_prompt_tokens_none() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let event = request_event("tool-use-none", None);

    let result: TestResult = async {
        fixture.storage.append_request_event(&event).await?;
        let stored = fixture
            .storage
            .get_request_event(event.event_id.as_deref().expect("event id"))
            .await?
            .expect("event persisted");
        assert_eq!(stored.tool_use_prompt_tokens, None);
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

fn request_event(event_id: &str, tool_use_prompt_tokens: Option<u64>) -> RequestEvent {
    RequestEvent {
        ts: 1_800_300_100,
        ts_ms: Some(1_800_300_100_000),
        request_id: format!("request-{event_id}"),
        event_id: Some(event_id.to_owned()),
        tool_use_prompt_tokens,
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
