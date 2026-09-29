use std::{error::Error, str::FromStr, sync::Arc};

use cc_lb_storage_api::{MetaStore, RequestEvent, RequestEventStore};
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
        fixture.storage.append_request_event(&event).await?;
        let stored = select_cost_components(&fixture, &event).await?;

        assert_eq!(stored, (None, None, None, None, None, None));
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn request_setup_timings_roundtrip_through_postgres_payload() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let event = RequestEvent {
        status: 502,
        json_parse_ms: Some(0.125),
        cache_structure_ms: Some(0.0),
        cache_token_key_ms: Some(0.25),
        cache_count_lookup_ms: Some(0.5),
        cache_tokenizer_queue_ms: Some(0.75),
        cache_serialize_ms: Some(1.0),
        cache_tokenize_ms: None,
        prepare_signer_ms: Some(2.0),
        ..request_event("setup-timings")
    };

    let result: TestResult = async {
        fixture.storage.append_request_event(&event).await?;
        let payload: Vec<u8> =
            sqlx::query_scalar("SELECT payload FROM request_events_v1 WHERE event_id = $1")
                .bind(event.event_id.as_deref())
                .fetch_one(fixture.storage.pool())
                .await?;
        let restored: RequestEvent = serde_json::from_slice(&payload)?;

        assert_eq!(restored.json_parse_ms, Some(0.125));
        assert_eq!(restored.cache_structure_ms, Some(0.0));
        assert_eq!(restored.cache_token_key_ms, Some(0.25));
        assert_eq!(restored.cache_count_lookup_ms, Some(0.5));
        assert_eq!(restored.cache_tokenizer_queue_ms, Some(0.75));
        assert_eq!(restored.cache_serialize_ms, Some(1.0));
        assert_eq!(restored.cache_tokenize_ms, None);
        assert_eq!(restored.prepare_signer_ms, Some(2.0));
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

async fn select_cost_components(
    fixture: &Fixture,
    event: &RequestEvent,
) -> TestResult<StoredCostComponents> {
    Ok(sqlx::query_as::<_, StoredCostComponents>(
        "SELECT list_cost_usd_micros, list_cost_input_micros, list_cost_output_micros, \
                list_cost_cache_creation_5m_micros, list_cost_cache_creation_1h_micros, \
                list_cost_cache_read_micros \
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
        if let Err(error) = storage.initialize().await {
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
        ts_ms: Some(1_800_300_100_000),
        event_id: Some(event_id.to_owned()),
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
