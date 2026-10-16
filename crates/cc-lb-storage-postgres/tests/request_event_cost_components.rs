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
async fn request_setup_timings_roundtrip_through_postgres_columns() -> TestResult {
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
        let restored = fixture
            .storage
            .get_request_event(event.event_id.as_deref().expect("fixture event id"))
            .await?
            .expect("stored request event");

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

#[tokio::test]
async fn every_retained_request_event_scalar_roundtrips_relationally() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let event = RequestEvent {
        ts: 1_800_000_000,
        request_id: "roundtrip-request_id".to_owned(),
        source_kind: Some("roundtrip-source_kind".to_owned()),
        source_ref_id: Some("roundtrip-source_ref_id".to_owned()),
        event_kind: Some(cc_lb_storage_api::RequestEventKind::Messages),
        ts_ms: Some(1_800_000_000_123),
        principal_id: Some("roundtrip-principal_id".to_owned()),
        key_id: Some("roundtrip-key_id".to_owned()),
        principal_kind: Some("roundtrip-principal_kind".to_owned()),
        upstream_id: Some(Uuid::from_u128(10)),
        upstream_name: Some("roundtrip-upstream_name".to_owned()),
        model: Some("roundtrip-model".to_owned()),
        status: 502,
        input_tokens: Some(14),
        output_tokens: Some(15),
        cache_creation_input_tokens: Some(16),
        cache_creation_input_tokens_5m: Some(17),
        cache_creation_input_tokens_1h: Some(18),
        cache_read_input_tokens: Some(19),
        thread_id: Some("roundtrip-thread_id".to_owned()),
        observed_session_id: Some("roundtrip-observed_session_id".to_owned()),
        request_kind: Some("roundtrip-request_kind".to_owned()),
        claude_agent_id: Some("roundtrip-claude_agent_id".to_owned()),
        claude_parent_agent_id: Some("roundtrip-claude_parent_agent_id".to_owned()),
        parent_session_id: Some("roundtrip-parent_session_id".to_owned()),
        client_app: Some("roundtrip-client_app".to_owned()),
        session_id_source: Some("roundtrip-session_id_source".to_owned()),
        thinking_budget_tokens: Some(28),
        reasoning_effort: Some("roundtrip-reasoning_effort".to_owned()),
        token_estimate_source: Some("roundtrip-token_estimate_source".to_owned()),
        cost_usd_micros: Some(31),
        cost_input_micros: Some(32),
        cost_output_micros: Some(33),
        cost_cache_creation_5m_micros: Some(34),
        cost_cache_creation_1h_micros: Some(35),
        cost_cache_read_micros: Some(36),
        auth_ms: Some(37),
        route_ms: Some(38),
        limit_reserve_ms: Some(39),
        json_parse_ms: Some(40.125),
        cache_structure_ms: Some(41.125),
        cache_token_key_ms: Some(42.125),
        cache_count_lookup_ms: Some(43.125),
        cache_tokenizer_queue_ms: Some(44.125),
        cache_serialize_ms: Some(45.125),
        cache_tokenize_ms: Some(46.125),
        prepare_signer_ms: Some(47.125),
        bulkhead_wait_ms: Some(48),
        dns_ms: Some(49),
        connect_ms: Some(50),
        connection_reused: Some(true),
        request_body_read_ms: Some(52),
        request_body_first_chunk_ms: Some(53.125),
        request_body_receive_ms: Some(54.125),
        request_body_wait_ms: Some(55.125),
        request_body_process_ms: Some(56.125),
        request_body_chunk_count: Some(57),
        request_body_bytes: Some(58),
        duration_ms: 59,
        proxy_setup_ms: Some(60),
        shape_ms: Some(61),
        sign_ms: Some(62),
        upstream_ttfb_ms: Some(63),
        upstream_body_ms: Some(64),
        response_body_wait_ms: Some(65.125),
        response_body_process_ms: Some(66.125),
        response_body_downstream_poll_gap_ms: Some(67.125),
        retry_overhead_ms: Some(68.125),
        finalize_ms: Some(69),
        body_chunk_count: Some(70),
        body_bytes: Some(71),
        stream_message_start_ms: Some(72),
        stream_content_block_start_ms: Some(73),
        stream_first_content_delta_ms: Some(74),
        stream_last_content_delta_ms: Some(75),
        stream_message_stop_ms: Some(76),
        stream_last_chunk_ms: Some(77),
        stream_total_ms: Some(78),
        sse_event_count: Some(79),
        content_delta_count: Some(80),
        ping_count: Some(81),
        inter_token_avg_ms: Some(82),
        error_code: Some("roundtrip-error_code".to_owned()),
        event_id: Some("roundtrip-event_id".to_owned()),
        thinking_tokens: Some(85),
        service_tier: Some("roundtrip-service_tier".to_owned()),
        upstream_error_type: Some("roundtrip-upstream_error_type".to_owned()),
        upstream_error_message: Some("roundtrip-upstream_error_message".to_owned()),
        upstream_http_version: Some("roundtrip-upstream_http_version".to_owned()),
        upstream_request_id: Some("roundtrip-upstream_request_id".to_owned()),
        upstream_content_encoding: Some("roundtrip-upstream_content_encoding".to_owned()),
        upstream_content_length: Some(92),
        upstream_body_bytes: Some(93),
        upstream_body_end: Some("roundtrip-upstream_body_end".to_owned()),
        upstream_body_error_cause: Some("roundtrip-upstream_body_error_cause".to_owned()),
        upstream_body_error_io_kind: Some("roundtrip-upstream_body_error_io_kind".to_owned()),
        upstream_body_error_h2_reason: Some("roundtrip-upstream_body_error_h2_reason".to_owned()),
        upstream_stream_warning_type: Some("roundtrip-upstream_stream_warning_type".to_owned()),
        upstream_stream_warning_message: Some(
            "roundtrip-upstream_stream_warning_message".to_owned(),
        ),
        ..Default::default()
    };
    let result: TestResult = async {
        let cursor = fixture.storage.append_request_event(&event).await?;
        let restored = fixture
            .storage
            .get_request_event(event.event_id.as_deref().expect("fixture event id"))
            .await?
            .expect("stored relational event");
        assert_eq!(restored, event);
        let page = fixture
            .storage
            .query_request_events_between_cursors(cursor - 1, cursor, 10, &Default::default())
            .await?;
        assert_eq!(page, vec![(cursor, event.clone())]);
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
