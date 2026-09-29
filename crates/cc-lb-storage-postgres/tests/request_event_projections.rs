use std::{str::FromStr, sync::Arc};

use anyhow::Result;
use cc_lb_clock::SystemClock;
use cc_lb_storage_api::{
    CacheKeepaliveDecisionRow, CacheKeepaliveTurnRow, CacheTtl, MetaStore, RequestEvent,
    RequestEventProjections, RequestEventStore,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgConnectOptions, postgres::PgPoolOptions};
use uuid::Uuid;

fn postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL")
        .ok()
        .or_else(|| std::env::var("PG_URL").ok())
}

#[test]
fn request_event_projections_postgres() {
    let Some(url) = postgres_url() else {
        eprintln!(
            "skip: CI_POSTGRES_URL or PG_URL not set; requires isolated local/test postgres DSN"
        );
        return;
    };

    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(async move { run_projection_cases(&url).await })
        .expect("postgres request event projections");
}

async fn run_projection_cases(url: &str) -> Result<()> {
    let fixture = Fixture::create(url).await?;
    let storage = PostgresStorage::new(fixture.pool.clone(), Arc::new(SystemClock));
    storage.initialize().await?;

    append_with_projections_commits_all_rows_when_valid(&storage).await?;
    append_with_projections_rolls_back_all_rows_when_turn_insert_fails(&storage).await?;
    append_with_projections_inserts_one_projection_set_when_base_event_exists(&storage).await?;
    append_request_event_leaves_projection_tables_empty_for_proxy_rows(&storage).await?;

    fixture.drop_schema().await
}

fn event(event_id: &str, source_ref_id: &str) -> RequestEvent {
    RequestEvent {
        ts: 1_800_000_000,
        ts_ms: Some(1_800_000_000_000),
        request_id: format!("renewal-request-{event_id}"),
        source_kind: Some("renewal".to_owned()),
        source_ref_id: Some(source_ref_id.to_owned()),
        event_id: Some(event_id.to_owned()),
        principal_id: Some("principal-a".to_owned()),
        upstream_id: Some(Uuid::from_u128(7)),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        duration_ms: 12,
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

fn projections(source_ref_id: &str) -> RequestEventProjections {
    RequestEventProjections {
        turn: Some(CacheKeepaliveTurnRow {
            source_ref_id: source_ref_id.to_owned(),
            session_key_hash: "session-hash".to_owned(),
            principal_id: "principal-a".to_owned(),
            accounting_key_id: None,
            upstream_id: Uuid::from_u128(7),
            model: "claude-sonnet-4-5".to_owned(),
            input_tokens: 100,
            output_tokens: 20,
            cache_creation_input_tokens: 10,
            cache_creation_input_tokens_5m: 10,
            cache_creation_input_tokens_1h: 0,
            cache_read_input_tokens: 70,
            cost_micros: 123_456,
            hit_miss: "hit".to_owned(),
            ts: 1_800_000_000,
        }),
        decision: CacheKeepaliveDecisionRow {
            source_ref_id: source_ref_id.to_owned(),
            principal_id: "principal-a".to_owned(),
            session_key_hash: Some("session-hash".to_owned()),
            upstream_id: Uuid::from_u128(7),
            decision: "reschedule".to_owned(),
            reason: "cache_hit".to_owned(),
            error: None,
            generation: 7,
            ttl: CacheTtl::Ttl5m,
            config_snapshot: None,
            last_message_at_ms: 1_800_000_000_000,
            ts: 1_800_000_000,
        },
    }
}

async fn row_counts(storage: &PostgresStorage) -> Result<(i64, i64, i64)> {
    let request_events = sqlx::query_scalar("SELECT COUNT(*) FROM request_events_v1")
        .fetch_one(storage.pool())
        .await?;
    let turns = sqlx::query_scalar("SELECT COUNT(*) FROM cache_keepalive_turns")
        .fetch_one(storage.pool())
        .await?;
    let decisions = sqlx::query_scalar("SELECT COUNT(*) FROM cache_keepalive_decisions")
        .fetch_one(storage.pool())
        .await?;
    Ok((request_events, turns, decisions))
}

async fn append_with_projections_commits_all_rows_when_valid(
    storage: &PostgresStorage,
) -> Result<()> {
    let cursor = storage
        .append_request_event_with_projections(
            &event("renewal-event-success", "session-hash:7"),
            &projections("session-hash:7"),
        )
        .await?;

    assert_eq!(cursor, 1);
    assert_eq!(row_counts(storage).await?, (1, 1, 1));
    Ok(())
}

async fn append_with_projections_rolls_back_all_rows_when_turn_insert_fails(
    storage: &PostgresStorage,
) -> Result<()> {
    sqlx::query(
        "CREATE FUNCTION reject_keepalive_turn() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN RAISE EXCEPTION 'forced keepalive turn failure'; END; $$",
    )
    .execute(storage.pool())
    .await?;
    sqlx::query(
        "CREATE TRIGGER reject_keepalive_turn BEFORE INSERT ON cache_keepalive_turns \
         FOR EACH ROW EXECUTE FUNCTION reject_keepalive_turn()",
    )
    .execute(storage.pool())
    .await?;

    let error = storage
        .append_request_event_with_projections(
            &event("renewal-event-rollback", "session-hash:8"),
            &projections("session-hash:8"),
        )
        .await
        .expect_err("turn insert must fail");

    assert!(error.to_string().contains("forced keepalive turn failure"));
    sqlx::query("DROP TRIGGER reject_keepalive_turn ON cache_keepalive_turns")
        .execute(storage.pool())
        .await?;
    assert_eq!(row_counts(storage).await?, (1, 1, 1));
    Ok(())
}

async fn append_with_projections_inserts_one_projection_set_when_base_event_exists(
    storage: &PostgresStorage,
) -> Result<()> {
    let event = event("renewal-event-existing-base", "session-hash:9");
    storage.append_request_event(&event).await?;

    let first_cursor = storage
        .append_request_event_with_projections(&event, &projections("session-hash:9"))
        .await?;
    let second_cursor = storage
        .append_request_event_with_projections(&event, &projections("session-hash:9"))
        .await?;

    assert_eq!(first_cursor, 3);
    assert_eq!(second_cursor, first_cursor);
    assert_eq!(row_counts(storage).await?, (2, 2, 2));
    Ok(())
}

async fn append_request_event_leaves_projection_tables_empty_for_proxy_rows(
    storage: &PostgresStorage,
) -> Result<()> {
    let mut event = event("proxy-event-single-insert", "session-hash:10");
    event.source_kind = Some("proxy".to_owned());
    event.source_ref_id = None;

    storage.append_request_event(&event).await?;

    assert_eq!(row_counts(storage).await?, (3, 2, 2));
    Ok(())
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let schema = format!("request_event_projections_{}", Uuid::new_v4().simple());
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
        identifier.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        }),
        "unsafe postgres identifier: {identifier}"
    );
    format!("\"{identifier}\"")
}
