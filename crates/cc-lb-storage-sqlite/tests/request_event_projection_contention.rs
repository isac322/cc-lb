use std::{str::FromStr, sync::Arc, time::Duration};

use cc_lb_storage_api::{
    BackendKind, CacheKeepaliveDecisionRow, CacheKeepaliveTurnRow, MetaStore, RequestEvent,
    RequestEventProjections, RequestEventStore,
};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use tokio::time::{Instant, timeout};
use uuid::Uuid;

const BEGIN_IMMEDIATE_TIMEOUT: Duration = Duration::from_millis(250);
const PROXY_WRITE_SAMPLES: usize = 128;
const RENEWAL_PROJECTION_WRITES: usize = 64;
const PROXY_WRITE_P99_BUDGET: Duration = Duration::from_millis(100);

async fn storage() -> (
    tempfile::TempDir,
    String,
    cc_lb_storage_sqlite::SqliteStorage,
) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = sqlite_url(&temp_dir);
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open sqlite");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize sqlite");
    (temp_dir, database_url, storage)
}

fn sqlite_url(temp_dir: &tempfile::TempDir) -> String {
    format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("request-event-projection-contention.sqlite")
            .display()
    )
}

fn event(event_id: &str) -> RequestEvent {
    RequestEvent {
        ts: 1_800_000_000,
        request_id: "renewal-request".to_owned(),
        source_kind: Some("renewal".to_owned()),
        source_ref_id: Some("session-hash:7".to_owned()),
        event_id: Some(event_id.to_owned()),
        principal_id: Some("principal-a".to_owned()),
        upstream_id: Some(Uuid::from_u128(7)),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        duration_ms: 12,
        ..Default::default()
    }
}

fn projections(source_ref_id: &str) -> RequestEventProjections {
    RequestEventProjections {
        turn: CacheKeepaliveTurnRow {
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
        },
        decision: CacheKeepaliveDecisionRow {
            source_ref_id: source_ref_id.to_owned(),
            decision: "reschedule".to_owned(),
            reason: "cache_hit".to_owned(),
            generation: 7,
            ts: 1_800_000_000,
        },
    }
}

async fn row_counts(storage: &cc_lb_storage_sqlite::SqliteStorage) -> (i64, i64, i64) {
    let request_events = sqlx::query_scalar("SELECT COUNT(*) FROM request_events_v1")
        .fetch_one(storage.pool())
        .await
        .expect("count request events");
    let turns = sqlx::query_scalar("SELECT COUNT(*) FROM cache_keepalive_turns")
        .fetch_one(storage.pool())
        .await
        .expect("count keepalive turns");
    let decisions = sqlx::query_scalar("SELECT COUNT(*) FROM cache_keepalive_decisions")
        .fetch_one(storage.pool())
        .await
        .expect("count keepalive decisions");
    (request_events, turns, decisions)
}

fn p99_latency(mut latencies: Vec<Duration>) -> Duration {
    latencies.sort_unstable();
    let index = ((latencies.len() - 1) * 99).div_ceil(100);
    latencies[index]
}

#[tokio::test]
async fn begin_immediate_helper_obtains_sqlite_writer_lock() {
    let (_temp_dir, database_url, storage) = storage().await;
    let other_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::from_str(&database_url)
                .expect("sqlite url")
                .journal_mode(SqliteJournalMode::Wal)
                .synchronous(SqliteSynchronous::Normal)
                .foreign_keys(true)
                .busy_timeout(Duration::from_millis(1)),
        )
        .await
        .expect("open competing sqlite pool");

    let mut tx = storage.begin_immediate().await.expect("begin immediate");
    let competing_writer = timeout(
        BEGIN_IMMEDIATE_TIMEOUT,
        other_pool.begin_with("BEGIN IMMEDIATE"),
    )
    .await
    .expect("competing begin should return promptly");

    assert!(
        competing_writer
            .expect_err("BEGIN IMMEDIATE must hold the writer lock")
            .to_string()
            .contains("locked")
    );
    sqlx::query("SELECT 1")
        .execute(&mut *tx)
        .await
        .expect("transaction handle remains usable");
    tx.commit().await.expect("commit immediate tx");
}

#[tokio::test]
async fn proxy_writes_keep_latency_budget_during_concurrent_projection_transactions() {
    let (_temp_dir, _database_url, storage) = storage().await;
    let storage = Arc::new(storage);

    let proxy_storage = Arc::clone(&storage);
    let proxy_writes = async move {
        let mut latencies = Vec::with_capacity(PROXY_WRITE_SAMPLES);
        for index in 0..PROXY_WRITE_SAMPLES {
            let mut event = event(&format!("proxy-contention-{index}"));
            event.request_id = format!("proxy-contention-request-{index}");
            event.source_kind = Some("proxy".to_owned());
            event.source_ref_id = None;

            let started = Instant::now();
            proxy_storage
                .append_request_event(&event)
                .await
                .expect("append proxy event");
            latencies.push(started.elapsed());
        }
        latencies
    };

    let renewal_storage = Arc::clone(&storage);
    let renewal_projections = async move {
        for index in 0..RENEWAL_PROJECTION_WRITES {
            let source_ref_id = format!("session-hash:contention-{index}");
            let mut event = event(&format!("renewal-contention-{index}"));
            event.request_id = format!("renewal-contention-request-{index}");
            event.source_ref_id = Some(source_ref_id.clone());
            renewal_storage
                .append_request_event_with_projections(&event, &projections(&source_ref_id))
                .await
                .expect("append renewal projections");
        }
    };

    let (proxy_latencies, ()) = tokio::join!(proxy_writes, renewal_projections);
    let proxy_p99 = p99_latency(proxy_latencies);

    eprintln!(
        "T16 sqlite proxy write p99 under concurrent projection transactions: {proxy_p99:?} budget {PROXY_WRITE_P99_BUDGET:?}"
    );
    assert!(
        proxy_p99 <= PROXY_WRITE_P99_BUDGET,
        "proxy p99 write latency {proxy_p99:?} exceeded budget {PROXY_WRITE_P99_BUDGET:?}"
    );
    assert_eq!(
        row_counts(&storage).await,
        (
            (PROXY_WRITE_SAMPLES + RENEWAL_PROJECTION_WRITES) as i64,
            RENEWAL_PROJECTION_WRITES as i64,
            RENEWAL_PROJECTION_WRITES as i64,
        )
    );
}
