use std::{str::FromStr, sync::Arc, time::Duration};

use cc_lb_storage_api::{
    BackendKind, CacheKeepaliveDecisionRow, CacheKeepaliveTurnRow, CacheTtl, MetaStore,
    RequestEvent, RequestEventProjections, RequestEventStore,
};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use tokio::time::timeout;
use uuid::Uuid;

// Hang guard only; the exact invariant is the SQLITE_BUSY result below.
const WRITER_LOCK_HANG_GUARD: Duration = Duration::from_secs(30);
const PROXY_WRITE_SAMPLES: usize = 128;
const RENEWAL_PROJECTION_WRITES: usize = 64;

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

#[tokio::test]
async fn proxy_write_reports_busy_until_projection_writer_lock_is_released() {
    let (_temp_dir, database_url, storage) = storage().await;
    let proxy_pool = SqlitePoolOptions::new()
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
    let proxy_storage =
        cc_lb_storage_sqlite::SqliteStorage::new(proxy_pool, Arc::new(cc_lb_clock::SystemClock));

    let mut projection_tx = storage.begin_immediate().await.expect("begin immediate");
    let mut proxy_event = event("proxy-writer-lock");
    proxy_event.request_id = "proxy-writer-lock-request".to_owned();
    proxy_event.source_kind = Some("proxy".to_owned());
    proxy_event.source_ref_id = None;

    let blocked_write = timeout(
        WRITER_LOCK_HANG_GUARD,
        proxy_storage.append_request_event(&proxy_event),
    )
    .await
    .expect("proxy write should return its SQLite busy result promptly");
    assert!(
        blocked_write
            .expect_err("projection transaction must hold the SQLite writer lock")
            .to_string()
            .contains("locked")
    );

    sqlx::query("SELECT 1")
        .execute(&mut *projection_tx)
        .await
        .expect("projection transaction remains usable");
    projection_tx
        .commit()
        .await
        .expect("release projection writer lock");

    proxy_storage
        .append_request_event(&proxy_event)
        .await
        .expect("proxy write succeeds after projection transaction commits");
    assert_eq!(row_counts(&storage).await, (1, 0, 0));
}

#[tokio::test]
async fn proxy_and_projection_writes_are_lossless_under_contention() {
    let (_temp_dir, _database_url, storage) = storage().await;
    let storage = Arc::new(storage);

    let proxy_storage = Arc::clone(&storage);
    let proxy_writes = async move {
        for index in 0..PROXY_WRITE_SAMPLES {
            let mut event = event(&format!("proxy-contention-{index}"));
            event.request_id = format!("proxy-contention-request-{index}");
            event.source_kind = Some("proxy".to_owned());
            event.source_ref_id = None;

            proxy_storage
                .append_request_event(&event)
                .await
                .expect("append proxy event");
        }
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

    let ((), ()) = tokio::join!(proxy_writes, renewal_projections);
    assert_eq!(
        row_counts(&storage).await,
        (
            (PROXY_WRITE_SAMPLES + RENEWAL_PROJECTION_WRITES) as i64,
            RENEWAL_PROJECTION_WRITES as i64,
            RENEWAL_PROJECTION_WRITES as i64,
        )
    );
}
