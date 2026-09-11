use std::sync::Arc;

use cc_lb_storage_api::{
    CacheKeepaliveConfigSnapshot, CacheKeepaliveDecisionRow, CacheKeepaliveReplaceRequest,
    CacheKeepaliveSessionStore, CacheKeepaliveTurnRow, CacheTtl, RequestEvent,
    RequestEventProjections, RequestEventStore,
};
use cc_lb_storage_postgres::PostgresStorage;
use uuid::Uuid;

#[tokio::test]
async fn t3_postgres__renewal_storage_paths_require_live_dsn() {
    // Given
    let fixture = crate::postgres_fixture::postgres_fixture()
        .await
        .expect("create isolated postgres fixture");
    let storage = fixture.storage().clone();
    let session = storage
        .replace_from_real_request(&CacheKeepaliveReplaceRequest {
            session_key_hash: "postgres-renewal-session".to_owned(),
            principal_id: "postgres-renewal-principal".to_owned(),
            accounting_key_id: None,
            upstream_id: Uuid::from_u128(7),
            cache_anchor_at_unix_secs: 1_000,
            ttl: CacheTtl::Ttl5m,
            run_at_unix_secs: 1_010,
            expires_at_unix_secs: 1_300,
            encrypted_payload: vec![1],
            display_reason: "agent-in-turn".to_owned(),
            config_snapshot: CacheKeepaliveConfigSnapshot {
                refresh_lead_time_5m_secs: 30,
                refresh_lead_time_1h_secs: 300,
                max_refreshes_per_session: 12,
                max_total_duration_secs: 14_400,
                snapshot_max_bytes: 524_288,
            },
            now_unix_secs: 1_000,
        })
        .await
        .expect("schedule postgres renewal session");
    assert!(
        storage
            .mark_cache_keepalive_enqueued(&session.session_key_hash, session.generation, 1_001)
            .await
            .expect("mark postgres renewal enqueued")
    );

    // When
    let claims = concurrent_claims(Arc::new(storage.clone()), session.generation).await;
    storage
        .append_request_event_with_projections(&renewal_event(), &renewal_projections())
        .await
        .expect("persist postgres renewal event and projections");

    // Then
    assert_eq!(claims, 1);
    assert_eq!(row_counts(&storage).await, (1, 1, 1));
    fixture
        .teardown()
        .await
        .expect("drop isolated postgres fixture");
}

async fn concurrent_claims(storage: Arc<PostgresStorage>, generation: u64) -> u8 {
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let first = tokio::spawn(claim_after_barrier(
        Arc::clone(&storage),
        Arc::clone(&barrier),
        generation,
    ));
    let second = tokio::spawn(claim_after_barrier(storage, barrier, generation));
    let (first, second) = tokio::join!(first, second);
    u8::from(
        first
            .expect("first postgres claim task")
            .expect("first postgres claim"),
    ) + u8::from(
        second
            .expect("second postgres claim task")
            .expect("second postgres claim"),
    )
}

async fn claim_after_barrier(
    storage: Arc<PostgresStorage>,
    barrier: Arc<tokio::sync::Barrier>,
    generation: u64,
) -> cc_lb_storage_api::StorageResult<bool> {
    barrier.wait().await;
    storage
        .claim_cache_keepalive_turn("postgres-renewal-session", generation, 1_002)
        .await
}

fn renewal_event() -> RequestEvent {
    RequestEvent {
        ts: 1_003,
        request_id: "renewal:postgres-renewal-session:1".to_owned(),
        event_id: Some("renewal:postgres-renewal-session:1".to_owned()),
        source_kind: Some("renewal".to_owned()),
        source_ref_id: Some("postgres-renewal-session:1".to_owned()),
        principal_id: Some("postgres-renewal-principal".to_owned()),
        upstream_id: Some(Uuid::from_u128(7)),
        model: Some("claude-test".to_owned()),
        status: 200,
        cost_usd_micros: Some(10),
        duration_ms: 1,
        ..RequestEvent::default()
    }
}

fn renewal_projections() -> RequestEventProjections {
    RequestEventProjections {
        turn: Some(CacheKeepaliveTurnRow {
            source_ref_id: "postgres-renewal-session:1".to_owned(),
            session_key_hash: "postgres-renewal-session".to_owned(),
            principal_id: "postgres-renewal-principal".to_owned(),
            accounting_key_id: None,
            upstream_id: Uuid::from_u128(7),
            model: "claude-test".to_owned(),
            input_tokens: 0,
            output_tokens: 0,
            cache_creation_input_tokens: 0,
            cache_creation_input_tokens_5m: 0,
            cache_creation_input_tokens_1h: 0,
            cache_read_input_tokens: 10,
            cost_micros: 10,
            hit_miss: "hit".to_owned(),
            ts: 1_003,
        }),
        decision: CacheKeepaliveDecisionRow {
            source_ref_id: "postgres-renewal-session:1".to_owned(),
            principal_id: "postgres-renewal-principal".to_owned(),
            session_key_hash: Some("postgres-renewal-session".to_owned()),
            upstream_id: Uuid::from_u128(7),
            decision: "reschedule".to_owned(),
            reason: "cache_hit".to_owned(),
            error: None,
            generation: 1,
            ttl: CacheTtl::Ttl5m,
            config_snapshot: None,
            last_message_at_ms: 1_003_000,
            ts: 1_003,
        },
    }
}

async fn row_counts(storage: &PostgresStorage) -> (i64, i64, i64) {
    let events = sqlx::query_scalar("SELECT COUNT(*) FROM request_events_v1")
        .fetch_one(storage.pool())
        .await
        .expect("count postgres request events");
    let turns = sqlx::query_scalar("SELECT COUNT(*) FROM cache_keepalive_turns")
        .fetch_one(storage.pool())
        .await
        .expect("count postgres renewal turns");
    let decisions = sqlx::query_scalar("SELECT COUNT(*) FROM cache_keepalive_decisions")
        .fetch_one(storage.pool())
        .await
        .expect("count postgres renewal decisions");
    (events, turns, decisions)
}
