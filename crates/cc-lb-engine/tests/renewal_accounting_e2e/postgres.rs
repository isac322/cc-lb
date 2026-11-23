use std::{str::FromStr, sync::Arc};

use cc_lb_storage_api::{
    CacheKeepaliveConfigSnapshot, CacheKeepaliveDecisionRow, CacheKeepaliveReplaceRequest,
    CacheKeepaliveSessionStore, CacheKeepaliveTurnRow, CacheTtl, MetaStore, RequestEvent,
    RequestEventProjections, RequestEventStore,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgConnectOptions, postgres::PgPoolOptions};
use uuid::Uuid;

#[tokio::test]
async fn postgres_renewal_storage_paths_require_live_dsn() {
    // Given
    let fixture = PostgresFixture::create(&required_postgres_url()).await;
    let storage = PostgresStorage::new(fixture.pool.clone(), Arc::new(cc_lb_clock::SystemClock));
    storage
        .initialize()
        .await
        .expect("initialize isolated postgres schema");
    let session = storage
        .replace_from_real_request_fenced(
            &CacheKeepaliveReplaceRequest {
                session_key_hash: "postgres-renewal-session".to_owned(),
                principal_id: "postgres-renewal-principal".to_owned(),
                accounting_key_id: None,
                upstream_id: Uuid::from_u128(7),
                cycle_id: Uuid::nil(),
                ttl: CacheTtl::Ttl5m,
                cache_anchor_at_unix_secs: 1_000,
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
            },
            None,
        )
        .await
        .expect("schedule postgres renewal session")
        .expect("postgres renewal session record");
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
    fixture.drop_schema().await;
}

fn required_postgres_url() -> String {
    std::env::var("CI_POSTGRES_URL")
        .ok()
        .or_else(|| std::env::var("PG_URL").ok())
        .expect(
            "renewal_accounting_e2e requires CI_POSTGRES_URL or PG_URL with --features postgres",
        )
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
        ts_ms: Some(1_003_000),
        request_id:
            "renewal:keepalive-v2:postgres-renewal-session:00000000-0000-0000-0000-000000000000:1"
                .to_owned(),
        event_id: Some(
            "renewal:keepalive-v2:postgres-renewal-session:00000000-0000-0000-0000-000000000000:1"
                .to_owned(),
        ),
        source_kind: Some("renewal".to_owned()),
        source_ref_id: Some(
            "keepalive-v2:postgres-renewal-session:00000000-0000-0000-0000-000000000000:1"
                .to_owned(),
        ),
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
            source_ref_id:
                "keepalive-v2:postgres-renewal-session:00000000-0000-0000-0000-000000000000:1"
                    .to_owned(),
            session_key_hash: "postgres-renewal-session".to_owned(),
            principal_id: "postgres-renewal-principal".to_owned(),
            accounting_key_id: None,
            upstream_id: Uuid::from_u128(7),
            cycle_id: None,
            model: "claude-test".to_owned(),
            input_tokens: 0,
            output_tokens: 0,
            cache_creation_input_tokens: 0,
            cache_creation_input_tokens_5m: 0,
            cache_creation_input_tokens_1h: 0,
            cache_read_input_tokens: 10,
            cost_micros: Some(10),
            hit_miss: "hit".to_owned(),
            ts: 1_003,
        }),
        decision: CacheKeepaliveDecisionRow {
            source_ref_id:
                "keepalive-v2:postgres-renewal-session:00000000-0000-0000-0000-000000000000:1"
                    .to_owned(),
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
        evidence: Vec::new(),
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

enum PostgresFixtureBackend {
    Database { db: String, admin_url: String },
    Schema { schema: String, admin_pool: PgPool },
}

struct PostgresFixture {
    backend: PostgresFixtureBackend,
    pool: PgPool,
}

impl PostgresFixture {
    async fn create(url: &str) -> Self {
        if let Ok(template_base) = std::env::var("CC_LB_PG_APP_TEMPLATE")
            && !template_base.trim().is_empty()
        {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash as _, Hasher as _};

            let template_base = template_base.trim();
            let db = format!("renewal_accounting_e2e_{}", Uuid::new_v4().simple());
            let pool_size = std::env::var("CC_LB_PG_APP_TEMPLATE_POOL_SIZE")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(1)
                .max(1);
            let slot = {
                let mut hasher = DefaultHasher::new();
                db.hash(&mut hasher);
                (hasher.finish() as usize) % pool_size
            };
            let template_name = if pool_size > 1 {
                format!("{template_base}_{slot}")
            } else {
                template_base.to_owned()
            };
            let base_opts = PgConnectOptions::from_str(url).expect("parse postgres URL");
            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_opts.clone().database("postgres"))
                .await
                .expect("connect postgres admin pool");
            let lock_key = 0x4343_4C42_0000_0000_i64 + i64::try_from(slot).unwrap_or(0);
            let mut conn = admin_pool.acquire().await.expect("acquire postgres admin");
            sqlx::query("SELECT pg_advisory_lock($1)")
                .bind(lock_key)
                .execute(&mut *conn)
                .await
                .expect("lock template slot");
            let wal_sql = format!(
                "CREATE DATABASE {} TEMPLATE {} STRATEGY = WAL_LOG",
                quote_ident(&db),
                quote_ident(&template_name)
            );
            let created = match sqlx::query(AssertSqlSafe(wal_sql))
                .execute(&mut *conn)
                .await
            {
                Ok(_) => Ok(()),
                Err(_) => sqlx::query(AssertSqlSafe(format!(
                    "CREATE DATABASE {} TEMPLATE {}",
                    quote_ident(&db),
                    quote_ident(&template_name)
                )))
                .execute(&mut *conn)
                .await
                .map(|_| ()),
            };
            let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                .bind(lock_key)
                .execute(&mut *conn)
                .await;
            drop(conn);
            admin_pool.close().await;
            created.expect("create templated renewal postgres database");

            let pool = PgPoolOptions::new()
                .max_connections(4)
                .connect_with(base_opts.database(&db))
                .await
                .expect("connect isolated postgres pool");
            return Self {
                backend: PostgresFixtureBackend::Database {
                    db,
                    admin_url: url.to_owned(),
                },
                pool,
            };
        }

        let schema = format!("renewal_accounting_e2e_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await
            .expect("connect postgres admin pool");
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await
        .expect("create isolated postgres schema");
        let options = PgConnectOptions::from_str(url)
            .expect("parse postgres URL")
            .options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .expect("connect isolated postgres pool");
        Self {
            backend: PostgresFixtureBackend::Schema { schema, admin_pool },
            pool,
        }
    }

    async fn drop_schema(self) {
        self.pool.close().await;
        match self.backend {
            PostgresFixtureBackend::Database { db, admin_url } => {
                let admin_opts = PgConnectOptions::from_str(&admin_url)
                    .expect("parse postgres URL")
                    .database("postgres");
                let admin_pool = PgPoolOptions::new()
                    .max_connections(1)
                    .connect_with(admin_opts)
                    .await
                    .expect("connect postgres admin pool");
                sqlx::query(AssertSqlSafe(format!(
                    "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                    quote_ident(&db)
                )))
                .execute(&admin_pool)
                .await
                .expect("drop isolated postgres database");
                admin_pool.close().await;
            }
            PostgresFixtureBackend::Schema { schema, admin_pool } => {
                sqlx::query(AssertSqlSafe(format!(
                    "DROP SCHEMA IF EXISTS {} CASCADE",
                    quote_ident(&schema)
                )))
                .execute(&admin_pool)
                .await
                .expect("drop isolated postgres schema");
                admin_pool.close().await;
            }
        }
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
