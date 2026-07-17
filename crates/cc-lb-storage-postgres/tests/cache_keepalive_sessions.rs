use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use cc_lb_clock::SystemClock;
use cc_lb_storage_api::{
    BackendKind, CacheKeepaliveEnqueueState, CacheKeepaliveHitRefreshRequest,
    CacheKeepaliveReplaceRequest, CacheKeepaliveSessionStatus, CacheKeepaliveSessionStore,
    CacheKeepaliveTerminalReason, CacheTtl, MetaStore, cache_keepalive_job_key,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgConnectOptions, postgres::PgPoolOptions};
use uuid::Uuid;

fn postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL").ok()
}

#[test]
fn cache_keepalive_postgres_conformance() {
    let Some(url) = postgres_url() else {
        eprintln!("skip: CI_POSTGRES_URL not set; requires isolated local/test postgres DSN");
        return;
    };

    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(async move { run_conformance(&url).await })
        .expect("cache keepalive postgres conformance");
}

async fn run_conformance(url: &str) -> Result<()> {
    let fixture = Fixture::create(url).await?;
    let storage = PostgresStorage::new(fixture.pool.clone(), Arc::new(SystemClock));
    storage.initialize(BackendKind::Postgres).await?;

    replace_from_real_request_bumps_generation_and_resets_counters(&storage).await?;
    replace_roundtrips_optional_accounting_key_id(&storage).await?;
    update_payload_only_mutates_matching_pending_generation(&storage).await?;
    cache_hit_reschedule_preserves_duration_anchor_and_rejects_stale_generation(&storage).await?;
    conditional_enqueue_terminal_and_purge_are_generation_safe(&storage).await?;
    mark_latest_terminal_only_mutates_active_latest_session(&storage).await?;
    purge_stale_pending_keeps_enqueued_work(&storage).await?;
    concurrent_replace_from_real_request_bumps_each_generation(&storage).await?;
    concurrent_hit_reschedule_allows_only_one_generation_cas(&storage).await?;
    concurrent_claim_allows_exactly_one_enqueued_generation_winner(&storage).await?;

    fixture.drop_schema().await
}

async fn replace_from_real_request_bumps_generation_and_resets_counters(
    storage: &PostgresStorage,
) -> Result<()> {
    let first = storage
        .replace_from_real_request(&replace_request("replace-session", b"ciphertext-one", 100))
        .await?;
    let second = storage
        .replace_from_real_request(&replace_request("replace-session", b"ciphertext-two", 200))
        .await?;

    assert_eq!(first.generation, 1);
    assert_eq!(second.generation, 2);
    assert_eq!(second.refresh_count, 0);
    assert_eq!(second.first_scheduled_at_unix_secs, 200);
    assert_eq!(second.cache_anchor_at_unix_secs, 200);
    assert_eq!(second.run_at_unix_secs, 470);
    assert_eq!(
        second.current_job_key,
        cache_keepalive_job_key("replace-session", 2)
    );
    assert_eq!(second.encrypted_payload, b"ciphertext-two");
    Ok(())
}

async fn replace_roundtrips_optional_accounting_key_id(storage: &PostgresStorage) -> Result<()> {
    let with_key = CacheKeepaliveReplaceRequest {
        accounting_key_id: Some("key-live-123".to_owned()),
        ..replace_request("keyed-session", b"ciphertext-with-key", 100)
    };
    let without_key = replace_request("unkeyed-session", b"ciphertext-without-key", 110);

    let with_key = storage.replace_from_real_request(&with_key).await?;
    let without_key = storage.replace_from_real_request(&without_key).await?;

    assert_eq!(with_key.accounting_key_id.as_deref(), Some("key-live-123"));
    assert_eq!(without_key.accounting_key_id, None);
    Ok(())
}

async fn update_payload_only_mutates_matching_pending_generation(
    storage: &PostgresStorage,
) -> Result<()> {
    let record = storage
        .replace_from_real_request(&replace_request(
            "payload-session",
            b"pending-placeholder",
            100,
        ))
        .await?;

    assert!(
        !storage
            .update_cache_keepalive_payload("payload-session", record.generation + 1, b"stale", 101)
            .await?
    );
    assert!(
        storage
            .update_cache_keepalive_payload(
                "payload-session",
                record.generation,
                b"ciphertext",
                102
            )
            .await?
    );

    let updated = storage
        .get_cache_keepalive_session("payload-session")
        .await?
        .expect("row exists");
    assert_eq!(updated.encrypted_payload, b"ciphertext");
    assert_eq!(updated.enqueue_state, CacheKeepaliveEnqueueState::Pending);

    assert!(
        storage
            .mark_cache_keepalive_enqueued("payload-session", record.generation, 103)
            .await?
    );
    assert!(
        !storage
            .update_cache_keepalive_payload("payload-session", record.generation, b"late", 104)
            .await?
    );
    Ok(())
}

async fn cache_hit_reschedule_preserves_duration_anchor_and_rejects_stale_generation(
    storage: &PostgresStorage,
) -> Result<()> {
    let original = storage
        .replace_from_real_request(&replace_request("hit-session", b"ciphertext-one", 100))
        .await?;

    let stale = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: "hit-session".to_owned(),
            generation: original.generation + 1,
            cache_anchor_at_unix_secs: 150,
            run_at_unix_secs: 420,
            expires_at_unix_secs: 450,
            encrypted_payload: b"stale".to_vec(),
            now_unix_secs: 151,
        })
        .await?;
    assert!(stale.is_none());

    let updated = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: "hit-session".to_owned(),
            generation: original.generation,
            cache_anchor_at_unix_secs: 150,
            run_at_unix_secs: 420,
            expires_at_unix_secs: 450,
            encrypted_payload: b"ciphertext-hit".to_vec(),
            now_unix_secs: 151,
        })
        .await?
        .expect("row updated");

    assert_eq!(updated.generation, 2);
    assert_eq!(updated.refresh_count, 1);
    assert_eq!(updated.first_scheduled_at_unix_secs, 100);
    assert_eq!(updated.cache_anchor_at_unix_secs, 150);
    assert_eq!(updated.run_at_unix_secs, 420);
    assert_eq!(updated.encrypted_payload, b"ciphertext-hit");
    Ok(())
}

async fn conditional_enqueue_terminal_and_purge_are_generation_safe(
    storage: &PostgresStorage,
) -> Result<()> {
    let record = storage
        .replace_from_real_request(&replace_request("terminal-session", b"ciphertext", 50))
        .await?;

    assert!(
        storage
            .mark_cache_keepalive_enqueued("terminal-session", record.generation, 101)
            .await?
    );
    assert!(
        !storage
            .mark_cache_keepalive_terminal(
                "terminal-session",
                record.generation + 1,
                CacheKeepaliveTerminalReason::CacheMiss,
                102,
            )
            .await?
    );
    assert!(
        storage
            .mark_cache_keepalive_terminal(
                "terminal-session",
                record.generation,
                CacheKeepaliveTerminalReason::CacheMiss,
                102,
            )
            .await?
    );
    let check = storage
        .check_cache_keepalive_generation("terminal-session")
        .await?
        .expect("row exists");
    assert_eq!(check.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(check.enqueue_state, CacheKeepaliveEnqueueState::Enqueued);

    let removed = storage.purge_cache_keepalive_expired(351).await?;
    assert_eq!(removed, 1);
    assert!(
        storage
            .get_cache_keepalive_session("terminal-session")
            .await?
            .is_none()
    );
    Ok(())
}

async fn mark_latest_terminal_only_mutates_active_latest_session(
    storage: &PostgresStorage,
) -> Result<()> {
    let session_key_hash = "latest-terminal";
    let first = storage
        .replace_from_real_request(&replace_request(session_key_hash, b"ciphertext-one", 100))
        .await?;
    let second = storage
        .replace_from_real_request(&replace_request(session_key_hash, b"ciphertext-two", 110))
        .await?;

    assert_eq!(first.generation, 1);
    assert_eq!(second.generation, 2);
    assert!(
        storage
            .mark_latest_cache_keepalive_terminal(
                session_key_hash,
                CacheKeepaliveTerminalReason::Cancelled,
                120,
            )
            .await?
    );
    assert!(
        !storage
            .mark_latest_cache_keepalive_terminal(
                session_key_hash,
                CacheKeepaliveTerminalReason::CacheMiss,
                121,
            )
            .await?
    );

    let record = storage
        .get_cache_keepalive_session(session_key_hash)
        .await?
        .expect("row exists");
    assert_eq!(record.generation, 2);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Cancelled)
    );
    Ok(())
}

async fn purge_stale_pending_keeps_enqueued_work(storage: &PostgresStorage) -> Result<()> {
    let pending = storage
        .replace_from_real_request(&replace_request("stale-pending", b"ciphertext", 100))
        .await?;
    let enqueued = storage
        .replace_from_real_request(&replace_request("old-enqueued", b"ciphertext", 110))
        .await?;
    assert!(
        storage
            .mark_cache_keepalive_enqueued("old-enqueued", enqueued.generation, 111)
            .await?
    );

    let removed = storage.purge_cache_keepalive_stale_pending(112).await?;

    assert_eq!(pending.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert_eq!(removed, 1);
    assert!(
        storage
            .get_cache_keepalive_session("stale-pending")
            .await?
            .is_none()
    );
    assert!(
        storage
            .get_cache_keepalive_session("old-enqueued")
            .await?
            .is_some()
    );
    Ok(())
}

async fn concurrent_replace_from_real_request_bumps_each_generation(
    storage: &PostgresStorage,
) -> Result<()> {
    let session_key_hash = "concurrent-replace";
    let original = storage
        .replace_from_real_request(&replace_request(session_key_hash, b"init", 100))
        .await?;
    assert_eq!(original.generation, 1);
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let first = tokio::spawn({
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .replace_from_real_request(&replace_request(session_key_hash, b"first", 110))
                .await
        }
    });
    let second = tokio::spawn({
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .replace_from_real_request(&replace_request(session_key_hash, b"second", 120))
                .await
        }
    });

    let (first, second) = tokio::try_join!(first, second)?;
    let mut generations = vec![first?.generation, second?.generation];
    generations.sort_unstable();
    assert_eq!(generations, [2, 3]);
    let final_record = storage
        .get_cache_keepalive_session(session_key_hash)
        .await?
        .expect("row exists");
    assert_eq!(final_record.generation, 3);
    assert_eq!(
        final_record.current_job_key,
        cache_keepalive_job_key(session_key_hash, 3)
    );
    Ok(())
}

async fn concurrent_hit_reschedule_allows_only_one_generation_cas(
    storage: &PostgresStorage,
) -> Result<()> {
    let session_key_hash = "concurrent-hit-cas";
    let original = storage
        .replace_from_real_request(&replace_request(session_key_hash, b"init", 100))
        .await?;
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let first = tokio::spawn({
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .reschedule_after_cache_hit(&hit_request(
                    session_key_hash,
                    original.generation,
                    b"first",
                ))
                .await
        }
    });
    let second = tokio::spawn({
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .reschedule_after_cache_hit(&hit_request(
                    session_key_hash,
                    original.generation,
                    b"second",
                ))
                .await
        }
    });

    let (first, second) = tokio::try_join!(first, second)?;
    let first = first?;
    let second = second?;
    assert_eq!(first.is_some() as u8 + second.is_some() as u8, 1);
    let winner = first.or(second).expect("one CAS succeeds");
    assert_eq!(winner.generation, 2);
    let final_record = storage
        .get_cache_keepalive_session(session_key_hash)
        .await?
        .expect("row exists");
    assert_eq!(final_record.generation, 2);
    assert_eq!(final_record.refresh_count, 1);
    Ok(())
}

async fn concurrent_claim_allows_exactly_one_enqueued_generation_winner(
    storage: &PostgresStorage,
) -> Result<()> {
    let session_key_hash = "concurrent-claim-cas";
    let record = storage
        .replace_from_real_request(&replace_request(session_key_hash, b"init", 100))
        .await?;
    assert!(
        storage
            .mark_cache_keepalive_enqueued(session_key_hash, record.generation, 101)
            .await?
    );
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let first = tokio::spawn({
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .claim_cache_keepalive_turn(session_key_hash, record.generation, 200)
                .await
        }
    });
    let second = tokio::spawn({
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .claim_cache_keepalive_turn(session_key_hash, record.generation, 200)
                .await
        }
    });

    let (first, second) = tokio::try_join!(first, second)?;
    let first = first?;
    let second = second?;
    assert_eq!(u8::from(first) + u8::from(second), 1);

    let claimed = storage
        .get_cache_keepalive_session(session_key_hash)
        .await?
        .expect("session exists");
    assert_eq!(claimed.enqueue_state, CacheKeepaliveEnqueueState::Running);
    assert_eq!(claimed.running_since_unix_secs, Some(200));
    Ok(())
}

fn replace_request(
    session_key_hash: &str,
    payload: &[u8],
    now: u64,
) -> CacheKeepaliveReplaceRequest {
    CacheKeepaliveReplaceRequest {
        session_key_hash: session_key_hash.to_owned(),
        principal_id: "principal".to_owned(),
        upstream_id: Uuid::from_u128(7),
        cache_anchor_at_unix_secs: now,
        ttl: CacheTtl::Ttl5m,
        run_at_unix_secs: now + 270,
        expires_at_unix_secs: now + 300,
        encrypted_payload: payload.to_vec(),
        accounting_key_id: None,
        now_unix_secs: now,
    }
}

fn hit_request(
    session_key_hash: &str,
    generation: u64,
    payload: &[u8],
) -> CacheKeepaliveHitRefreshRequest {
    CacheKeepaliveHitRefreshRequest {
        session_key_hash: session_key_hash.to_owned(),
        generation,
        cache_anchor_at_unix_secs: 150,
        run_at_unix_secs: 420,
        expires_at_unix_secs: 450,
        encrypted_payload: payload.to_vec(),
        now_unix_secs: 151,
    }
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let schema = format!("cache_keepalive_{}", Uuid::new_v4().simple());
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
