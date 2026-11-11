use std::{
    future::Future,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use cc_lb_clock::SystemClock;
use cc_lb_storage_api::{
    CacheKeepaliveConfigSnapshot, CacheKeepaliveEnqueueState, CacheKeepaliveHitRefreshRequest,
    CacheKeepaliveReplaceRequest, CacheKeepaliveSessionRecord, CacheKeepaliveSessionStatus,
    CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason, CacheTtl, MetaStore,
    cache_keepalive_job_key,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::sync::Notify;
use uuid::Uuid;

#[test]
fn hit_reschedule_keeps_result_owned_when_real_request_supersedes() {
    run_test(|fixture| async move {
        for (suffix, encrypted_payload) in
            [("none", None), ("some", Some(b"hit-ciphertext".to_vec()))]
        {
            let key = format!("superseded-{suffix}");
            let seed = seed_request(&key);
            let seed_record = fixture.b.replace_from_real_request(&seed).await?;
            ensure!(seed_record.generation == 1);

            let hit = hit_request(&key, 1, encrypted_payload);
            fixture.gate.arm(&key);
            let task_storage = fixture.a.clone();
            let task_hit = hit.clone();
            let task =
                tokio::spawn(
                    async move { task_storage.reschedule_after_cache_hit(&task_hit).await },
                );

            fixture.gate.wait_until_entered().await?;
            let committed = fixture
                .b
                .get_cache_keepalive_session(&key)
                .await?
                .context("generation-2 snapshot")?;
            assert_hit_snapshot(&committed, &seed_record, &hit)?;

            let replacement =
                replacement_request(&key, b"", 200, "principal-b", 9, "accounting-b", 33);
            let replacement_record = fixture.b.replace_from_real_request(&replacement).await?;
            assert_replacement_record(
                &replacement_record,
                &replacement,
                3,
                &replacement.encrypted_payload,
            )?;

            fixture.gate.release();
            let task_result = tokio::time::timeout(Duration::from_secs(15), task)
                .await
                .context("cache hit task did not finish")?;
            let storage_result = task_result.context("cache hit task panicked")?;
            let owned = storage_result
                .context("cache hit storage operation failed")?
                .context("cache hit did not return its row")?;
            ensure!(
                owned == committed,
                "cache hit returned a row it did not write"
            );

            ensure!(
                !fixture
                    .b
                    .mark_cache_keepalive_enqueued(&key, owned.generation, 202)
                    .await?,
                "stale cache-hit generation must not enqueue the replacement"
            );
            ensure!(
                !fixture
                    .b
                    .update_cache_keepalive_payload(
                        &key,
                        owned.generation,
                        b"stale-ciphertext",
                        203
                    )
                    .await?,
                "stale cache-hit generation must not publish into the replacement"
            );
            ensure!(
                !fixture
                    .b
                    .mark_cache_keepalive_terminal(
                        &key,
                        owned.generation,
                        CacheKeepaliveTerminalReason::DispatchError,
                        Some("stale cache-hit terminalization"),
                        204,
                    )
                    .await?,
                "stale cache-hit generation must not terminalize the replacement"
            );
            let b_payload = b"replacement-ciphertext";
            ensure!(
                fixture
                    .b
                    .update_cache_keepalive_payload(
                        &key,
                        replacement_record.generation,
                        b_payload,
                        205,
                    )
                    .await?,
                "replacement owner must still publish its ciphertext"
            );
            let current = fixture
                .b
                .get_cache_keepalive_session(&key)
                .await?
                .context("current replacement")?;
            assert_replacement_record(&current, &replacement, 3, b_payload)?;
            ensure!(current.enqueue_state == CacheKeepaliveEnqueueState::Pending);
            ensure!(current.status == CacheKeepaliveSessionStatus::Active);
            ensure!(current.encrypted_payload == b_payload);
        }
        Ok(())
    });
}

#[test]
fn hit_reschedule_keeps_result_owned_when_cancelled() {
    run_test(|fixture| async move {
        for (suffix, encrypted_payload) in
            [("none", None), ("some", Some(b"hit-ciphertext".to_vec()))]
        {
            let key = format!("cancelled-{suffix}");
            let seed = seed_request(&key);
            let seed_record = fixture.b.replace_from_real_request(&seed).await?;
            ensure!(seed_record.generation == 1);

            let hit = hit_request(&key, 1, encrypted_payload);
            fixture.gate.arm(&key);
            let task_storage = fixture.a.clone();
            let task_hit = hit.clone();
            let task =
                tokio::spawn(
                    async move { task_storage.reschedule_after_cache_hit(&task_hit).await },
                );

            fixture.gate.wait_until_entered().await?;
            let committed = fixture
                .b
                .get_cache_keepalive_session(&key)
                .await?
                .context("generation-2 snapshot")?;
            assert_hit_snapshot(&committed, &seed_record, &hit)?;
            ensure!(
                fixture
                    .b
                    .cancel_cache_keepalive_fenced(
                        &key,
                        None,
                        committed.cache_anchor_at_unix_secs,
                        CacheKeepaliveTerminalReason::Cancelled,
                        None,
                        152,
                    )
                    .await?,
                "same-generation cancellation should succeed"
            );

            fixture.gate.release();
            let task_result = tokio::time::timeout(Duration::from_secs(15), task)
                .await
                .context("cache hit task did not finish")?;
            let storage_result = task_result.context("cache hit task panicked")?;
            let owned = storage_result
                .context("cache hit storage operation failed")?
                .context("cache hit did not return its row")?;
            ensure!(
                owned == committed,
                "cache hit returned a row it did not write"
            );
            ensure!(owned.status == CacheKeepaliveSessionStatus::Active);
            ensure!(owned.enqueue_state == CacheKeepaliveEnqueueState::Pending);

            ensure!(
                !fixture
                    .b
                    .mark_cache_keepalive_enqueued(&key, owned.generation, 153)
                    .await?,
                "cancelled generation must not be enqueued by stale caller"
            );
            ensure!(
                !fixture
                    .b
                    .mark_cache_keepalive_terminal(
                        &key,
                        owned.generation,
                        CacheKeepaliveTerminalReason::DispatchError,
                        Some("stale terminalization"),
                        154,
                    )
                    .await?,
                "cancelled generation must not be terminalized twice"
            );
            let current = fixture
                .b
                .get_cache_keepalive_session(&key)
                .await?
                .context("cancelled current row")?;
            ensure!(current.generation == 2);
            ensure!(current.status == CacheKeepaliveSessionStatus::Terminal);
            ensure!(current.terminal_reason == Some(CacheKeepaliveTerminalReason::Cancelled));
            ensure!(current.encrypted_payload.is_empty());
        }
        Ok(())
    });
}

#[test]
fn hit_reschedule_missing_stale_and_terminal_are_noops() {
    run_test(|fixture| async move {
        for (suffix, encrypted_payload) in
            [("none", None), ("some", Some(b"hit-ciphertext".to_vec()))]
        {
            let missing_key = format!("missing-{suffix}");
            ensure!(
                fixture
                    .b
                    .reschedule_after_cache_hit(&hit_request(
                        &missing_key,
                        1,
                        encrypted_payload.clone()
                    ))
                    .await?
                    .is_none(),
                "missing row must be a no-op"
            );
            ensure!(
                fixture
                    .b
                    .get_cache_keepalive_session(&missing_key)
                    .await?
                    .is_none(),
                "missing row must remain absent"
            );

            let stale_key = format!("stale-{suffix}");
            let stale_seed = seed_request(&stale_key);
            let stale_record = fixture.b.replace_from_real_request(&stale_seed).await?;
            ensure!(
                fixture
                    .b
                    .reschedule_after_cache_hit(&hit_request(
                        &stale_key,
                        stale_record.generation + 1,
                        encrypted_payload.clone(),
                    ))
                    .await?
                    .is_none(),
                "stale generation must be a no-op"
            );
            let stale_current = fixture
                .b
                .get_cache_keepalive_session(&stale_key)
                .await?
                .context("stale current row")?;
            ensure!(stale_current == stale_record);

            let terminal_key = format!("terminal-{suffix}");
            let terminal_seed = seed_request(&terminal_key);
            let terminal_record = fixture.b.replace_from_real_request(&terminal_seed).await?;
            ensure!(
                fixture
                    .b
                    .mark_cache_keepalive_terminal(
                        &terminal_key,
                        terminal_record.generation,
                        CacheKeepaliveTerminalReason::Cancelled,
                        None,
                        151,
                    )
                    .await?,
                "terminal control setup should succeed"
            );
            ensure!(
                fixture
                    .b
                    .reschedule_after_cache_hit(&hit_request(
                        &terminal_key,
                        terminal_record.generation,
                        encrypted_payload,
                    ))
                    .await?
                    .is_none(),
                "terminal row must be a no-op"
            );
            let terminal_current = fixture
                .b
                .get_cache_keepalive_session(&terminal_key)
                .await?
                .context("terminal current row")?;
            ensure!(terminal_current.generation == terminal_record.generation);
            ensure!(terminal_current.status == CacheKeepaliveSessionStatus::Terminal);
            ensure!(
                terminal_current.terminal_reason == Some(CacheKeepaliveTerminalReason::Cancelled)
            );
            ensure!(terminal_current.encrypted_payload.is_empty());
        }
        Ok(())
    });
}

fn run_test<F, Fut>(test: F)
where
    F: FnOnce(TestFixture) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
        eprintln!("skip: CI_POSTGRES_URL not set; requires isolated local/test postgres DSN");
        return;
    };
    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(async move {
            let fixture = TestFixture::create(&url).await?;
            let result = test(fixture.clone()).await;
            fixture.gate.release();
            fixture.drop_schema().await?;
            result
        })
        .expect("cache keepalive result ownership regression");
}

#[derive(Clone)]
struct TestFixture {
    schema: String,
    admin_pool: PgPool,
    a: PostgresStorage,
    b: PostgresStorage,
    gate: Arc<Gate>,
}

impl TestFixture {
    async fn create(url: &str) -> Result<Self> {
        let schema = format!("cache_keepalive_result_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;
        let options = PgConnectOptions::from_str(url)?.options([("search_path", schema.as_str())]);
        let b_pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options.clone())
            .await?;
        let b = PostgresStorage::new(b_pool, Arc::new(SystemClock));
        b.initialize().await?;

        let gate = Arc::new(Gate::default());
        let callback_gate = Arc::clone(&gate);
        let a_pool = PgPoolOptions::new()
            .max_connections(1)
            .after_release(move |connection, _metadata| {
                let callback_gate = Arc::clone(&callback_gate);
                Box::pin(async move {
                    if callback_gate.armed.load(Ordering::SeqCst) {
                        let key = callback_gate
                            .key()
                            .expect("armed callback gate must have a session key");
                        let generation = sqlx::query_scalar::<_, i64>(
                            "SELECT generation FROM cache_keepalive_sessions WHERE session_key_hash = $1",
                        )
                        .bind(key)
                        .fetch_optional(&mut *connection)
                        .await?;
                        callback_gate.pause_if_target(generation).await;
                    }
                    Ok(true)
                })
            })
            .connect_with(options)
            .await?;
        let a = PostgresStorage::new(a_pool, Arc::new(SystemClock));
        Ok(Self {
            schema,
            admin_pool,
            a,
            b,
            gate,
        })
    }

    async fn drop_schema(self) -> Result<()> {
        self.a.pool().close().await;
        self.b.pool().close().await;
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

#[derive(Default)]
struct Gate {
    armed: AtomicBool,
    entered: Notify,
    release: Notify,
    key: std::sync::Mutex<Option<String>>,
}

impl Gate {
    fn arm(&self, key: &str) {
        *self.key.lock().expect("gate key lock") = Some(key.to_owned());
        self.armed.store(true, Ordering::SeqCst);
    }

    fn key(&self) -> Option<String> {
        self.key.lock().expect("gate key lock").clone()
    }

    async fn wait_until_entered(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(15), self.entered.notified())
            .await
            .context("pool gate did not observe generation 2 after A's committed write")?;
        Ok(())
    }

    fn release(&self) {
        self.release.notify_one();
    }

    async fn pause_if_target(&self, generation: Option<i64>) {
        if generation == Some(2) && self.armed.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
    }
}

fn replacement_request(
    key: &str,
    payload: &[u8],
    now: u64,
    principal_id: &str,
    upstream_id: u128,
    accounting_key_id: &str,
    refresh_lead_time_5m_secs: u32,
) -> CacheKeepaliveReplaceRequest {
    CacheKeepaliveReplaceRequest {
        session_key_hash: key.to_owned(),
        principal_id: principal_id.to_owned(),
        accounting_key_id: Some(accounting_key_id.to_owned()),
        upstream_id: Uuid::from_u128(upstream_id),
        cycle_id: Uuid::nil(),
        cache_anchor_at_unix_secs: now,
        ttl: CacheTtl::Ttl5m,
        run_at_unix_secs: now + 270,
        expires_at_unix_secs: now + 300,
        encrypted_payload: payload.to_vec(),
        display_reason: "agent-in-turn".to_owned(),
        config_snapshot: CacheKeepaliveConfigSnapshot {
            refresh_lead_time_5m_secs,
            refresh_lead_time_1h_secs: refresh_lead_time_5m_secs * 10,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14_400,
            snapshot_max_bytes: 524_288,
        },
        now_unix_secs: now,
    }
}

fn seed_request(key: &str) -> CacheKeepaliveReplaceRequest {
    replacement_request(
        key,
        b"seed-ciphertext",
        100,
        "principal-seed",
        7,
        "accounting-seed",
        31,
    )
}

fn hit_request(
    key: &str,
    generation: u64,
    encrypted_payload: Option<Vec<u8>>,
) -> CacheKeepaliveHitRefreshRequest {
    CacheKeepaliveHitRefreshRequest {
        session_key_hash: key.to_owned(),
        generation,
        cache_anchor_at_unix_secs: 150,
        run_at_unix_secs: 420,
        expires_at_unix_secs: 450,
        encrypted_payload,
        now_unix_secs: 151,
    }
}

fn assert_hit_snapshot(
    record: &CacheKeepaliveSessionRecord,
    seed: &CacheKeepaliveSessionRecord,
    hit: &CacheKeepaliveHitRefreshRequest,
) -> Result<()> {
    ensure!(record.generation == 2);
    ensure!(record.refresh_count == 1);
    ensure!(record.principal_id == seed.principal_id);
    ensure!(record.accounting_key_id == seed.accounting_key_id);
    ensure!(record.upstream_id == seed.upstream_id);
    ensure!(record.config_snapshot == seed.config_snapshot);
    ensure!(record.first_scheduled_at_unix_secs == seed.first_scheduled_at_unix_secs);
    ensure!(record.cache_anchor_at_unix_secs == hit.cache_anchor_at_unix_secs);
    ensure!(record.run_at_unix_secs == hit.run_at_unix_secs);
    ensure!(record.expires_at_unix_secs == hit.expires_at_unix_secs);
    ensure!(record.current_job_key == cache_keepalive_job_key(&hit.session_key_hash, 2));
    ensure!(
        record.encrypted_payload
            == hit
                .encrypted_payload
                .clone()
                .unwrap_or(seed.encrypted_payload.clone())
    );
    ensure!(record.status == CacheKeepaliveSessionStatus::Active);
    ensure!(record.enqueue_state == CacheKeepaliveEnqueueState::Pending);
    Ok(())
}

fn assert_replacement_record(
    record: &CacheKeepaliveSessionRecord,
    request: &CacheKeepaliveReplaceRequest,
    generation: u64,
    expected_payload: &[u8],
) -> Result<()> {
    ensure!(record.generation == generation);
    ensure!(record.refresh_count == 0);
    ensure!(record.session_key_hash == request.session_key_hash);
    ensure!(record.principal_id == request.principal_id);
    ensure!(record.accounting_key_id == request.accounting_key_id);
    ensure!(record.upstream_id == request.upstream_id);
    ensure!(record.config_snapshot == Some(request.config_snapshot.clone()));
    ensure!(record.first_scheduled_at_unix_secs == request.cache_anchor_at_unix_secs);
    ensure!(record.cache_anchor_at_unix_secs == request.cache_anchor_at_unix_secs);
    ensure!(record.run_at_unix_secs == request.run_at_unix_secs);
    ensure!(record.expires_at_unix_secs == request.expires_at_unix_secs);
    ensure!(
        record.current_job_key == cache_keepalive_job_key(&request.session_key_hash, generation)
    );
    ensure!(record.encrypted_payload == expected_payload);
    Ok(())
}

fn quote_ident(identifier: &str) -> String {
    assert!(identifier.chars().all(|character| {
        character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
    }));
    format!("\"{identifier}\"")
}
