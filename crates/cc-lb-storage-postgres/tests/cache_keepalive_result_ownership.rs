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

// Published master fdc0f9d parser/allocator, intentionally without v2 key helpers.
async fn allocate_deployed_legacy_ticket(storage: &PostgresStorage, key: &str) -> Result<u64> {
    let key = format!("keepalive_order:{key}");
    let mut tx = storage.pool().begin().await?;
    sqlx::query("INSERT INTO meta (key, value) VALUES ($1, '0:0') ON CONFLICT (key) DO NOTHING")
        .bind(&key)
        .execute(&mut *tx)
        .await?;
    let existing =
        sqlx::query_scalar::<_, String>("SELECT value FROM meta WHERE key = $1 FOR UPDATE")
            .bind(&key)
            .fetch_optional(&mut *tx)
            .await?;
    let (issued, applied) = deployed_legacy_counters(existing.as_deref());
    let next = issued.checked_add(1).context("legacy overflow")?;
    sqlx::query("UPDATE meta SET value = $2 WHERE key = $1")
        .bind(&key)
        .bind(format!("{next}:{applied}"))
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(next)
}

fn deployed_legacy_counters(value: Option<&str>) -> (u64, u64) {
    let Some(raw) = value else { return (0, 0) };
    let Some((issued_raw, applied_raw)) = raw.split_once(':') else {
        return (0, 0);
    };
    let issued = issued_raw.parse::<u64>().unwrap_or(0);
    let applied = applied_raw.parse::<u64>().unwrap_or(0);
    (issued, applied)
}

#[test]
fn mixed_version_metadata_namespace_isolates_deployed_legacy_allocator_and_gc() {
    run_test(|fixture| async move {
        // QA mirrors SQLite: deployed allocator and GC interleaved with v2 lifecycle.
        let storage = &fixture.b;
        let key = "mixed-version";
        let legacy_key = format!("keepalive_order:{key}");
        storage.put_meta_value(&legacy_key, "40:17").await?;
        ensure!(allocate_deployed_legacy_ticket(storage, key).await? == 41);
        let ticket = storage
            .allocate_cache_keepalive_completion_ticket(key, 100, 900)
            .await?;
        ensure!(ticket.sequence == 42);
        let legacy = storage.get_meta_value(&legacy_key).await?;
        ensure!(
            deployed_legacy_counters(legacy.as_deref()) == (41, 17),
            "deployed legacy reader must still see its numeric pair"
        );
        let v2_key = cc_lb_storage_api::cache_keepalive_order_meta_key(key);
        let raw = storage
            .get_meta_value(&v2_key)
            .await?
            .context("v2 metadata")?;
        let meta = cc_lb_storage_api::parse_keepalive_order_meta(Some(&raw))?;
        ensure!(meta.applied == 0, "legacy applied is not a v2 owner");
        ensure!(meta.lease.context("lease")?.epoch == ticket.epoch);
        ensure!(allocate_deployed_legacy_ticket(storage, key).await? == 42);
        ensure!(storage.get_meta_value(&v2_key).await?.as_deref() == Some(raw.as_str()));
        sqlx::query(
            "DELETE FROM meta WHERE key LIKE 'keepalive_order:%'
             AND substr(key, 17) NOT IN (SELECT session_key_hash FROM cache_keepalive_sessions)",
        )
        .execute(storage.pool())
        .await?;
        ensure!(storage.get_meta_value(&legacy_key).await?.is_none());
        ensure!(storage.get_meta_value(&v2_key).await?.as_deref() == Some(raw.as_str()));
        let mut request = seed_request(key);
        request.cache_anchor_at_unix_secs = 101;
        request.now_unix_secs = 101;
        ensure!(
            storage
                .replace_from_real_request_fenced(&request, Some(ticket))
                .await?
                .is_some()
        );
        let owner = storage
            .allocate_cache_keepalive_completion_ticket(key, 102, 1_000)
            .await?;
        ensure!(
            storage
                .begin_cache_keepalive_request_fenced(key, Some(owner), 102, 950)
                .await?
        );
        let owned_raw = storage.get_meta_value(&v2_key).await?;
        ensure!(allocate_deployed_legacy_ticket(storage, key).await? == 1);
        ensure!(storage.get_meta_value(&v2_key).await? == owned_raw);
        storage.put_meta_value(&legacy_key, "corrupt").await?;
        let later = storage
            .allocate_cache_keepalive_completion_ticket(key, 103, 200)
            .await?;
        ensure!(later.epoch == owner.epoch && later.sequence == owner.sequence + 1);
        ensure!(later.valid_until_unix_secs == 200);
        ensure!(storage.get_meta_value(&legacy_key).await?.as_deref() == Some("corrupt"));
        let updated = storage
            .get_meta_value(&v2_key)
            .await?
            .context("updated v2")?;
        ensure!(
            cc_lb_storage_api::parse_keepalive_order_meta(Some(&updated))?
                .lease
                .context("lease")?
                .protected_until_unix_secs
                == 1_000
        );
        ensure!(
            storage
                .cancel_cache_keepalive_fenced(
                    key,
                    Some(owner),
                    102,
                    CacheKeepaliveTerminalReason::Cancelled,
                    Some("request_failed"),
                    104
                )
                .await?
        );
        Ok(())
    });
}

#[test]
fn in_request_marker_lifecycle_preserves_ticket_and_logical_anchor_fences() {
    run_test(|fixture| async move {
        let storage = &fixture.b;
        let key = "request-marker";
        let original = storage
            .replace_from_real_request(&seed_request(key))
            .await?;
        storage
            .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
                session_key_hash: key.to_owned(),
                generation: original.generation,
                cache_anchor_at_unix_secs: 371,
                run_at_unix_secs: 641,
                expires_at_unix_secs: 671,
                encrypted_payload: None,
                now_unix_secs: 371,
            })
            .await?
            .context("synthetic hit accepted")?;
        let older_ticket = storage
            .allocate_cache_keepalive_completion_ticket(key, 100, 10_000_000)
            .await?;
        let begin_ticket = storage
            .allocate_cache_keepalive_completion_ticket(key, 100, 10_000_000)
            .await?;
        ensure!(
            storage
                .begin_cache_keepalive_request_fenced(key, Some(begin_ticket), 370, 900)
                .await?,
            "begin must compare against logical rather than synthetic anchor"
        );
        let marker = storage
            .get_cache_keepalive_session(key)
            .await?
            .context("marker")?;
        ensure!(marker.status == CacheKeepaliveSessionStatus::Terminal);
        ensure!(marker.terminal_reason == Some(CacheKeepaliveTerminalReason::Cancelled));
        ensure!(marker.cancel_reason.as_deref() == Some("new_request"));
        ensure!(marker.running_since_unix_secs == Some(370));
        ensure!(marker.run_at_unix_secs == 900);
        ensure!(marker.expires_at_unix_secs >= 900);
        ensure!(marker.first_scheduled_at_unix_secs == 370);
        ensure!(marker.cache_anchor_at_unix_secs == 371);
        ensure!(marker.enqueue_state == CacheKeepaliveEnqueueState::Pending);
        ensure!(marker.encrypted_payload.is_empty());
        ensure!(
            !storage
                .authorize_cache_keepalive_dispatch(key, marker.generation)
                .await?
        );
        ensure!(
            !storage
                .begin_cache_keepalive_request_fenced(key, Some(older_ticket), 380, 910)
                .await?
        );
        ensure!(
            !storage
                .cancel_cache_keepalive_fenced(
                    key,
                    Some(older_ticket),
                    380,
                    CacheKeepaliveTerminalReason::Cancelled,
                    Some("request_failed"),
                    381,
                )
                .await?
        );
        let next_ticket = storage
            .allocate_cache_keepalive_completion_ticket(key, 100, 10_000_000)
            .await?;
        ensure!(
            !storage
                .begin_cache_keepalive_request_fenced(key, Some(next_ticket), 369, 950)
                .await?
        );
        ensure!(
            storage
                .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(key))
                .await?
                .map(|raw| {
                    let meta = cc_lb_storage_api::parse_keepalive_order_meta(Some(&raw)).unwrap();
                    (meta.issued, meta.applied)
                })
                == Some((next_ticket.sequence, begin_ticket.sequence)),
            "rejected begin must roll back applied ticket"
        );
        ensure!(
            storage
                .begin_cache_keepalive_request_fenced(key, Some(next_ticket), 380, 950)
                .await?
        );
        let extended = storage
            .get_cache_keepalive_session(key)
            .await?
            .context("extended marker")?;
        ensure!(extended.running_since_unix_secs == Some(380));
        ensure!(extended.run_at_unix_secs == 950);
        ensure!(extended.generation == marker.generation);
        ensure!(
            !storage
                .cancel_cache_keepalive_fenced(
                    key,
                    Some(next_ticket),
                    379,
                    CacheKeepaliveTerminalReason::Cancelled,
                    Some("request_failed"),
                    400,
                )
                .await?
        );
        ensure!(
            storage
                .cancel_cache_keepalive_fenced(
                    key,
                    Some(next_ticket),
                    380,
                    CacheKeepaliveTerminalReason::Cancelled,
                    Some("request_failed"),
                    400,
                )
                .await?
        );
        let finalized = storage
            .get_cache_keepalive_session(key)
            .await?
            .context("final cancellation")?;
        ensure!(finalized.status == CacheKeepaliveSessionStatus::Terminal);
        ensure!(finalized.running_since_unix_secs.is_none());
        ensure!(finalized.cancel_reason.as_deref() == Some("request_failed"));
        ensure!(
            !storage
                .begin_cache_keepalive_request_fenced(key, None, 401, 960)
                .await?
        );
        Ok(())
    });
}

#[test]
fn in_request_marker_replacement_and_legacy_terminal_states() {
    run_test(|fixture| async move {
        let storage = &fixture.b;
        let key = "request-replacement";
        storage
            .replace_from_real_request(&seed_request(key))
            .await?;
        ensure!(
            storage
                .begin_cache_keepalive_request_fenced(key, None, 200, 900)
                .await?
        );
        let fresh_marker = storage
            .get_cache_keepalive_session(key)
            .await?
            .context("fresh marker")?;
        ensure!(fresh_marker.refresh_count == 0);
        ensure!(fresh_marker.expires_at_unix_secs >= 900);
        ensure!(
            !storage
                .begin_cache_keepalive_request_fenced(key, None, 199, 950)
                .await?
        );
        ensure!(
            !storage
                .cancel_cache_keepalive_fenced(
                    key,
                    None,
                    199,
                    CacheKeepaliveTerminalReason::Cancelled,
                    Some("request_failed"),
                    201,
                )
                .await?
        );
        let request =
            replacement_request(key, b"new-prefix", 201, "principal", 7, "accounting", 30);
        let active = storage
            .replace_from_real_request_fenced(&request, None)
            .await?
            .context("completion replaces marker")?;
        ensure!(active.status == CacheKeepaliveSessionStatus::Active);
        ensure!(active.running_since_unix_secs.is_none());
        ensure!(active.terminal_reason.is_none());
        ensure!(active.cancel_reason.is_none());
        ensure!(active.encrypted_payload == b"new-prefix");
        ensure!(
            storage
                .cancel_cache_keepalive_fenced(
                    key,
                    None,
                    202,
                    CacheKeepaliveTerminalReason::Cancelled,
                    Some("new_request"),
                    202,
                )
                .await?
        );
        ensure!(
            !storage
                .begin_cache_keepalive_request_fenced(key, None, 203, 950)
                .await?
        );
        ensure!(
            !storage
                .cancel_cache_keepalive_fenced(
                    key,
                    None,
                    203,
                    CacheKeepaliveTerminalReason::Cancelled,
                    Some("request_failed"),
                    203,
                )
                .await?,
            "legacy terminal new_request without start remains final"
        );
        Ok(())
    });
}

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

async fn replace_deployed_legacy_request(
    storage: &PostgresStorage,
    seed: &CacheKeepaliveReplaceRequest,
    anchor: i64,
    now: i64,
) -> Result<Option<CacheKeepaliveSessionRecord>> {
    let key = seed.session_key_hash.as_str();
    let legacy_key = format!("keepalive_order:{key}");
    let old_ticket = allocate_deployed_legacy_ticket(storage, key).await?;
    let mut tx = storage.pool().begin().await?;
    // Exact fdc0f9d replacement metadata statements, with literal legacy key.
    sqlx::query("INSERT INTO meta (key, value) VALUES ($1, '0:0') ON CONFLICT (key) DO NOTHING")
        .bind(&legacy_key)
        .execute(&mut *tx)
        .await?;
    let raw = sqlx::query_scalar::<_, String>("SELECT value FROM meta WHERE key = $1 FOR UPDATE")
        .bind(&legacy_key)
        .fetch_optional(&mut *tx)
        .await?;
    let (issued, applied) = deployed_legacy_counters(raw.as_deref());
    if old_ticket <= applied {
        tx.rollback().await?;
        return Ok(None);
    }
    sqlx::query("UPDATE meta SET value = $2 WHERE key = $1")
        .bind(&legacy_key)
        .bind(format!("{}:{}", issued.max(old_ticket), old_ticket))
        .execute(&mut *tx)
        .await?;
    // Verbatim deployed master upsert with its ticketed logical-anchor fence.
    let row = sqlx::query("INSERT INTO cache_keepalive_sessions
             (session_key_hash, principal_id, accounting_key_id, upstream_id, cycle_id, generation, refresh_count,
              first_scheduled_at, cache_anchor_at, run_at, ttl, status, enqueue_state, running_since_unix_secs,
              current_job_key, encrypted_payload, display_reason, error, config_snapshot, last_message_at_ms, terminal_reason, expires_at, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, 1, 0, $6, $7, $8, $9, 'active', 'pending', NULL, $10, $11, $12, NULL, $13, $14, NULL, $15, $16, $17)
             ON CONFLICT(session_key_hash) DO UPDATE SET
              principal_id = EXCLUDED.principal_id,
              accounting_key_id = EXCLUDED.accounting_key_id,
              upstream_id = EXCLUDED.upstream_id,
              cycle_id = EXCLUDED.cycle_id,
              generation = cache_keepalive_sessions.generation + 1,
              refresh_count = 0,
              first_scheduled_at = EXCLUDED.first_scheduled_at,
              cache_anchor_at = EXCLUDED.cache_anchor_at,
              run_at = EXCLUDED.run_at,
              ttl = EXCLUDED.ttl,
              status = 'active',
              enqueue_state = 'pending',
              running_since_unix_secs = NULL,
              current_job_key = 'cache_keepalive:' || EXCLUDED.session_key_hash || ':' || (cache_keepalive_sessions.generation + 1),
              encrypted_payload = EXCLUDED.encrypted_payload,
              display_reason = EXCLUDED.display_reason,
              error = NULL,
              config_snapshot = EXCLUDED.config_snapshot,
              last_message_at_ms = EXCLUDED.last_message_at_ms,
              terminal_reason = NULL,
              cancel_reason = NULL,
              expires_at = EXCLUDED.expires_at,
              updated_at = EXCLUDED.updated_at
             WHERE ($18 AND cache_keepalive_sessions.first_scheduled_at <= EXCLUDED.cache_anchor_at)
                OR (NOT $18 AND cache_keepalive_sessions.cache_anchor_at <= EXCLUDED.cache_anchor_at
                    AND (cache_keepalive_sessions.status != 'terminal' OR cache_keepalive_sessions.updated_at <= EXCLUDED.cache_anchor_at))
             RETURNING *")
            .bind(key).bind(&seed.principal_id).bind(&seed.accounting_key_id)
            .bind(seed.upstream_id).bind(seed.cycle_id)
            .bind(anchor).bind(anchor).bind(anchor + 270).bind("5m")
            .bind(cache_keepalive_job_key(key, 1)).bind(b"old-completion".as_slice())
            .bind(&seed.display_reason).bind(serde_json::to_string(&seed.config_snapshot)?)
            .bind(anchor * 1_000).bind(anchor + 300).bind(now).bind(now).bind(true)
            .fetch_optional(&mut *tx).await?;
    if row.is_none() {
        tx.rollback().await?;
        return Ok(None);
    }
    tx.commit().await?;
    Ok(storage.get_cache_keepalive_session(key).await?)
}

#[test]
fn deployed_master_replacement_cannot_resolve_modern_request_marker() {
    run_test(|fixture| async move {
        // QA mirrors SQLite, including inclusive deadline and post-protection resume.
        let storage = &fixture.b;
        for finish in ["replace", "cancel", "legacy-resume"] {
            let key = format!("mixed-version-shared-row-{finish}");
            storage
                .put_meta_value(&format!("keepalive_order:{key}"), "3:3")
                .await?;
            let seed = seed_request(&key);
            let active = storage.replace_from_real_request(&seed).await?;
            ensure!(active.first_scheduled_at_unix_secs == 100);
            let owner = storage
                .allocate_cache_keepalive_completion_ticket(&key, 105, 900)
                .await?;
            ensure!(owner.sequence == 4);
            ensure!(
                storage
                    .begin_cache_keepalive_request_fenced(&key, Some(owner), 105, 700)
                    .await?
            );
            let marker = storage
                .get_cache_keepalive_session(&key)
                .await?
                .context("marker")?;
            ensure!(marker.first_scheduled_at_unix_secs == 105);
            ensure!(marker.running_since_unix_secs == Some(105) && marker.run_at_unix_secs == 700);
            let raw_anchor = sqlx::query_scalar::<_, i64>(
                "SELECT first_scheduled_at FROM cache_keepalive_sessions WHERE session_key_hash = $1",
            ).bind(&key).fetch_one(storage.pool()).await?;
            ensure!(
                raw_anchor == 901,
                "owner lease, not shorter marker deadline"
            );
            let v2_key = cc_lb_storage_api::cache_keepalive_order_meta_key(&key);
            let owned_meta = storage.get_meta_value(&v2_key).await?;
            assert_deployed_active_only_mutations_reject_marker(storage, &key, marker.generation)
                .await?;
            ensure!(storage.get_cache_keepalive_session(&key).await?.as_ref() == Some(&marker));
            ensure!(storage.get_meta_value(&v2_key).await? == owned_meta);
            for anchor in [110, 900] {
                ensure!(
                    replace_deployed_legacy_request(storage, &seed, anchor, anchor + 10)
                        .await?
                        .is_none()
                );
                ensure!(storage.get_cache_keepalive_session(&key).await?.as_ref() == Some(&marker));
                ensure!(storage.get_meta_value(&v2_key).await? == owned_meta);
            }
            if finish == "replace" {
                let mut completion = seed.clone();
                completion.cache_anchor_at_unix_secs = 110;
                completion.now_unix_secs = 120;
                let row = storage
                    .replace_from_real_request_fenced(&completion, Some(owner))
                    .await?
                    .context("owner replacement")?;
                ensure!(
                    row.status == CacheKeepaliveSessionStatus::Active
                        && row.first_scheduled_at_unix_secs == 110
                );
            } else if finish == "cancel" {
                ensure!(
                    storage
                        .cancel_cache_keepalive_fenced(
                            &key,
                            Some(owner),
                            110,
                            CacheKeepaliveTerminalReason::Cancelled,
                            Some("request_failed"),
                            120
                        )
                        .await?
                );
                let row = storage
                    .get_cache_keepalive_session(&key)
                    .await?
                    .context("cancelled")?;
                ensure!(
                    row.first_scheduled_at_unix_secs == 110
                        && row.running_since_unix_secs.is_none()
                );
            } else {
                ensure!(
                    !storage
                        .cancel_cache_keepalive_fenced(
                            &key,
                            Some(owner),
                            901,
                            CacheKeepaliveTerminalReason::Cancelled,
                            Some("request_failed"),
                            901
                        )
                        .await?
                );
                ensure!(storage.get_cache_keepalive_session(&key).await?.as_ref() == Some(&marker));
                let resumed = replace_deployed_legacy_request(storage, &seed, 901, 910)
                    .await?
                    .context("later legacy replacement")?;
                ensure!(
                    resumed.status == CacheKeepaliveSessionStatus::Active
                        && resumed.first_scheduled_at_unix_secs == 901
                );
            }
            let row = storage
                .get_cache_keepalive_session(&key)
                .await?
                .context("resolved")?;
            let raw = sqlx::query_scalar::<_, i64>(
                "SELECT first_scheduled_at FROM cache_keepalive_sessions WHERE session_key_hash = $1",
            ).bind(&key).fetch_one(storage.pool()).await?;
            ensure!(
                raw as u64 == row.first_scheduled_at_unix_secs,
                "resolved row retains no synthetic fence"
            );
        }
        Ok(())
    });
}

async fn assert_deployed_active_only_mutations_reject_marker(
    storage: &PostgresStorage,
    key: &str,
    generation: u64,
) -> Result<()> {
    // Verbatim master cancellation/reschedule statements: both require active.
    let cancelled = sqlx::query("UPDATE cache_keepalive_sessions SET status = 'terminal', enqueue_state = 'pending', terminal_reason = $1, cancel_reason = $7, error = NULL, running_since_unix_secs = NULL,
             encrypted_payload = ''::bytea,
             expires_at = CASE WHEN refresh_count > 0 OR EXISTS (SELECT 1 FROM cache_keepalive_turns turn_row WHERE turn_row.principal_id = cache_keepalive_sessions.principal_id AND turn_row.session_key_hash = cache_keepalive_sessions.session_key_hash) THEN GREATEST(expires_at, $6) ELSE expires_at END,
             updated_at = $2,
             first_scheduled_at = CASE WHEN $4 THEN $5 ELSE first_scheduled_at END
             WHERE session_key_hash = $3 AND status = 'active'
               AND (CASE WHEN $4 THEN first_scheduled_at ELSE cache_anchor_at END) <= $5")
        .bind("cancelled").bind(120_i64).bind(key).bind(true).bind(110_i64)
        .bind(604_920_i64).bind("request_failed").execute(storage.pool()).await?;
    ensure!(cancelled.rows_affected() == 0);
    let refreshed = sqlx::query("UPDATE cache_keepalive_sessions SET generation = $1, refresh_count = refresh_count + 1,
                 cache_anchor_at = $2, run_at = $3, status = 'active', enqueue_state = 'pending', running_since_unix_secs = NULL,
                 current_job_key = $4, encrypted_payload = $5, terminal_reason = NULL, error = NULL, expires_at = $6, updated_at = $7
                 WHERE session_key_hash = $8 AND generation = $9 AND status = 'active'
                 RETURNING *")
        .bind((generation + 1) as i64).bind(110_i64).bind(380_i64)
        .bind(cache_keepalive_job_key(key, generation + 1)).bind(b"old-hit".as_slice())
        .bind(410_i64).bind(120_i64).bind(key).bind(generation as i64)
        .fetch_optional(storage.pool()).await?;
    ensure!(refreshed.is_none());
    Ok(())
}

#[test]
fn unticketed_marker_fence_restores_real_start_and_active_duration_anchor() {
    run_test(|fixture| async move {
        let storage = &fixture.b;
        let key = "unticketed-marker-fence";
        let seed = seed_request(key);
        let active = storage.replace_from_real_request(&seed).await?;
        let refreshed = storage
            .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
                session_key_hash: key.to_owned(),
                generation: active.generation,
                cache_anchor_at_unix_secs: 150,
                run_at_unix_secs: 420,
                expires_at_unix_secs: 450,
                encrypted_payload: None,
                now_unix_secs: 150,
            })
            .await?
            .context("active refresh")?;
        ensure!(
            refreshed.first_scheduled_at_unix_secs == 100,
            "active duration anchor is not a protection fence"
        );
        ensure!(
            storage
                .begin_cache_keepalive_request_fenced(key, None, 160, 700)
                .await?
        );
        let marker = storage
            .get_cache_keepalive_session(key)
            .await?
            .context("marker")?;
        ensure!(marker.first_scheduled_at_unix_secs == 160);
        let raw = sqlx::query_scalar::<_, i64>(
            "SELECT first_scheduled_at FROM cache_keepalive_sessions WHERE session_key_hash = $1",
        )
        .bind(key)
        .fetch_one(storage.pool())
        .await?;
        ensure!(raw == 701, "None-owner direct API uses marker deadline");
        ensure!(
            storage
                .cancel_cache_keepalive_fenced(
                    key,
                    None,
                    170,
                    CacheKeepaliveTerminalReason::Cancelled,
                    Some("request_failed"),
                    180
                )
                .await?
        );
        let row = storage
            .get_cache_keepalive_session(key)
            .await?
            .context("cancelled")?;
        ensure!(
            row.first_scheduled_at_unix_secs == 160 && row.running_since_unix_secs.is_none(),
            "unticketed finalization restores prior real start"
        );
        let raw = sqlx::query_scalar::<_, i64>(
            "SELECT first_scheduled_at FROM cache_keepalive_sessions WHERE session_key_hash = $1",
        )
        .bind(key)
        .fetch_one(storage.pool())
        .await?;
        ensure!(raw == 160);
        Ok(())
    });
}
