use std::sync::Arc;

use anyhow::{Context, Result};
use cc_lb_clock::TestClock;
use cc_lb_storage_api::{
    ApiKeyUsage, ApiKeyUsageBucketDelta, ApiKeyUsageBucketKey, ApiKeyUsageBucketQuery,
    ApiKeyUsageBucketStore, ApiKeyUsageFlush, ApiKeyUsageFlushResult, MetaStore,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::PgPool;
use uuid::Uuid;

const NOW: u64 = 10_000;

#[test]
fn api_key_usage_contracts() {
    let Some(url) = std::env::var("CI_POSTGRES_URL")
        .ok()
        .or_else(|| std::env::var("PG_URL").ok())
    else {
        eprintln!(
            "skip: CI_POSTGRES_URL or PG_URL not set; requires isolated local/test postgres DSN"
        );
        return;
    };
    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(async move {
            let fixture = Fixture::create(&url).await?;
            let result = run_contracts(&fixture.pool).await;
            fixture
                .drop_schema()
                .await
                .context("drop API-key usage schema")?;
            result
        })
        .expect("PostgreSQL durable API-key usage contracts");
}

async fn run_contracts(pool: &PgPool) -> Result<()> {
    let clock = Arc::new(TestClock::new_at_secs(NOW));
    let storage = PostgresStorage::new(pool.clone(), clock.clone());
    storage.initialize().await?;
    let first = Uuid::now_v7();
    let second = Uuid::now_v7();
    for (writer, requests) in [(first, 2), (second, 3)] {
        storage
            .register_api_key_usage_writer(writer, NOW + 60)
            .await?;
        let flush = usage_flush(writer, requests, NOW + 60);
        assert_eq!(
            storage.flush_api_key_usage(&flush).await?,
            ApiKeyUsageFlushResult::Applied
        );
        assert_eq!(
            storage.flush_api_key_usage(&flush).await?,
            ApiKeyUsageFlushResult::AlreadyApplied
        );
    }
    let remote = storage
        .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
            key_ids: vec!["key-a".to_owned()],
            since_unix_secs: 0,
            until_unix_secs: NOW,
            exclude_writer_epoch: first,
        })
        .await?;
    assert_eq!(remote.len(), 1);
    assert_eq!(remote[0].usage.requests, 3);

    let expired = Uuid::now_v7();
    storage
        .register_api_key_usage_writer(expired, NOW - 1)
        .await?;
    assert_eq!(
        storage
            .flush_api_key_usage(&usage_flush(expired, 9, NOW + 60))
            .await?,
        ApiKeyUsageFlushResult::LeaseLost
    );

    clock.advance_secs(180);
    let first_run = storage
        .compact_api_key_usage_buckets(60, 7 * 86_400, 1)
        .await?;
    let second_run = storage
        .compact_api_key_usage_buckets(60, 7 * 86_400, 1)
        .await?;
    assert_eq!(first_run.folded_rows + second_run.folded_rows, 2);
    assert_eq!(
        first_run.pruned_rows + second_run.pruned_rows,
        0,
        "live retention buckets must survive"
    );
    assert_eq!(
        storage
            .flush_api_key_usage(&usage_flush(first, 7, NOW + 240))
            .await?,
        ApiKeyUsageFlushResult::LeaseLost
    );
    let buckets = storage
        .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
            key_ids: vec!["key-a".to_owned()],
            since_unix_secs: 0,
            until_unix_secs: NOW + 180,
            exclude_writer_epoch: Uuid::now_v7(),
        })
        .await?;
    assert_eq!(buckets.len(), 1);
    assert_eq!(
        buckets[0].usage,
        ApiKeyUsage {
            requests: 5,
            input_tokens: 50,
            output_tokens: 100,
            cost_usd_micros: 150
        }
    );

    // Multi-delta bulk UNNEST flush inserts and accumulates multiple distinct buckets atomically.
    let bulk_writer = Uuid::now_v7();
    storage
        .register_api_key_usage_writer(bulk_writer, NOW + 300)
        .await?;
    let bulk_flush = ApiKeyUsageFlush {
        writer_epoch: bulk_writer,
        flush_id: Uuid::now_v7(),
        lease_until_unix_secs: NOW + 300,
        deltas: vec![
            ApiKeyUsageBucketDelta {
                key: ApiKeyUsageBucketKey {
                    key_id: "key-bulk-1".to_owned(),
                    bucket_width_secs: 60,
                    bucket_start_unix_secs: NOW,
                },
                usage: ApiKeyUsage {
                    requests: 4,
                    input_tokens: 40,
                    output_tokens: 80,
                    cost_usd_micros: 120,
                },
            },
            ApiKeyUsageBucketDelta {
                key: ApiKeyUsageBucketKey {
                    key_id: "key-bulk-2".to_owned(),
                    bucket_width_secs: 60,
                    bucket_start_unix_secs: NOW,
                },
                usage: ApiKeyUsage {
                    requests: 6,
                    input_tokens: 60,
                    output_tokens: 120,
                    cost_usd_micros: 180,
                },
            },
        ],
    };
    assert_eq!(
        storage.flush_api_key_usage(&bulk_flush).await?,
        ApiKeyUsageFlushResult::Applied
    );
    let bulk_buckets = storage
        .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
            key_ids: vec!["key-bulk-1".to_owned(), "key-bulk-2".to_owned()],
            since_unix_secs: NOW - 60,
            until_unix_secs: NOW + 60,
            exclude_writer_epoch: Uuid::now_v7(),
        })
        .await?;
    assert_eq!(bulk_buckets.len(), 2);

    // Duplicate keys in a single flush coalesce cleanly with additive semantics.
    let duplicate_flush = ApiKeyUsageFlush {
        writer_epoch: bulk_writer,
        flush_id: Uuid::now_v7(),
        lease_until_unix_secs: NOW + 300,
        deltas: vec![
            ApiKeyUsageBucketDelta {
                key: ApiKeyUsageBucketKey {
                    key_id: "key-dup".to_owned(),
                    bucket_width_secs: 60,
                    bucket_start_unix_secs: NOW,
                },
                usage: ApiKeyUsage {
                    requests: 2,
                    input_tokens: 25,
                    output_tokens: 50,
                    cost_usd_micros: 75,
                },
            },
            ApiKeyUsageBucketDelta {
                key: ApiKeyUsageBucketKey {
                    key_id: "key-dup".to_owned(),
                    bucket_width_secs: 60,
                    bucket_start_unix_secs: NOW,
                },
                usage: ApiKeyUsage {
                    requests: 3,
                    input_tokens: 35,
                    output_tokens: 70,
                    cost_usd_micros: 105,
                },
            },
        ],
    };
    assert_eq!(
        storage.flush_api_key_usage(&duplicate_flush).await?,
        ApiKeyUsageFlushResult::Applied
    );
    let dup_buckets = storage
        .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
            key_ids: vec!["key-dup".to_owned()],
            since_unix_secs: NOW - 60,
            until_unix_secs: NOW + 60,
            exclude_writer_epoch: Uuid::now_v7(),
        })
        .await?;
    assert_eq!(dup_buckets.len(), 1);
    assert_eq!(
        dup_buckets[0].usage,
        ApiKeyUsage {
            requests: 5,
            input_tokens: 60,
            output_tokens: 120,
            cost_usd_micros: 180,
        }
    );

    // Duplicate deltas against a negative stored balance must apply sequentially in input order
    // so intermediate prefixes like `stored (-1) + i64::MAX + 1 == i64::MAX` do not falsely fail.
    let neg_seed = ApiKeyUsageFlush {
        writer_epoch: bulk_writer,
        flush_id: Uuid::now_v7(),
        lease_until_unix_secs: NOW + 300,
        deltas: vec![ApiKeyUsageBucketDelta {
            key: ApiKeyUsageBucketKey {
                key_id: "key-dup-neg-seed".to_owned(),
                bucket_width_secs: 60,
                bucket_start_unix_secs: NOW,
            },
            usage: ApiKeyUsage {
                requests: -1,
                input_tokens: -1,
                output_tokens: -1,
                cost_usd_micros: -1,
            },
        }],
    };
    assert_eq!(
        storage.flush_api_key_usage(&neg_seed).await?,
        ApiKeyUsageFlushResult::Applied
    );
    let boundary_dup = ApiKeyUsageFlush {
        writer_epoch: bulk_writer,
        flush_id: Uuid::now_v7(),
        lease_until_unix_secs: NOW + 300,
        deltas: vec![
            ApiKeyUsageBucketDelta {
                key: ApiKeyUsageBucketKey {
                    key_id: "key-dup-neg-seed".to_owned(),
                    bucket_width_secs: 60,
                    bucket_start_unix_secs: NOW,
                },
                usage: ApiKeyUsage {
                    requests: i64::MAX,
                    input_tokens: i64::MAX,
                    output_tokens: i64::MAX,
                    cost_usd_micros: i64::MAX,
                },
            },
            ApiKeyUsageBucketDelta {
                key: ApiKeyUsageBucketKey {
                    key_id: "key-dup-neg-seed".to_owned(),
                    bucket_width_secs: 60,
                    bucket_start_unix_secs: NOW,
                },
                usage: ApiKeyUsage {
                    requests: 1,
                    input_tokens: 1,
                    output_tokens: 1,
                    cost_usd_micros: 1,
                },
            },
        ],
    };
    assert_eq!(
        storage.flush_api_key_usage(&boundary_dup).await?,
        ApiKeyUsageFlushResult::Applied
    );
    let boundary_buckets = storage
        .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
            key_ids: vec!["key-dup-neg-seed".to_owned()],
            since_unix_secs: NOW - 60,
            until_unix_secs: NOW + 60,
            exclude_writer_epoch: Uuid::now_v7(),
        })
        .await?;
    assert_eq!(boundary_buckets.len(), 1);
    assert_eq!(
        boundary_buckets[0].usage,
        ApiKeyUsage {
            requests: i64::MAX,
            input_tokens: i64::MAX,
            output_tokens: i64::MAX,
            cost_usd_micros: i64::MAX,
        }
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flush_idempotency_survives_lease_expiry_and_writer_compaction() -> Result<()> {
    let Some(url) = std::env::var("CI_POSTGRES_URL")
        .ok()
        .or_else(|| std::env::var("PG_URL").ok())
    else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    let result = async {
        let clock = std::sync::Arc::new(cc_lb_clock::TestClock::new_at_secs(NOW));
        let storage = PostgresStorage::new(fixture.pool.clone(), clock.clone());
        storage.initialize().await?;
        let first = Uuid::now_v7();
        let second = Uuid::now_v7();
        storage
            .register_api_key_usage_writer(first, NOW + 30)
            .await?;
        storage
            .register_api_key_usage_writer(second, NOW + 600)
            .await?;

        let committed_flush = usage_flush(first, 5, NOW + 30);
        assert_eq!(
            storage.flush_api_key_usage(&committed_flush).await?,
            cc_lb_storage_api::ApiKeyUsageFlushResult::Applied
        );

        clock.advance(std::time::Duration::from_secs(120));
        storage
            .compact_api_key_usage_buckets(60, 3_600, 100)
            .await?;

        assert_eq!(
            storage.flush_api_key_usage(&committed_flush).await?,
            cc_lb_storage_api::ApiKeyUsageFlushResult::AlreadyApplied
        );
        let mut rotated = committed_flush.clone();
        rotated.writer_epoch = second;
        assert_eq!(
            storage.flush_api_key_usage(&rotated).await?,
            cc_lb_storage_api::ApiKeyUsageFlushResult::AlreadyApplied
        );
        let buckets = storage
            .query_api_key_usage_buckets(&cc_lb_storage_api::ApiKeyUsageBucketQuery {
                key_ids: vec!["key-a".to_owned()],
                since_unix_secs: NOW - 120,
                until_unix_secs: NOW + 120,
                exclude_writer_epoch: Uuid::now_v7(),
            })
            .await?;
        assert_eq!(
            buckets.iter().map(|b| b.usage.requests).sum::<i64>(),
            5,
            "retried flush_id must never double-count after lease loss or compaction"
        );
        Ok::<(), anyhow::Error>(())
    }
    .await;
    fixture.drop_schema().await?;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn try_acquire_concurrency_hold_enforces_exact_cap_under_contention() -> Result<()> {
    use cc_lb_storage_api::ApiKeyConcurrencyHoldStore;
    let Some(url) = std::env::var("CI_POSTGRES_URL")
        .ok()
        .or_else(|| std::env::var("PG_URL").ok())
    else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    let result = async {
        let storage = std::sync::Arc::new(PostgresStorage::new(
            fixture.pool.clone(),
            std::sync::Arc::new(cc_lb_clock::SystemClock),
        ));
        storage.initialize().await?;
        let cap = 4_u32;
        let writer_epoch = Uuid::now_v7();
        let mut handles = Vec::new();
        for _ in 0..(cap * 2) {
            let store = std::sync::Arc::clone(&storage);
            let hold_id = Uuid::now_v7();
            handles.push(tokio::spawn(async move {
                store
                    .try_acquire_api_key_concurrency_hold(
                        hold_id,
                        "key-conc",
                        writer_epoch,
                        cap,
                        90,
                    )
                    .await
                    .map(|admitted| (hold_id, admitted))
            }));
        }
        let mut admitted_ids = Vec::new();
        let mut rejected = 0_u32;
        for handle in handles {
            let (hold_id, admitted) = handle.await??;
            if admitted {
                admitted_ids.push(hold_id);
            } else {
                rejected += 1;
            }
        }
        assert_eq!(admitted_ids.len(), cap as usize);
        assert_eq!(rejected, cap);

        // Releasing one slot allows exactly one subsequent admission.
        storage
            .delete_api_key_concurrency_hold(admitted_ids[0])
            .await?;
        assert!(
            storage
                .try_acquire_api_key_concurrency_hold(
                    Uuid::now_v7(),
                    "key-conc",
                    writer_epoch,
                    cap,
                    90,
                )
                .await?
        );

        // A stale crash-leftover row does not block a fresh hold, and DB NOW() expiry cleans it.
        storage
            .insert_api_key_concurrency_hold(&cc_lb_storage_api::ApiKeyConcurrencyHold {
                hold_id: Uuid::now_v7(),
                key_id: "key-stale".to_owned(),
                writer_epoch,
                acquired_at_unix_secs: 10,
            })
            .await?;
        assert!(
            storage
                .try_acquire_api_key_concurrency_hold(
                    Uuid::now_v7(),
                    "key-stale",
                    writer_epoch,
                    1,
                    90,
                )
                .await?
        );
        let deleted = storage
            .delete_expired_api_key_concurrency_holds(0, 90)
            .await?;
        assert_eq!(deleted, 1);
        Ok::<(), anyhow::Error>(())
    }
    .await;
    fixture.drop_schema().await?;
    result
}

fn usage_flush(writer_epoch: Uuid, requests: i64, lease_until_unix_secs: u64) -> ApiKeyUsageFlush {
    ApiKeyUsageFlush {
        writer_epoch,
        flush_id: Uuid::now_v7(),
        lease_until_unix_secs,
        deltas: vec![ApiKeyUsageBucketDelta {
            key: ApiKeyUsageBucketKey {
                key_id: "key-a".to_owned(),
                bucket_width_secs: 60,
                bucket_start_unix_secs: NOW - 60,
            },
            usage: ApiKeyUsage {
                requests,
                input_tokens: requests * 10,
                output_tokens: requests * 20,
                cost_usd_micros: requests * 30,
            },
        }],
    }
}

struct Fixture {
    isolated: crate::pg_test_support::IsolatedPg,
    pool: PgPool,
}
impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let isolated = crate::pg_test_support::IsolatedPg::create_templated(url).await?;
        let pool = match isolated.pool(4).await {
            Ok(pool) => pool,
            Err(error) => {
                let _ = isolated.drop().await;
                return Err(error);
            }
        };
        Ok(Self { isolated, pool })
    }
    async fn drop_schema(self) -> Result<()> {
        self.pool.close().await;
        self.isolated.drop().await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_heartbeat_flush_skips_dedup_table_and_compaction_drains_multiple_marker_batches()
-> Result<()> {
    let Some(url) = std::env::var("CI_POSTGRES_URL")
        .ok()
        .or_else(|| std::env::var("PG_URL").ok())
    else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    let result = async {
        let clock = std::sync::Arc::new(cc_lb_clock::TestClock::new_at_secs(NOW));
        let storage = PostgresStorage::new(fixture.pool.clone(), clock.clone());
        storage.initialize().await?;
        let writer = Uuid::now_v7();
        storage
            .register_api_key_usage_writer(writer, NOW + 30)
            .await?;

        for offset in 1..=3 {
            let heartbeat = ApiKeyUsageFlush {
                writer_epoch: writer,
                flush_id: Uuid::now_v7(),
                lease_until_unix_secs: NOW + 30 + offset,
                deltas: Vec::new(),
            };
            assert_eq!(
                storage.flush_api_key_usage(&heartbeat).await?,
                cc_lb_storage_api::ApiKeyUsageFlushResult::Applied
            );
            assert_eq!(
                storage.flush_api_key_usage(&heartbeat).await?,
                cc_lb_storage_api::ApiKeyUsageFlushResult::AlreadyApplied
            );
        }
        let flush_rows_after_heartbeats: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM api_key_usage_flushes_v1")
                .fetch_one(storage.pool())
                .await?;
        assert_eq!(flush_rows_after_heartbeats, 0);

        for _ in 0..5 {
            assert_eq!(
                storage
                    .flush_api_key_usage(&usage_flush(writer, 1, NOW + 120))
                    .await?,
                cc_lb_storage_api::ApiKeyUsageFlushResult::Applied
            );
        }
        let flush_rows_before_prune: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM api_key_usage_flushes_v1")
                .fetch_one(storage.pool())
                .await?;
        assert_eq!(flush_rows_before_prune, 5);

        clock.advance(std::time::Duration::from_secs(7_200));
        storage.compact_api_key_usage_buckets(60, 3_600, 2).await?;
        let flush_rows_after_prune: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM api_key_usage_flushes_v1")
                .fetch_one(storage.pool())
                .await?;
        assert_eq!(flush_rows_after_prune, 0);
        Ok::<(), anyhow::Error>(())
    }
    .await;
    fixture.drop_schema().await?;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_uncertain_api_key_concurrency_hold_serializes_behind_admission_advisory_lock()
-> Result<()> {
    use cc_lb_storage_api::ApiKeyConcurrencyHoldStore;

    let Some(url) = std::env::var("CI_POSTGRES_URL")
        .ok()
        .or_else(|| std::env::var("PG_URL").ok())
    else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    let result = async {
        let clock = std::sync::Arc::new(cc_lb_clock::TestClock::new_at_secs(NOW));
        let storage = std::sync::Arc::new(PostgresStorage::new(fixture.pool.clone(), clock));
        storage.initialize().await?;
        let key_id = "key-uncertain-lock";
        let hold_id = Uuid::now_v7();
        let writer_epoch = Uuid::now_v7();

        // Simulate an admission transaction that acquired the key-scoped advisory
        // lock and inserted the hold row, whose COMMIT visibility is in flight.
        let mut admission_tx = storage.pool().begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(key_id)
            .execute(&mut *admission_tx)
            .await?;
        sqlx::query(
            "INSERT INTO api_key_concurrency_holds_v1 (hold_id, key_id, writer_epoch, acquired_at_unix_secs) VALUES ($1, $2, $3, EXTRACT(EPOCH FROM NOW())::BIGINT)",
        )
        .bind(hold_id)
        .bind(key_id)
        .bind(writer_epoch)
        .execute(&mut *admission_tx)
        .await?;

        let cleanup_storage = std::sync::Arc::clone(&storage);
        let cleanup_task = tokio::spawn(async move {
            cleanup_storage
                .delete_uncertain_api_key_concurrency_hold(hold_id, key_id)
                .await
        });

        // Deterministically wait until Postgres reports the cleanup backend blocked
        // on the same advisory lock rather than guessing with a bare sleep.
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let waiting_advisory_locks: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted",
                )
                .fetch_one(storage.pool())
                .await?;
                if waiting_advisory_locks > 0 {
                    break Ok::<(), anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("cleanup task never blocked on advisory lock"))??;
        assert!(
            !cleanup_task.is_finished(),
            "uncertain hold cleanup must block on the per-key advisory lock while admission tx is open"
        );

        admission_tx.commit().await?;
        cleanup_task
            .await
            .map_err(|err| anyhow::anyhow!("cleanup task join error: {err}"))??;

        let live = storage
            .count_api_key_concurrency_holds(key_id, NOW, 90)
            .await?;
        assert_eq!(live, 0, "committed hold must be deleted once lock transfers");
        Ok::<(), anyhow::Error>(())
    }
    .await;
    fixture.drop_schema().await?;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeatable_read_session_sees_committed_holds_across_advisory_lock() -> Result<()> {
    use cc_lb_storage_api::ApiKeyConcurrencyHoldStore;
    use sqlx::postgres::PgPoolOptions;

    let Some(url) = std::env::var("CI_POSTGRES_URL")
        .ok()
        .or_else(|| std::env::var("PG_URL").ok())
    else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    let rr_pool = PgPoolOptions::new()
        .max_connections(4)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query(
                    "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                )
                .execute(conn)
                .await?;
                Ok(())
            })
        })
        .connect_with(fixture.isolated.connect_options())
        .await?;
    let result = async {
        let clock = std::sync::Arc::new(cc_lb_clock::TestClock::new_at_secs(NOW));
        let storage = std::sync::Arc::new(PostgresStorage::new(rr_pool.clone(), clock));
        storage.initialize().await?;
        let key_id = "key-rr-advisory-lock";
        let writer_epoch = Uuid::now_v7();

        // 1. Cleanup under REPEATABLE READ default must still see and delete a hold committed while waiting on the lock.
        let hold_cleanup = Uuid::now_v7();
        let mut admission_tx = fixture.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(key_id)
            .execute(&mut *admission_tx)
            .await?;
        sqlx::query(
            "INSERT INTO api_key_concurrency_holds_v1 (hold_id, key_id, writer_epoch, acquired_at_unix_secs) VALUES ($1, $2, $3, EXTRACT(EPOCH FROM NOW())::BIGINT)",
        )
        .bind(hold_cleanup)
        .bind(key_id)
        .bind(writer_epoch)
        .execute(&mut *admission_tx)
        .await?;

        let cleanup_storage = std::sync::Arc::clone(&storage);
        let cleanup_task = tokio::spawn(async move {
            cleanup_storage
                .delete_uncertain_api_key_concurrency_hold(hold_cleanup, key_id)
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let waiting: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted",
                )
                .fetch_one(&fixture.pool)
                .await?;
                if waiting > 0 {
                    break Ok::<(), anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("cleanup task never waited on advisory lock"))??;
        admission_tx.commit().await?;
        cleanup_task
            .await
            .map_err(|err| anyhow::anyhow!("cleanup join error: {err}"))??;
        assert_eq!(
            storage
                .count_api_key_concurrency_holds(key_id, NOW, 90)
                .await?,
            0,
            "READ COMMITTED override must let DELETE observe hold committed during lock wait"
        );

        // 2. Admission under REPEATABLE READ default must still see a concurrent hold committed while waiting on the lock.
        let hold_first = Uuid::now_v7();
        let hold_second = Uuid::now_v7();
        let mut tx2 = fixture.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(key_id)
            .execute(&mut *tx2)
            .await?;
        sqlx::query(
            "INSERT INTO api_key_concurrency_holds_v1 (hold_id, key_id, writer_epoch, acquired_at_unix_secs) VALUES ($1, $2, $3, EXTRACT(EPOCH FROM NOW())::BIGINT)",
        )
        .bind(hold_first)
        .bind(key_id)
        .bind(writer_epoch)
        .execute(&mut *tx2)
        .await?;

        let acquire_storage = std::sync::Arc::clone(&storage);
        let acquire_task = tokio::spawn(async move {
            acquire_storage
                .try_acquire_api_key_concurrency_hold(hold_second, key_id, writer_epoch, 1, 90)
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let waiting: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted",
                )
                .fetch_one(&fixture.pool)
                .await?;
                if waiting > 0 {
                    break Ok::<(), anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("acquire task never waited on advisory lock"))??;
        tx2.commit().await?;
        let acquired = acquire_task
            .await
            .map_err(|err| anyhow::anyhow!("acquire join error: {err}"))??;
        assert!(
            !acquired,
            "READ COMMITTED override must let COUNT(*) observe hold committed during lock wait under cap=1"
        );
        Ok::<(), anyhow::Error>(())
    }
    .await;
    rr_pool.close().await;
    fixture.drop_schema().await?;
    result
}
