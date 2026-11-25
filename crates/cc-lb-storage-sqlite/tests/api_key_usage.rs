use std::sync::Arc;

use cc_lb_clock::TestClock;
use cc_lb_storage_api::{
    ApiKeyUsage, ApiKeyUsageBucketDelta, ApiKeyUsageBucketKey, ApiKeyUsageBucketQuery,
    ApiKeyUsageBucketStore, ApiKeyUsageFlush, ApiKeyUsageFlushResult,
};
use uuid::Uuid;

const NOW: u64 = 10_000;

async fn storage() -> (
    tempfile::TempDir,
    Arc<TestClock>,
    cc_lb_storage_sqlite::SqliteStorage,
) {
    let directory = tempfile::tempdir().expect("tempdir");
    let clock = Arc::new(TestClock::new_at_secs(NOW));
    let storage = super::common::templated_sqlite_storage(
        &directory.path().join("usage.sqlite"),
        clock.clone(),
    )
    .await
    .expect("migrate sqlite");
    (directory, clock, storage)
}

fn flush(writer_epoch: Uuid, requests: i64, lease_until_unix_secs: u64) -> ApiKeyUsageFlush {
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

#[tokio::test]
async fn api_key_usage_flush_is_idempotent_epoch_isolated_and_lease_guarded() {
    let (_directory, _clock, storage) = storage().await;
    let first = Uuid::now_v7();
    let second = Uuid::now_v7();
    for (writer, requests) in [(first, 2), (second, 3)] {
        storage
            .register_api_key_usage_writer(writer, NOW + 60)
            .await
            .expect("register writer");
        let flush = flush(writer, requests, NOW + 60);
        assert_eq!(
            storage
                .flush_api_key_usage(&flush)
                .await
                .expect("first flush"),
            ApiKeyUsageFlushResult::Applied
        );
        assert_eq!(
            storage
                .flush_api_key_usage(&flush)
                .await
                .expect("retry flush"),
            ApiKeyUsageFlushResult::AlreadyApplied
        );
    }
    let remote = storage
        .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
            key_ids: vec!["key-a".to_owned()],
            since_unix_secs: NOW - 120,
            until_unix_secs: NOW,
            exclude_writer_epoch: first,
        })
        .await
        .expect("query remote usage");
    assert_eq!(remote.len(), 1);
    assert_eq!(remote[0].usage.requests, 3);

    let expired = Uuid::now_v7();
    storage
        .register_api_key_usage_writer(expired, NOW - 1)
        .await
        .expect("register expired writer");
    assert_eq!(
        storage
            .flush_api_key_usage(&flush(expired, 9, NOW + 60))
            .await
            .expect("reject expired writer"),
        ApiKeyUsageFlushResult::LeaseLost
    );
}

#[tokio::test]
async fn api_key_usage_compaction_adds_repeated_folds_into_retired_bucket() {
    let (_directory, clock, storage) = storage().await;
    let first = Uuid::now_v7();
    let second = Uuid::now_v7();
    for (writer, requests) in [(first, 2), (second, 3)] {
        storage
            .register_api_key_usage_writer(writer, NOW + 60)
            .await
            .expect("register writer");
        storage
            .flush_api_key_usage(&flush(writer, requests, NOW + 60))
            .await
            .expect("flush writer");
    }
    clock.advance_secs(180);
    let first_run = storage
        .compact_api_key_usage_buckets(60, 7 * 86_400, 1)
        .await
        .expect("first fold");
    let second_run = storage
        .compact_api_key_usage_buckets(60, 7 * 86_400, 1)
        .await
        .expect("second fold");
    assert_eq!(first_run.folded_rows + second_run.folded_rows, 2);
    assert_eq!(
        first_run.pruned_rows + second_run.pruned_rows,
        0,
        "buckets inside retention must survive"
    );
    assert_eq!(
        storage
            .flush_api_key_usage(&flush(first, 7, NOW + 240))
            .await
            .expect("reject deleted writer"),
        ApiKeyUsageFlushResult::LeaseLost
    );
    let buckets = storage
        .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
            key_ids: vec!["key-a".to_owned()],
            since_unix_secs: 0,
            until_unix_secs: NOW + 180,
            exclude_writer_epoch: Uuid::now_v7(),
        })
        .await
        .expect("query folded usage");
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
}

#[tokio::test]
async fn api_key_usage_flush_idempotency_survives_lease_expiry_and_writer_compaction() {
    let (_dir, clock, storage) = storage().await;
    let first = Uuid::now_v7();
    let second = Uuid::now_v7();
    storage
        .register_api_key_usage_writer(first, NOW + 30)
        .await
        .expect("register first");
    storage
        .register_api_key_usage_writer(second, NOW + 600)
        .await
        .expect("register second");

    let committed_flush = flush(first, 5, NOW + 30);
    assert_eq!(
        storage
            .flush_api_key_usage(&committed_flush)
            .await
            .expect("initial apply"),
        ApiKeyUsageFlushResult::Applied
    );

    clock.advance(std::time::Duration::from_secs(120));
    storage
        .compact_api_key_usage_buckets(60, 3_600, 100)
        .await
        .expect("compact first");

    assert_eq!(
        storage
            .flush_api_key_usage(&committed_flush)
            .await
            .expect("retry after compaction with old epoch"),
        ApiKeyUsageFlushResult::AlreadyApplied
    );
    let mut rotated_retry = committed_flush.clone();
    rotated_retry.writer_epoch = second;
    assert_eq!(
        storage
            .flush_api_key_usage(&rotated_retry)
            .await
            .expect("retry after compaction with rotated epoch"),
        ApiKeyUsageFlushResult::AlreadyApplied
    );
    let rows = storage
        .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
            key_ids: vec!["key-a".to_owned()],
            since_unix_secs: NOW - 120,
            until_unix_secs: NOW + 120,
            exclude_writer_epoch: Uuid::now_v7(),
        })
        .await
        .expect("query buckets");
    assert_eq!(
        rows.iter().map(|b| b.usage.requests).sum::<i64>(),
        5,
        "retried flush_id must never double-count after lease loss or compaction"
    );
}

#[tokio::test]
async fn empty_heartbeat_flush_skips_dedup_table_and_compaction_drains_multiple_marker_batches() {
    let (_dir, clock, storage) = storage().await;
    let writer = Uuid::now_v7();
    storage
        .register_api_key_usage_writer(writer, NOW + 30)
        .await
        .expect("register writer");

    for offset in 1..=3 {
        let heartbeat = ApiKeyUsageFlush {
            writer_epoch: writer,
            flush_id: Uuid::now_v7(),
            lease_until_unix_secs: NOW + 30 + offset,
            deltas: Vec::new(),
        };
        assert_eq!(
            storage
                .flush_api_key_usage(&heartbeat)
                .await
                .expect("empty heartbeat applies"),
            ApiKeyUsageFlushResult::Applied
        );
        assert_eq!(
            storage
                .flush_api_key_usage(&heartbeat)
                .await
                .expect("repeated latest heartbeat is idempotent"),
            ApiKeyUsageFlushResult::AlreadyApplied
        );
    }
    let flush_rows_after_heartbeats: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM api_key_usage_flushes_v1")
            .fetch_one(storage.pool())
            .await
            .expect("count flush markers");
    assert_eq!(
        flush_rows_after_heartbeats, 0,
        "empty heartbeats must not insert deduplication rows"
    );

    // Insert 5 nonempty flushes and compact with batch_size=2 to verify bounded multi-batch pruning.
    for _ in 0..5 {
        assert_eq!(
            storage
                .flush_api_key_usage(&flush(writer, 1, NOW + 120))
                .await
                .expect("nonempty flush applies"),
            ApiKeyUsageFlushResult::Applied
        );
    }
    let flush_rows_before_prune: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM api_key_usage_flushes_v1")
            .fetch_one(storage.pool())
            .await
            .expect("count nonempty markers");
    assert_eq!(flush_rows_before_prune, 5);

    clock.advance(std::time::Duration::from_secs(7_200));
    storage
        .compact_api_key_usage_buckets(60, 3_600, 2)
        .await
        .expect("compact prunes all expired markers across batches");
    let flush_rows_after_prune: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM api_key_usage_flushes_v1")
            .fetch_one(storage.pool())
            .await
            .expect("count markers after prune");
    assert_eq!(flush_rows_after_prune, 0);
}
