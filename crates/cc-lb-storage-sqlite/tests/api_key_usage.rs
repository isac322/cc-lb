use std::sync::Arc;

use cc_lb_clock::TestClock;
use cc_lb_storage_api::{
    ApiKeyUsage, ApiKeyUsageBucketDelta, ApiKeyUsageBucketKey, ApiKeyUsageBucketQuery,
    ApiKeyUsageBucketStore, ApiKeyUsageFlush, ApiKeyUsageFlushResult, BackendKind, MetaStore,
};
use uuid::Uuid;

const NOW: u64 = 10_000;

async fn storage() -> (
    tempfile::TempDir,
    Arc<TestClock>,
    cc_lb_storage_sqlite::SqliteStorage,
) {
    let directory = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        directory.path().join("usage.sqlite").display()
    );
    let clock = Arc::new(TestClock::new_at_secs(NOW));
    let storage = cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
        .await
        .expect("open sqlite");
    storage
        .initialize(BackendKind::Sqlite)
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
async fn t3__api_key_usage_flush_is_idempotent_epoch_isolated_and_lease_guarded() {
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
async fn t3__api_key_usage_compaction_adds_repeated_folds_into_retired_bucket() {
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
